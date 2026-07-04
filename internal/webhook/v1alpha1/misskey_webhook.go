/*
Copyright (C) 2026 chan-mai

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package v1alpha1

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	misskeyv1alpha1 "github.com/chan-mai/cloud-native-misskey/api/v1alpha1"
)

var misskeyGroupKind = schema.GroupKind{Group: "cloudnative-misskey.dev", Kind: "Misskey"}

// SetupMisskeyWebhookWithManager: Misskeyのdefaulter/validatorをmanagerへ登録
func SetupMisskeyWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&misskeyv1alpha1.Misskey{}).
		WithDefaulter(&MisskeyCustomDefaulter{}).
		WithValidator(&MisskeyCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-cloudnative-misskey-dev-v1alpha1-misskey,mutating=true,failurePolicy=fail,sideEffects=None,groups=cloudnative-misskey.dev,resources=misskeys,verbs=create;update,versions=v1alpha1,name=mmisskey-v1alpha1.kb.io,admissionReviewVersions=v1

// MisskeyCustomDefaulter: CRD既定で表せない項目を補完
type MisskeyCustomDefaulter struct{}

var _ webhook.CustomDefaulter = &MisskeyCustomDefaulter{}

// Default: tenant未設定はnamespaceで確定。以後immutableとなり「未設定→初回設定」の穴を塞ぐ
func (d *MisskeyCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	m, ok := obj.(*misskeyv1alpha1.Misskey)
	if !ok {
		return fmt.Errorf("expected Misskey, got %T", obj)
	}
	if m.Spec.Tenant == "" {
		m.Spec.Tenant = m.Namespace
	}
	return nil
}

// +kubebuilder:webhook:path=/validate-cloudnative-misskey-dev-v1alpha1-misskey,mutating=false,failurePolicy=fail,sideEffects=None,groups=cloudnative-misskey.dev,resources=misskeys,verbs=create;update,versions=v1alpha1,name=vmisskey-v1alpha1.kb.io,admissionReviewVersions=v1

// MisskeyCustomValidator: 不変フィールド検証 + cross-field整合性検証
type MisskeyCustomValidator struct{}

var _ webhook.CustomValidator = &MisskeyCustomValidator{}

func (v *MisskeyCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	m, ok := obj.(*misskeyv1alpha1.Misskey)
	if !ok {
		return nil, fmt.Errorf("expected Misskey, got %T", obj)
	}
	return validateSpec(m)
}

func (v *MisskeyCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	oldM, ok := oldObj.(*misskeyv1alpha1.Misskey)
	if !ok {
		return nil, fmt.Errorf("expected Misskey, got %T", oldObj)
	}
	newM, ok := newObj.(*misskeyv1alpha1.Misskey)
	if !ok {
		return nil, fmt.Errorf("expected Misskey, got %T", newObj)
	}
	warns, errs := validateImmutable(oldM, newM)
	w2, err := validateSpec(newM)
	warns = append(warns, w2...)
	if err != nil {
		if serr, ok := err.(*apierrors.StatusError); ok {
			errs = append(errs, statusCauseAsFieldErrs(serr, newM)...)
		} else {
			return warns, err
		}
	}
	if len(errs) == 0 {
		return warns, nil
	}
	return warns, apierrors.NewInvalid(misskeyGroupKind, newM.Name, errs)
}

func (v *MisskeyCustomValidator) ValidateDelete(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

// validateImmutable: 初期化後に変えてはならないフィールドの検証
func validateImmutable(oldM, newM *misskeyv1alpha1.Misskey) (admission.Warnings, field.ErrorList) {
	var errs field.ErrorList
	sp := field.NewPath("spec")
	if oldM.Spec.URL != newM.Spec.URL {
		errs = append(errs, field.Invalid(sp.Child("url"), newM.Spec.URL, "url is immutable"))
	}
	if oldM.Spec.IDGenerationMethod != newM.Spec.IDGenerationMethod {
		errs = append(errs, field.Invalid(sp.Child("idGenerationMethod"), newM.Spec.IDGenerationMethod, "idGenerationMethod is immutable"))
	}
	if oldM.Spec.Tenant != newM.Spec.Tenant {
		errs = append(errs, field.Invalid(sp.Child("tenant"), newM.Spec.Tenant, "tenant is immutable"))
	}
	return nil, errs
}

// validateSpec: managed/externalの排他やautoscalingの範囲などcross-field検証
func validateSpec(m *misskeyv1alpha1.Misskey) (admission.Warnings, error) {
	var errs field.ErrorList
	var warns admission.Warnings
	sp := field.NewPath("spec")

	// --- PostgreSQL ---
	pg := m.Spec.Postgres
	if pg.External != nil {
		if pg.Pooler != nil {
			errs = append(errs, field.Forbidden(sp.Child("postgres", "pooler"), "pooler requires managed PostgreSQL; remove postgres.external"))
		}
		if pg.Backup != nil {
			errs = append(errs, field.Forbidden(sp.Child("postgres", "backup"), "backup requires managed PostgreSQL; remove postgres.external"))
		}
		if pg.ReadOffload != nil && *pg.ReadOffload {
			warns = append(warns, "spec.postgres.readOffload has no effect with an external database")
		}
	} else if pg.ReadOffload != nil && *pg.ReadOffload && instancesOr(pg.Instances) < 2 {
		warns = append(warns, "spec.postgres.readOffload needs postgres.instances>=2 to take effect")
	}

	// --- Redis(default + roles) ---
	rs := m.Spec.Redis
	if rs.External != nil {
		if rs.HA != nil {
			errs = append(errs, field.Forbidden(sp.Child("redis", "ha"), "HA requires managed Redis; remove redis.external"))
		}
		if rs.Roles != nil {
			warns = append(warns, "spec.redis.roles is ignored while redis.external is set")
		}
	}
	if rs.Roles != nil {
		validateRedisRole(sp.Child("redis", "roles", "jobQueue"), rs.Roles.JobQueue, &errs)
		validateRedisRole(sp.Child("redis", "roles", "pubsub"), rs.Roles.Pubsub, &errs)
		validateRedisRole(sp.Child("redis", "roles", "timelines"), rs.Roles.Timelines, &errs)
		validateRedisRole(sp.Child("redis", "roles", "reactions"), rs.Roles.Reactions, &errs)
	}

	// --- Autoscaling(app/worker) ---
	validateAutoscaling(sp.Child("app", "autoscaling"), m.Spec.App.Autoscaling, &errs)
	validateAutoscaling(sp.Child("worker", "autoscaling"), m.Spec.Worker.Autoscaling, &errs)

	// --- Search ---
	if m.Spec.Search.Provider == misskeyv1alpha1.SearchSQLPgroonga && pg.External == nil && pg.ImageName == "" {
		warns = append(warns, "search.provider=sqlPgroonga requires postgres.imageName with the PGroonga extension")
	}

	if len(errs) == 0 {
		return warns, nil
	}
	return warns, apierrors.NewInvalid(misskeyGroupKind, m.Name, errs)
}

// validateRedisRole: role毎のexternal xor managed override排他
func validateRedisRole(p *field.Path, role *misskeyv1alpha1.RedisRole, errs *field.ErrorList) {
	if role == nil || role.External == nil {
		return
	}
	if role.HA != nil {
		*errs = append(*errs, field.Forbidden(p.Child("ha"), "external role cannot also set HA"))
	}
	if role.MaxMemory != "" || role.MaxMemoryPolicy != "" || !role.Storage.IsZero() {
		*errs = append(*errs, field.Forbidden(p, "external role cannot also set managed overrides (maxMemory/storage/etc.)"))
	}
}

// validateAutoscaling: minReplicas<=maxReplicas
func validateAutoscaling(p *field.Path, a *misskeyv1alpha1.AutoscalingSpec, errs *field.ErrorList) {
	if a == nil || a.MinReplicas == nil {
		return
	}
	if *a.MinReplicas > a.MaxReplicas {
		*errs = append(*errs, field.Invalid(p.Child("minReplicas"), *a.MinReplicas, "minReplicas must not exceed maxReplicas"))
	}
}

// instancesOr: pg.Instances(0は既定1)
func instancesOr(instances int32) int32 {
	if instances == 0 {
		return 1
	}
	return instances
}

// statusCauseAsFieldErrs: validateSpecが返したStatusErrorのcauseをfield.Errorへ戻す(update時の集約用)
func statusCauseAsFieldErrs(serr *apierrors.StatusError, _ *misskeyv1alpha1.Misskey) field.ErrorList {
	var out field.ErrorList
	if serr.ErrStatus.Details == nil {
		return out
	}
	for _, c := range serr.ErrStatus.Details.Causes {
		out = append(out, &field.Error{Type: field.ErrorTypeInvalid, Field: c.Field, Detail: c.Message})
	}
	return out
}
