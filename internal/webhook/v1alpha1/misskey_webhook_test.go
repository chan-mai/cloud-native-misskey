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
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	misskeyv1alpha1 "github.com/chan-mai/cloud-native-misskey/api/v1alpha1"
)

func base() *misskeyv1alpha1.Misskey {
	return &misskeyv1alpha1.Misskey{
		ObjectMeta: metav1.ObjectMeta{Name: "ex", Namespace: "ns"},
		Spec:       misskeyv1alpha1.MisskeySpec{URL: "https://m.example.com/", Image: "misskey/misskey:x"},
	}
}

func TestDefaultTenant(t *testing.T) {
	d := &MisskeyCustomDefaulter{}
	m := base()
	if err := d.Default(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if m.Spec.Tenant != "ns" {
		t.Errorf("tenant default: got %q, want ns", m.Spec.Tenant)
	}
	// 設定済みは上書きしない
	m2 := base()
	m2.Spec.Tenant = "acme"
	_ = d.Default(context.Background(), m2)
	if m2.Spec.Tenant != "acme" {
		t.Errorf("tenant overwritten: %q", m2.Spec.Tenant)
	}
}

func TestValidateCreateOK(t *testing.T) {
	v := &MisskeyCustomValidator{}
	if _, err := v.ValidateCreate(context.Background(), base()); err != nil {
		t.Errorf("valid spec rejected: %v", err)
	}
}

func TestValidatePoolerRequiresManaged(t *testing.T) {
	v := &MisskeyCustomValidator{}
	m := base()
	m.Spec.Postgres.External = &misskeyv1alpha1.ExternalPostgres{Host: "h"}
	m.Spec.Postgres.Pooler = &misskeyv1alpha1.PostgresPooler{}
	if _, err := v.ValidateCreate(context.Background(), m); !apierrors.IsInvalid(err) {
		t.Errorf("pooler+external must be invalid, got %v", err)
	}
}

func TestValidateRedisHAExternal(t *testing.T) {
	v := &MisskeyCustomValidator{}
	m := base()
	m.Spec.Redis.External = &misskeyv1alpha1.ExternalRedis{Host: "r"}
	m.Spec.Redis.HA = &misskeyv1alpha1.RedisHA{}
	if _, err := v.ValidateCreate(context.Background(), m); !apierrors.IsInvalid(err) {
		t.Errorf("ha+external must be invalid, got %v", err)
	}
}

func TestValidateAutoscalingRange(t *testing.T) {
	v := &MisskeyCustomValidator{}
	m := base()
	min := int32(5)
	m.Spec.Worker.Autoscaling = &misskeyv1alpha1.AutoscalingSpec{MinReplicas: &min, MaxReplicas: 3}
	if _, err := v.ValidateCreate(context.Background(), m); !apierrors.IsInvalid(err) {
		t.Errorf("min>max must be invalid, got %v", err)
	}
}

func TestValidateImmutable(t *testing.T) {
	v := &MisskeyCustomValidator{}
	old := base()
	old.Spec.Tenant = "ns"

	urlChange := base()
	urlChange.Spec.Tenant = "ns"
	urlChange.Spec.URL = "https://other.example.com/"
	if _, err := v.ValidateUpdate(context.Background(), old, urlChange); !apierrors.IsInvalid(err) {
		t.Errorf("url change must be rejected: %v", err)
	}

	tenantChange := base()
	tenantChange.Spec.Tenant = "acme"
	if _, err := v.ValidateUpdate(context.Background(), old, tenantChange); !apierrors.IsInvalid(err) {
		t.Errorf("tenant change must be rejected: %v", err)
	}

	noChange := base()
	noChange.Spec.Tenant = "ns"
	if _, err := v.ValidateUpdate(context.Background(), old, noChange); err != nil {
		t.Errorf("unchanged update rejected: %v", err)
	}
}
