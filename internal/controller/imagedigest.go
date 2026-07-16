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

package controller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// digestResolveTTL: 同一imageのレジストリ再問い合わせ間隔
// mutableタグの変更検知はこのTTL+reconcile周期の遅れで反映される
const digestResolveTTL = 5 * time.Minute

// digestResolveTimeout: レジストリ1回あたりの問い合わせ上限
const digestResolveTimeout = 10 * time.Second

// DigestResolver: image参照をレジストリで解決しimage@digestへpinする(TTL cache付き)
// headFuncはテスト差し替え用。両controllerで1インスタンスを共有する
type DigestResolver struct {
	mu       sync.Mutex
	cache    map[string]digestEntry
	headFunc func(ctx context.Context, image string, keychain authn.Keychain) (string, error)
}

type digestEntry struct {
	digest     string
	resolvedAt time.Time
}

func NewDigestResolver() *DigestResolver {
	return &DigestResolver{
		cache:    map[string]digestEntry{},
		headFunc: registryHead,
	}
}

// registryHead: レジストリのmanifest HEADでtagのdigestを取得
func registryHead(ctx context.Context, image string, keychain authn.Keychain) (string, error) {
	ref, err := name.ParseReference(image)
	if err != nil {
		return "", fmt.Errorf("parse image %q: %w", image, err)
	}
	ctx, cancel := context.WithTimeout(ctx, digestResolveTimeout)
	defer cancel()
	if keychain == nil {
		keychain = authn.DefaultKeychain
	}
	desc, err := remote.Head(ref, remote.WithContext(ctx), remote.WithAuthFromKeychain(keychain))
	if err != nil {
		return "", fmt.Errorf("resolve digest of %q: %w", image, err)
	}
	return desc.Digest.String(), nil
}

// Pinned: imageをimage@digestへ解決する。digest指定済みならそのまま
// 失敗時はTTL切れでもcacheがあればstaleを返す(pin↔非pinのflapでpodを無駄にrollさせない)
func (r *DigestResolver) Pinned(ctx context.Context, image string, keychain authn.Keychain) (string, error) {
	if strings.Contains(image, "@") {
		return image, nil
	}
	r.mu.Lock()
	entry, ok := r.cache[image]
	r.mu.Unlock()
	if ok && time.Since(entry.resolvedAt) < digestResolveTTL {
		return image + "@" + entry.digest, nil
	}
	digest, err := r.headFunc(ctx, image, keychain)
	if err != nil {
		if ok {
			return image + "@" + entry.digest, nil
		}
		return "", err
	}
	r.mu.Lock()
	r.cache[image] = digestEntry{digest: digest, resolvedAt: time.Now()}
	r.mu.Unlock()
	return image + "@" + digest, nil
}
