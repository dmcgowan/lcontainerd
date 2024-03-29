/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package credentials

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/pkg/transfer/registry"
)

type keychainCredentials struct {
	user string
	ref  string
}

// NewKeychainCredentialHelper gets credentials from the default credential store
func NewKeychainCredentialHelper(ref, user string) (registry.CredentialHelper, error) {
	return &keychainCredentials{
		user: user,
		ref:  ref,
	}, nil
}

func (sc *keychainCredentials) GetCredentials(ctx context.Context, ref, host string) (registry.Credentials, error) {
	if ref == sc.ref {
		creds, err := getCredentials(ctx, host, sc.user)
		if !errors.Is(err, errdefs.ErrNotFound) {
			return creds, err
		}
	}
	return registry.Credentials{}, nil
}

type localCredentials struct {
	user    string
	ref     string
	dir     string
	decoder Decoder
}

// NewLocalCredentialHelper gets credentials from the default credential store
func NewLocalCredentialHelper(ref, user, dir string, decoder Decoder) (registry.CredentialHelper, error) {
	return &localCredentials{
		user:    user,
		ref:     ref,
		dir:     dir,
		decoder: decoder,
	}, nil
}

func (lc *localCredentials) GetCredentials(ctx context.Context, ref, host string) (registry.Credentials, error) {
	if ref != lc.ref {
		return registry.Credentials{}, nil
	}
	files, err := os.ReadDir(lc.dir)
	if err != nil {
		if errors.Is(err, errdefs.ErrNotFound) {
			err = nil
		}
		return registry.Credentials{}, err
	}
	fullMatch := host
	if lc.user != "" {
		fullMatch = fmt.Sprintf("%s@%s", lc.user, host)
	}
	var bestMatch string
	for _, e := range files {
		name := e.Name()
		if name == fullMatch {
			bestMatch = fullMatch
			break
		}
		if name == host {
			bestMatch = host
		} else if bestMatch == "" && strings.HasSuffix(name, "@"+host) {
			bestMatch = name
		}
	}
	if bestMatch == "" {
		return registry.Credentials{}, nil
	}

	b, err := os.ReadFile(filepath.Join(lc.dir, bestMatch))
	if err != nil {
		return registry.Credentials{}, err
	}

	return lc.decoder.Decode(b)
}
