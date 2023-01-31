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
	"encoding/json"
	"fmt"

	"github.com/containerd/containerd/errdefs"
	image "github.com/containerd/containerd/pkg/transfer/image"

	"github.com/keybase/go-keychain"
)

func storeCredentials(ctx context.Context, host string, creds image.Credentials) error {
	b, err := json.Marshal(creds)
	if err != nil {
		return err
	}

	item := keychain.NewItem()
	item.SetSecClass(keychain.SecClassGenericPassword)
	item.SetService(host)
	if creds.Username != "" {
		item.SetAccount(creds.Username)
	}
	item.SetLabel(id(host))
	item.SetDescription("containerd registry credentials")
	item.SetAccessGroup("io.containerd")
	item.SetSynchronizable(keychain.SynchronizableNo)
	item.SetAccessible(keychain.AccessibleWhenUnlocked)
	item.SetData(b)

	err = keychain.AddItem(item)
	if err == keychain.ErrorDuplicateItem {
		return keychain.UpdateItem(item, item)
	}

	return err
}

func getCredentials(ctx context.Context, host, user string) (image.Credentials, error) {
	kid := id(host)
	item := keychain.NewItem()
	item.SetSecClass(keychain.SecClassGenericPassword)
	item.SetService(host)
	item.SetLabel(kid)
	item.SetDescription("containerd registry credentials")
	item.SetAccessGroup("io.containerd")
	item.SetReturnAttributes(true)
	item.SetMatchLimit(keychain.MatchLimitAll)

	// Query all and choose best match
	items, err := keychain.QueryItem(item)
	if err != nil {
		return image.Credentials{}, fmt.Errorf("keychain query failed: %w", err)
	}
	var bestMatch *keychain.QueryResult
	for _, item := range items {
		if item.Account != "" {
			if item.Account == user {
				bestMatch = &item
				break
			} else if user != "" {
				continue
			}
		}
		if bestMatch != nil {
			bestMatch = &item
		}
	}

	if bestMatch == nil {
		return image.Credentials{}, errdefs.ErrNotFound
	}

	item = keychain.NewItem()
	item.SetSecClass(keychain.SecClassGenericPassword)
	item.SetService(host)
	item.SetLabel(kid)
	item.SetAccount(bestMatch.Account)
	item.SetAccessGroup("io.containerd")
	item.SetReturnAttributes(true)
	item.SetMatchLimit(keychain.MatchLimitOne)
	item.SetReturnData(true)

	// Get single result
	items, err = keychain.QueryItem(item)
	if err != nil {
		return image.Credentials{}, fmt.Errorf("keychain query failed: %w", err)
	}

	if len(items) != 1 {
		return image.Credentials{}, errdefs.ErrNotFound
	}

	var creds image.Credentials
	if err := json.Unmarshal(items[0].Data, &creds); err != nil {
		return image.Credentials{}, err
	}

	return creds, nil
}

func id(host string) string {
	return fmt.Sprintf("containerd login: %s", host)
}
