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

	"github.com/containerd/containerd/log"
	image "github.com/containerd/containerd/pkg/transfer/image"
	"github.com/keybase/go-keychain/secretservice"
)

func storeCredentials(ctx context.Context, host string, creds image.Credentials) error {
	attributes := map[string]string{
		"registry": host,
	}

	b, err := json.Marshal(creds)
	if err != nil {
		return err
	}

	s, err := secretservice.NewService()
	if err != nil {
		return err
	}
	session, err := s.OpenSession(secretservice.AuthenticationDHAES)
	if err != nil {
		return err
	}
	defer s.CloseSession(session)

	secret, err := session.NewSecret(b)
	if err != nil {
		return err
	}
	label := fmt.Sprintf("login for registry at %s", host)
	item, err := s.CreateItem(secretservice.DefaultCollection,
		secretservice.NewSecretProperties(label, attributes),
		secret, secretservice.ReplaceBehaviorReplace)
	if err != nil {
		return err
	}

	log.G(ctx).WithField("item", item).Debug("credentials saved to secret service")

	return nil
}

func getCredentials(ctx context.Context, host, user string) (image.Credentials, error) {
	attributes := map[string]string{
		"registry": host,
	}

	s, err := secretservice.NewService()
	if err != nil {
		return image.Credentials{}, err
	}
	session, err := s.OpenSession(secretservice.AuthenticationDHAES)
	if err != nil {
		return image.Credentials{}, err
	}
	defer s.CloseSession(session)

	items, err := s.SearchCollection(secretservice.DefaultCollection, attributes)
	if err != nil {
		return image.Credentials{}, err
	}
	for _, item := range items {
		sb, err := s.GetSecret(item, *session)
		if err != nil {
			return image.Credentials{}, err
		}
		var creds image.Credentials
		if err := json.Unmarshal(sb, &creds); err != nil {
			return image.Credentials{}, err
		}

		if creds.Secret != "" {
			// TODO: Select best item based on user/account match
			return creds, nil
		}
	}

	return image.Credentials{}, nil
}
