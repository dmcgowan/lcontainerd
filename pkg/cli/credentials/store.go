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
	"fmt"
	"os"
	"path/filepath"

	"github.com/containerd/containerd/pkg/transfer/image"
)

// StoreCredentialsInKeychain stores the credentials in the default keychain credential store
// for the system or environment
func StoreCredentialsInKeychain(ctx context.Context, host string, creds image.Credentials) error {
	return storeCredentials(ctx, host, creds)
}

// StoreCredentialsLocal stores the credentials to a local directory using the provided encoder
func StoreCredentialsLocal(ctx context.Context, dir, host string, creds image.Credentials, encoder Encoder) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	b, err := encoder.Encode(creds)
	if err != nil {
		return err
	}

	// TODO: Store at user@host and host if not exists?
	name := host
	if creds.Username != "" {
		name = fmt.Sprintf("%s@%s", creds.Username, host)
	}
	return os.WriteFile(filepath.Join(dir, name), b, 0600)
}

type Encoder interface {
	Encode(image.Credentials) ([]byte, error)
}

type Decoder interface {
	Decode([]byte) (image.Credentials, error)
}

type EncoderDecoder interface {
	Encoder
	Decoder
}
