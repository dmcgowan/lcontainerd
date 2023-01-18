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

func storeCredentials(ctx context.Context, host string, creds image.Credentials) error {
	item := keychain.NewItem()
	item.SetSecClass(keychain.SecClassGenericPassword)
	item.SetService("containerd")
	item.SetAccount(creds.Username)
	item.SetLabel(fmt.Sprintf("Login for %s", host))
	item.SetAccessGroup("io.containerd")
	item.SetData([]byte("toomanysecrets"))
	item.SetSynchronizable(keychain.SynchronizableNo)
	item.SetAccessible(keychain.AccessibleWhenUnlocked)
	err := keychain.AddItem(item)

	if err == keychain.ErrorDuplicateItem {
		// Duplicate
	}
	return nil
}

func getCredentials(ctx context.Context, host, user string) (image.Credentials, error) {
	returb image.Credentials{}, nil
}