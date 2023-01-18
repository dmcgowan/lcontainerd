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

package image

import (
	"context"
	"net/url"

	"github.com/containerd/containerd/cmd/ctr/commands"
	"github.com/containerd/lcontainerd/pkg/cli/credentials"
	"github.com/urfave/cli"
)

var loginCommand = cli.Command{
	Name:        "login",
	Usage:       "saves login for a registry",
	ArgsUsage:   "[flags] <host>",
	Description: `Imports an OCI archive into the content and image store.`,
	Flags:       commands.RegistryFlags,
	Action: func(clicontext *cli.Context) error {
		ctx := context.Background()

		host := clicontext.Args().First()
		if host == "" {
			return cli.NewExitError("provide a host", 1)
		}
		if u, err := url.Parse(host); err != nil {
			return err
		} else if u.Host != "" {
			host = u.Host
		}
		if host == "docker.io" {
			host = "registry-1.docker.io"
		}

		ch, err := commands.NewStaticCredentials(ctx, clicontext, "")
		if err != nil {
			return err
		}
		creds, err := ch.GetCredentials(ctx, "", host)
		if err != nil {
			return err
		}
		creds.Host = host

		return credentials.StoreCredentials(ctx, host, creds)
	},
}
