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

package content

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/containerd/containerd/content"
	"github.com/containerd/lcontainerd/pkg/db"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/urfave/cli"
)

var readCommand = cli.Command{
	Name:        "get",
	Usage:       "get content",
	ArgsUsage:   "<digest> [<file>]",
	Description: `Gets content from the local content store`,
	Flags:       []cli.Flag{},
	Action: func(clicontext *cli.Context) error {
		var (
			ctx = context.Background()
		)

		dgst, err := digest.Parse(clicontext.Args().First())
		if err != nil {
			return fmt.Errorf("invalid digest: %w", err)
		}

		var f io.Writer
		if path := clicontext.Args().Get(1); path != "" {
			fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0600)
			if err != nil {
				return err
			}
			defer fp.Close()
			f = fp
		} else {
			f = os.Stdout
		}

		mdb, err := db.NewDB(clicontext.GlobalString("data-dir"), db.WithReadOnly)
		if err != nil {
			return err
		}
		defer mdb.Close(ctx)

		ra, err := mdb.ContentStore().ReaderAt(ctx, ocispec.Descriptor{Digest: dgst})
		if err != nil {
			return err
		}

		_, err = io.Copy(f, content.NewReader(ra))

		return err
	},
}
