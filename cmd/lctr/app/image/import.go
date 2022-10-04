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
	"fmt"
	"io"
	"os"

	"github.com/containerd/containerd/cmd/ctr/commands"
	"github.com/containerd/containerd/pkg/transfer"
	"github.com/containerd/containerd/pkg/transfer/archive"
	image "github.com/containerd/containerd/pkg/transfer/image"
	"github.com/containerd/containerd/pkg/transfer/local"
	"github.com/containerd/lcontainerd/pkg/cli/progress"
	"github.com/containerd/lcontainerd/pkg/db"
	"github.com/urfave/cli"
)

var importCommand = cli.Command{
	Name:        "import",
	Usage:       "imports an image locally",
	ArgsUsage:   "[flags] <file>|-",
	Description: `Imports an OCI archive into the content and image store.`,
	Flags: append(append(commands.RegistryFlags, commands.LabelFlag),
		cli.StringFlag{
			Name:  "index-name",
			Usage: "image name to store index as",
		},
		cli.BoolFlag{
			Name:  "proto-out",
			Usage: "output progress directly to stdout as proto messages",
		},
	),
	Action: func(clicontext *cli.Context) error {
		var (
			in  = clicontext.Args().First()
			ctx = context.Background()
			err error
		)
		if in == "" {
			return fmt.Errorf("please provide a file to import")
		}

		mdb, err := db.NewDB(clicontext.GlobalString("data-dir"))
		if err != nil {
			return err
		}
		defer mdb.Close(ctx)

		ts := local.NewTransferService(db.NewLeaseManager(mdb), mdb.ContentStore(), db.NewImageStore(mdb))

		var opts []image.StoreOpt
		/*
			prefix := clicontext.String("base-name")
			if prefix == "" {
				prefix = fmt.Sprintf("import-%s", time.Now().Format("2006-01-02"))
				opts = append(opts, image.WithNamePrefix(prefix, false))
			} else {
				// When provided, filter out references which do not match
				opts = append(opts, image.WithNamePrefix(prefix, true))
			}

			if clicontext.Bool("digests") {
				opts = append(opts, image.WithDigestRefs(!clicontext.Bool("skip-digest-for-named")))
			}
		*/

		// TODO: Add platform options

		// TODO: Add unpack options

		is := image.NewStore(clicontext.String("index-name"), opts...)

		var iopts []archive.ImportOpt

		// Only for supporting images from old docker exports
		//if clicontext.Bool("compress-blobs") {
		//	iopts = append(iopts, archive.WithForceCompression)
		//}

		var r io.ReadCloser
		if in == "-" {
			r = os.Stdin
		} else {
			var err error
			r, err = os.Open(in)
			if err != nil {
				return err
			}
		}
		iis := archive.NewImageImportStream(r, "", iopts...)

		var pf transfer.ProgressFunc
		if clicontext.Bool("proto-out") {
			pf = progress.ForwardProto(ctx, os.Stdout)
		} else {
			pf = progress.Hierarchical(ctx, os.Stdout)
		}

		err = ts.Transfer(ctx, iis, is, transfer.WithProgress(pf))
		closeErr := r.Close()
		if err != nil {
			return err
		}

		return closeErr
	},
}
