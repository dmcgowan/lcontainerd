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

package app

import (
	"context"
	"fmt"
	"os"

	"github.com/containerd/containerd/cmd/ctr/commands"
	"github.com/containerd/containerd/pkg/transfer"
	image "github.com/containerd/containerd/pkg/transfer/image"
	"github.com/containerd/containerd/pkg/transfer/local"
	dockerref "github.com/containerd/containerd/reference/docker"
	"github.com/containerd/lcontainerd/pkg/db"
	"github.com/urfave/cli"
)

var pushCommand = cli.Command{
	Name:      "push-image",
	Aliases:   []string{"push"},
	Usage:     "push an image to a remote",
	ArgsUsage: "[flags] <ref> [<local>]",
	Description: `Fetch and prepare an image for use in containerd.

After pulling an image, it should be ready to use the same reference in a run
command. As part of this process, we do the following:

1. Fetch all resources into containerd.
2. Prepare the snapshot filesystem with the pulled resources.
3. Register metadata for the image.
`,
	Flags: append(append(commands.RegistryFlags, commands.LabelFlag),
		cli.StringSliceFlag{
			Name:  "platform",
			Usage: "Pull content from a specific platform",
			Value: &cli.StringSlice{},
		},
		cli.BoolFlag{
			Name:  "all-platforms",
			Usage: "pull content and metadata from all platforms",
		},
		cli.BoolFlag{
			Name:  "proto-out",
			Usage: "output progress directly to stdout as proto messages",
		},
		cli.IntFlag{
			Name:  "max-concurrent-downloads",
			Usage: "Set the max concurrent downloads for each pull",
		},
	),
	Action: func(clicontext *cli.Context) error {
		var (
			ref      = clicontext.Args().First()
			localref = clicontext.Args().Get(1)
			ctx      = context.Background()
			err      error
		)
		if ref == "" {
			return fmt.Errorf("please provide an image reference to pull")
		}
		ref, err = normalizeName(ref)
		if err != nil {
			return err
		}

		if localref == "" {
			localref = ref
		} else {
			localref, err = normalizeName(localref)
			if err != nil {
				return err
			}
		}

		ch, err := commands.NewStaticCredentials(ctx, clicontext, ref)
		if err != nil {
			return err
		}

		mdb, err := db.NewDB(clicontext.GlobalString("data-dir"))
		if err != nil {
			return err
		}
		defer mdb.Close(ctx)

		reg := image.NewOCIRegistry(ref, nil, ch)
		is := image.NewStore(localref)

		ts := local.NewTransferService(db.NewLeaseManager(mdb), mdb.ContentStore(), db.NewImageStore(mdb))

		var pf transfer.ProgressFunc
		if clicontext.Bool("proto-out") {
			pf = ProtoProgressForward(ctx, os.Stdout)
		} else {
			pf = ProgressHandler(ctx, os.Stdout)
		}

		if err := ts.Transfer(ctx, is, reg, transfer.WithProgress(pf)); err != nil {
			return err
		}

		return nil
	},
}

func normalizeName(name string) (string, error) {
	named, err := dockerref.ParseDockerRef(name)
	if err != nil {
		return "", err
	}
	return named.String(), nil
}
