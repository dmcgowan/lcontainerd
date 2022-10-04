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
	"os"

	"github.com/containerd/containerd/cmd/ctr/commands"
	"github.com/containerd/containerd/pkg/transfer"
	image "github.com/containerd/containerd/pkg/transfer/image"
	"github.com/containerd/containerd/pkg/transfer/local"
	"github.com/containerd/containerd/platforms"
	dockerref "github.com/containerd/containerd/reference/docker"
	"github.com/containerd/lcontainerd/pkg/cli/progress"
	"github.com/containerd/lcontainerd/pkg/db"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/urfave/cli"
)

var pullCommand = cli.Command{
	Name:      "pull",
	Usage:     "pull an image from a remote",
	ArgsUsage: "[flags] <ref>",
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
			ref = clicontext.Args().First()
			ctx = context.Background()
		)
		if ref == "" {
			return fmt.Errorf("please provide an image reference to pull")
		}

		named, err := dockerref.ParseDockerRef(ref)
		if err != nil {
			return err
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

		var sopts []image.StoreOpt
		storeplatforms := clicontext.StringSlice("platform")
		// Add platforms if provided, default to all platforms
		if len(storeplatforms) > 0 {
			var p []ocispec.Platform
			for _, s := range storeplatforms {
				ps, err := platforms.Parse(s)
				if err != nil {
					return fmt.Errorf("unable to parse platform %s: %w", s, err)
				}
				p = append(p, ps)
			}
			sopts = append(sopts, image.WithPlatforms(p...))
		}

		reg := image.NewOCIRegistry(named.String(), nil, ch)
		is := image.NewStore(named.String(), sopts...)

		ts := local.NewTransferService(db.NewLeaseManager(mdb), mdb.ContentStore(), db.NewImageStore(mdb))

		var pf transfer.ProgressFunc
		if clicontext.Bool("proto-out") {
			pf = progress.ForwardProto(ctx, os.Stdout)
		} else {
			pf = progress.Hierarchical(ctx, os.Stdout)
		}

		if err := ts.Transfer(ctx, reg, is, transfer.WithProgress(pf)); err != nil {
			return err
		}

		return nil
	},
}
