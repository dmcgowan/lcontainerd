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

	"github.com/containerd/containerd/leases"
	"github.com/containerd/lcontainerd/pkg/db"
	"github.com/urfave/cli"
)

var leaseImageCommand = cli.Command{
	Name:        "lease",
	Usage:       "leases an image",
	ArgsUsage:   "<image> [lease id] [flags]",
	Description: `Creates a lease on an image`,
	Flags: []cli.Flag{
		cli.DurationFlag{
			Name:  "expiration",
			Usage: "When to expire the lease",
		},
		cli.StringSliceFlag{
			Name:  "label",
			Usage: "Labels to add to the image",
		},
	},
	Action: func(clicontext *cli.Context) error {
		var (
			ctx   = context.Background()
			image = clicontext.Args().First()
		)
		mdb, err := db.NewDB(clicontext.GlobalString("data-dir"))
		if err != nil {
			return err
		}
		defer mdb.Close(ctx)

		imgdb := db.NewImageStore(mdb)
		img, err := imgdb.Get(ctx, image)
		if err != nil {
			return err
		}

		var opts []leases.Opt
		if id := clicontext.Args().Get(1); id != "" {
			opts = append(opts, leases.WithID(id))
		} else {
			opts = append(opts, leases.WithRandomID())
		}
		labels, err := keyValueArgs(clicontext.StringSlice("label"), "")
		if err != nil {
			return err
		}
		if len(labels) > 0 {
			opts = append(opts, leases.WithLabels(labels))
		}
		if d := clicontext.Duration("expiration"); d > 0 {
			opts = append(opts, leases.WithExpiration(d))
		}

		lm := db.NewLeaseManager(mdb)

		lease, err := lm.Create(ctx, opts...)
		if err != nil {
			return err
		}

		if err := lm.AddResource(ctx, lease, leases.Resource{
			ID:   img.Target.Digest.String(),
			Type: "content",
		}); err != nil {
			return err
		}

		fmt.Fprintf(os.Stdout, "Created lease %s\n", lease.ID)
		return nil
	},
}
