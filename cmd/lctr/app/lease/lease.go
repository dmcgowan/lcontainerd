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

package lease

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/containerd/containerd/leases"
	"github.com/containerd/lcontainerd/pkg/db"
	"github.com/urfave/cli"
)

// Command is the cli command for managing images
var Command = cli.Command{
	Name:    "lease",
	Aliases: []string{"l"},
	Usage:   "manage leases",
	Subcommands: cli.Commands{
		listLeaseCommand,
		inspectLeaseCommand,
		removeLeaseCommand,
	},
}

var listLeaseCommand = cli.Command{
	Name:        "list",
	Aliases:     []string{"ls"},
	Usage:       "list all leases",
	ArgsUsage:   "[flags]",
	Description: `Lists all leases`,
	Action: func(clicontext *cli.Context) error {
		var (
			ctx = context.Background()
		)
		mdb, err := db.NewDB(clicontext.GlobalString("data-dir"), db.WithReadOnly)
		if err != nil {
			return err
		}
		defer mdb.Close(ctx)

		lm := db.NewLeaseManager(mdb)

		leases, err := lm.List(ctx)
		if err != nil {
			return err
		}

		tw := tabwriter.NewWriter(os.Stdout, 8, 3, 1, ' ', 0)
		fmt.Fprintf(tw, "Lease ID\tCreated At\tLabels\n")
		fmt.Fprintf(tw, "----------\t------\t----------\n")

		for _, l := range leases {
			fmt.Fprintf(tw, "%s\t%s\t%s\n", l.ID, l.CreatedAt, formatLabels(l.Labels))
		}

		return tw.Flush()
	},
}

var inspectLeaseCommand = cli.Command{
	Name:        "inspect",
	Usage:       "inspect a lease",
	ArgsUsage:   "<lease id> [flags]",
	Description: `Inspect the resources owned by a lease`,
	Action: func(clicontext *cli.Context) error {
		var (
			ctx = context.Background()
			lid = clicontext.Args().First()
		)
		if lid == "" {
			return fmt.Errorf("must provide a lease ID")
		}
		mdb, err := db.NewDB(clicontext.GlobalString("data-dir"), db.WithReadOnly)
		if err != nil {
			return err
		}
		defer mdb.Close(ctx)

		lm := db.NewLeaseManager(mdb)

		resources, err := lm.ListResources(ctx, leases.Lease{ID: lid})
		if err != nil {
			return err
		}

		tw := tabwriter.NewWriter(os.Stdout, 8, 3, 1, ' ', 0)
		fmt.Fprintf(tw, "Type\tID\n")
		fmt.Fprintf(tw, "----\t--\n")

		for _, r := range resources {
			fmt.Fprintf(tw, "%s\t%s\n", r.Type, r.ID)
		}

		return tw.Flush()
	},
}

var removeLeaseCommand = cli.Command{
	Name:        "remove",
	Aliases:     []string{"rm"},
	Usage:       "remove a lease",
	ArgsUsage:   "<lease id> [flags]",
	Description: `Inspect the resources owned by a lease`,
	Action: func(clicontext *cli.Context) error {
		var (
			ctx = context.Background()
			lid = clicontext.Args().First()
		)
		if lid == "" {
			return fmt.Errorf("must provide a lease ID")
		}
		mdb, err := db.NewDB(clicontext.GlobalString("data-dir"))
		if err != nil {
			return err
		}
		defer mdb.Close(ctx)

		lm := db.NewLeaseManager(mdb)

		err = lm.Delete(ctx, leases.Lease{ID: lid})
		if err != nil {
			return err
		}

		fmt.Fprintf(os.Stdout, "Deleted lease %s\n", lid)
		return nil
	},
}

func formatLabels(l map[string]string) string {
	var ls []string
	for k, v := range l {
		ls = append(ls, fmt.Sprintf("%s=%s", k, v))
	}
	return strings.Join(ls, ", ")
}
