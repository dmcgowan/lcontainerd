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
	"strings"
	"text/tabwriter"

	"github.com/containerd/containerd/leases"
	"github.com/containerd/lcontainerd/pkg/db"
	"github.com/urfave/cli"
)

var listLeaseCommand = cli.Command{
	Name:        "list-leases",
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

var leaseImageCommand = cli.Command{
	Name:        "lease-image",
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

var inspectLeaseCommand = cli.Command{
	Name:        "inspect-lease",
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
	Name:        "remove-lease",
	Aliases:     []string{"rml"},
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

/*
var readCommand = cli.Command{
	Name:        "inspect-image",
	Usage:       "inspect an image",
	ArgsUsage:   "<image> [flags]",
	Description: `Inspect an image`,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "content",
			Usage: "Show JSON content",
		},
	},
	Action: func(clicontext *cli.Context) error {
		var (
			ctx = context.Background()
			ref = clicontext.Args().First()
		)
		mdb, err := db.NewDB(clicontext.GlobalString("data-dir"), db.WithReadOnly)
		if err != nil {
			return err
		}
		defer mdb.Close(ctx)

		imgdb := db.NewImageStore(mdb)
		img, err := imgdb.Get(ctx, ref)
		if err != nil {
			return err
		}

		w := os.Stdout
		fmt.Fprintln(w, img.Name)
		subchild := "│   "
		fmt.Fprintf(w, "%s Created: %s\n", subchild, img.CreatedAt)
		fmt.Fprintf(w, "%s Updated: %s\n", subchild, img.UpdatedAt)
		for k, v := range img.Labels {
			fmt.Fprintf(w, "%s Label %q: %q\n", subchild, k, v)
		}
		return printManifestTree(ctx, w, img.Target, mdb.ContentStore(), "└── ", "    ", clicontext.Bool("content"))
	},
}

func printManifestTree(ctx context.Context, w io.Writer, desc ocispec.Descriptor, p content.Store, prefix, childprefix string, verbose bool) error {
	subprefix := childprefix + "├── "
	subchild := childprefix + "│   "
	fmt.Fprintf(w, "%s%s @%s (%d bytes)\n", prefix, desc.MediaType, desc.Digest, desc.Size)

	if desc.Platform != nil && desc.Platform.Architecture != "" {
		// TODO: Use containerd platform library to format
		fmt.Fprintf(w, "%s Platform: %s/%s\n", subchild, desc.Platform.OS, desc.Platform.Architecture)
	}
	b, err := content.ReadBlob(ctx, p, desc)
	if err != nil {
		return err
	}
	if err := showContent(ctx, w, p, desc, subchild, verbose); err != nil {
		return err
	}

	switch desc.MediaType {
	case images.MediaTypeDockerSchema2Manifest, ocispec.MediaTypeImageManifest:
		var manifest ocispec.Manifest
		if err := json.Unmarshal(b, &manifest); err != nil {
			return err
		}

		if len(manifest.Layers) == 0 {
			subprefix = childprefix + "└── "
			subchild = childprefix + "    "
		}
		fmt.Fprintf(w, "%s%s @%s (%d bytes)\n", subprefix, manifest.Config.MediaType, manifest.Config.Digest, manifest.Config.Size)

		if err := showContent(ctx, w, p, manifest.Config, subchild, verbose); err != nil {
			return err
		}

		for i := range manifest.Layers {
			if len(manifest.Layers) == i+1 {
				subprefix = childprefix + "└── "
				//subchild = childprefix + "    "
			}
			fmt.Fprintf(w, "%s%s @%s (%d bytes)\n", subprefix, manifest.Layers[i].MediaType, manifest.Layers[i].Digest, manifest.Layers[i].Size)
		}

	case images.MediaTypeDockerSchema2ManifestList, ocispec.MediaTypeImageIndex:
		var idx ocispec.Index
		if err := json.Unmarshal(b, &idx); err != nil {
			return err
		}

		for i := range idx.Manifests {
			if len(idx.Manifests) == i+1 {
				subprefix = childprefix + "└── "
				subchild = childprefix + "    "
			}
			if err := printManifestTree(ctx, w, idx.Manifests[i], p, subprefix, subchild, verbose); err != nil {
				return err
			}
		}
	}

	return nil
}

func showContent(ctx context.Context, w io.Writer, p content.Store, desc ocispec.Descriptor, prefix string, verbose bool) error {
	if verbose {
		info, err := p.Info(ctx, desc.Digest)
		if err != nil {
			return err
		}
		if len(info.Labels) > 0 {
			fmt.Fprintf(w, "%s┌────────Labels─────────\n", prefix)
			for k, v := range info.Labels {
				fmt.Fprintf(w, "%s│%q: %q\n", prefix, k, v)
			}
			fmt.Fprintf(w, "%s└───────────────────────\n", prefix)
		}
	}
	if verbose && strings.HasSuffix(desc.MediaType, "json") {
		// Print content for config
		cb, err := content.ReadBlob(ctx, p, desc)
		if err != nil {
			return err
		}
		dst := bytes.NewBuffer(nil)
		json.Indent(dst, cb, prefix+"│", "   ")
		fmt.Fprintf(w, "%s┌────────Content────────\n", prefix)
		fmt.Fprintf(w, "%s│%s\n", prefix, strings.TrimSpace(dst.String()))
		fmt.Fprintf(w, "%s└───────────────────────\n", prefix)
	}
	return nil
}
*/
