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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/images"
	"github.com/containerd/lcontainerd/pkg/db"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/urfave/cli"
)

var listCommand = cli.Command{
	Name:        "list",
	Aliases:     []string{"ls"},
	Usage:       "list all images",
	ArgsUsage:   "[flags]",
	Description: `Lists all images stored locally`,
	Action: func(clicontext *cli.Context) error {
		var (
			ctx = context.Background()
		)
		mdb, err := db.NewDB(clicontext.GlobalString("data-dir"), db.WithReadOnly)
		if err != nil {
			return err
		}
		defer mdb.Close(ctx)

		imgdb := db.NewImageStore(mdb)
		images, err := imgdb.List(ctx)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(os.Stdout, 8, 3, 1, ' ', 0)
		fmt.Fprintf(tw, "Image Name\tDigest\tMedia Type\n")
		fmt.Fprintf(tw, "----------\t------\t----------\n")

		for _, img := range images {
			fmt.Fprintf(tw, "%s\t%s\t%s\n", img.Name, img.Target.Digest, img.Target.MediaType)
		}

		return tw.Flush()
	},
}

var readCommand = cli.Command{
	Name:        "inspect",
	Aliases:     []string{"i"},
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
