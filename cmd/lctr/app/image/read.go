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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/images"
	"github.com/containerd/lcontainerd/pkg/cli/display"
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

		opts := []display.PrintOpt{
			display.WithWriter(os.Stdout),
		}
		if clicontext.Bool("content") {
			opts = append(opts, display.Verbose)
		}

		return display.NewPrinter(opts...).PrintImageTree(ctx, img, mdb.ContentStore())
	},
}

var getContentCommand = cli.Command{
	Name:        "get-content",
	Usage:       "gets image content",
	ArgsUsage:   "<image> [file] [flags]",
	Description: `Gets content for an image, defaults to first layer (or first layer of first manifest when index)`,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "index",
			Usage: "Get the index (when image points to index)",
		},
		cli.IntFlag{
			Name:  "index-manifest",
			Usage: "Which manifest to use in index",
			Value: 0,
		},
		cli.BoolFlag{
			Name:  "manifest",
			Usage: "Get the manifest",
		},
		cli.BoolFlag{
			Name:  "config",
			Usage: "Get the config",
		},
		cli.IntFlag{
			Name:  "layer",
			Usage: "Which layer to get",
			Value: 0,
		},
		cli.BoolFlag{
			Name:  "media-type",
			Usage: "Get media type only",
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

		desc, err := resolveGetDigest(ctx, img.Target, clicontext, mdb.ContentStore())
		if err != nil {
			return err
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

		if clicontext.Bool("media-type") {
			_, err = fmt.Fprint(f, desc.MediaType)
			return err
		}

		ra, err := mdb.ContentStore().ReaderAt(ctx, desc)
		if err != nil {
			return err
		}

		_, err = io.Copy(f, content.NewReader(ra))
		return err
	},
}

func resolveGetDigest(ctx context.Context, desc ocispec.Descriptor, clicontext *cli.Context, store content.Store) (ocispec.Descriptor, error) {
	var (
		getIndex    = clicontext.Bool("index")
		getManifest = clicontext.Bool("manifest")
		getConfig   = clicontext.Bool("config")
		layerI      = clicontext.Int("layer")
		manifestI   = clicontext.Int("index-manifest")
	)
	switch desc.MediaType {
	case images.MediaTypeDockerSchema2Manifest, ocispec.MediaTypeImageManifest:
		if getManifest {
			return desc, nil
		}
		b, err := content.ReadBlob(ctx, store, desc)
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		var manifest ocispec.Manifest
		if err := json.Unmarshal(b, &manifest); err != nil {
			return ocispec.Descriptor{}, err
		}

		if getConfig {
			return manifest.Config, nil
		}

		if len(manifest.Layers) <= layerI {
			return ocispec.Descriptor{}, fmt.Errorf("index %d does not exist in %s", layerI, desc.Digest)
		}
		return manifest.Layers[layerI], nil
	case images.MediaTypeDockerSchema2ManifestList, ocispec.MediaTypeImageIndex:
		if getIndex {
			return desc, nil
		}
		b, err := content.ReadBlob(ctx, store, desc)
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		var idx ocispec.Index
		if err := json.Unmarshal(b, &idx); err != nil {
			return ocispec.Descriptor{}, err
		}
		if len(idx.Manifests) <= manifestI {
			return ocispec.Descriptor{}, fmt.Errorf("manifest %d does not exist in %s", manifestI, desc.Digest)
		}
		return resolveGetDigest(ctx, idx.Manifests[manifestI], clicontext, store)
	default:
		return desc, nil
	}
}
