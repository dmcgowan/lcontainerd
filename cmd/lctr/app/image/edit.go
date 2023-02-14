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

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/lcontainerd/pkg/db"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/urfave/cli"
)

var descriptorFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "file",
		Usage: "Input file, use (-) for stdin",
	},
	cli.StringFlag{
		Name:  "media-type",
		Usage: "Media type",
	},
	cli.StringFlag{
		Name:  "from-image",
		Usage: "Use the descriptor from an existing image",
	},
	cli.StringSliceFlag{
		Name:  "annotation",
		Usage: "Annotations to apply to descriptor",
	},
	cli.StringSliceFlag{
		Name:  "platform",
		Usage: "Platform to apply to descriptor",
	},
}

var createCommand = cli.Command{
	Name:        "create",
	Usage:       "create a new image",
	ArgsUsage:   "<image-name> <config-file> <config-type> [flags]",
	Description: `Create a new image locally`,
	Flags: append(descriptorFlags,
		cli.StringSliceFlag{
			Name:  "manifest-annotation",
			Usage: "Annotations to add to the manifest",
		},
		cli.StringSliceFlag{
			Name:  "label",
			Usage: "Labels to add to the image",
		},
	),
	Action: func(clicontext *cli.Context) error {
		var (
			ctx = context.Background()
			ref = clicontext.Args().First()
		)

		mdb, err := db.NewDB(clicontext.GlobalString("data-dir"))
		if err != nil {
			return err
		}
		defer mdb.Close(ctx)

		imgdb := db.NewImageStore(mdb)
		if _, err := imgdb.Get(ctx, ref); err == nil {
			return fmt.Errorf("image already exists, use image-append to make changes")
		}

		desc, err := getDescriptor(ctx, clicontext, mdb.ContentStore(), imgdb)
		if err != nil {
			return err
		}

		annotations, err := keyValueArgs(clicontext.StringSlice("manifest-annotation"), "")
		if err != nil {
			return err
		}

		labels, err := keyValueArgs(clicontext.StringSlice("label"), "true")
		if err != nil {
			return err
		}

		var copts []content.Opt
		var manifest interface{}
		var target ocispec.Descriptor
		if desc == nil {
			target.MediaType = "application/vnd.oci.image.index.v1+json"
			manifest = ocispec.Index{
				Versioned: specs.Versioned{
					SchemaVersion: 2,
				},
				MediaType:   target.MediaType,
				Annotations: annotations,
			}
		} else {
			target.MediaType = "application/vnd.oci.image.manifest.v1+json"
			manifest = ocispec.Manifest{
				Versioned: specs.Versioned{
					SchemaVersion: 2,
				},
				MediaType:   target.MediaType,
				Config:      *desc,
				Annotations: annotations,
			}
			copts = append(copts, content.WithLabels(getChildGCLabels(*desc, 0, nil)))
		}

		b, err := json.Marshal(manifest)
		if err != nil {
			return err
		}

		target.Size = int64(len(b))
		target.Digest = digest.FromBytes(b)

		// Add content label
		if err := content.WriteBlob(ctx, mdb.ContentStore(), target.Digest.String()+"-ingest", bytes.NewReader(b), target, copts...); err != nil {
			return fmt.Errorf("failed to write manifest: %w", err)
		}

		_, err = imgdb.Create(ctx, images.Image{
			Name:   ref,
			Target: target,
			Labels: labels,
		})

		return err
	},
}

var appendCommand = cli.Command{
	Name:        "append",
	Usage:       "create a new image",
	ArgsUsage:   "<image-name> [flags]",
	Description: `appends descriptor image locally`,
	Flags:       descriptorFlags,
	Action: func(clicontext *cli.Context) error {
		var (
			ctx = context.Background()
			ref = clicontext.Args().First()
		)
		mdb, err := db.NewDB(clicontext.GlobalString("data-dir"))
		if err != nil {
			return err
		}
		defer mdb.Close(ctx)

		imgdb := db.NewImageStore(mdb)
		img, err := imgdb.Get(ctx, ref)
		if err != nil {
			return fmt.Errorf("image could not be retrieved: %w", err)
		}

		desc, err := getDescriptor(ctx, clicontext, mdb.ContentStore(), imgdb)
		if err != nil {
			return err
		}

		if desc == nil {
			return fmt.Errorf("no object specified to append to image")
		}

		info, err := mdb.ContentStore().Info(ctx, img.Target.Digest)
		if err != nil {
			return err
		}

		var copts []content.Opt
		var manifest interface{}
		var position int
		switch img.Target.MediaType {
		case "application/vnd.oci.image.index.v1+json":
			b, err := content.ReadBlob(ctx, mdb.ContentStore(), img.Target)
			if err != nil {
				return err
			}
			var idx ocispec.Index
			if err := json.Unmarshal(b, &idx); err != nil {
				return err
			}
			position = len(idx.Manifests)
			idx.Manifests = append(idx.Manifests, *desc)
			manifest = idx
		case "application/vnd.oci.image.manifest.v1+json":
			b, err := content.ReadBlob(ctx, mdb.ContentStore(), img.Target)
			if err != nil {
				return err
			}
			var m ocispec.Manifest
			if err := json.Unmarshal(b, &m); err != nil {
				return err
			}
			// Add 1 to position to account for config as the first element for child labeling
			position = len(m.Layers) + 1
			m.Layers = append(m.Layers, *desc)
			manifest = m
		default:
			return fmt.Errorf("media type not supported for making updates: %s", img.Target.MediaType)
		}
		copts = append(copts, content.WithLabels(getChildGCLabels(*desc, position, info.Labels)))

		b, err := json.Marshal(manifest)
		if err != nil {
			return err
		}

		img.Target.Size = int64(len(b))
		img.Target.Digest = digest.FromBytes(b)

		if err := content.WriteBlob(ctx, mdb.ContentStore(), img.Target.Digest.String()+"-ingest", bytes.NewReader(b), img.Target, copts...); err != nil {
			return err
		}
		_, err = imgdb.Update(ctx, img)

		return err
	},
}

func getDescriptor(ctx context.Context, clicontext *cli.Context, ing content.Ingester, is images.Store) (desc *ocispec.Descriptor, err error) {
	if file := clicontext.String("file"); file != "" {
		var r io.Reader
		if file == "-" {
			r = os.Stdin
		} else {
			f, err := os.Open(file)
			if err != nil {
				return nil, err
			}
			defer f.Close()
			r = f
		}

		buf := bytes.NewBuffer(nil)
		if _, err := io.Copy(buf, r); err != nil {
			return nil, err
		}

		b := buf.Bytes()
		desc = &ocispec.Descriptor{
			MediaType: clicontext.String("media-type"),
			Size:      int64(buf.Len()),
			Digest:    digest.FromBytes(b),
		}
		if desc.MediaType == "" {
			// Default?
			return nil, nil
		}
		if err := content.WriteBlob(ctx, ing, desc.Digest.String()+"-ingest", bytes.NewReader(b), *desc); err != nil {
			return nil, fmt.Errorf("failed to write file content: %w", err)
		}
	} else if img := clicontext.String("from-image"); img != "" {
		i, err := is.Get(ctx, img)
		if err != nil {
			return nil, err
		}
		desc = &i.Target
	} else {
		return nil, nil
	}

	annotations, err := keyValueArgs(clicontext.StringSlice("annotations"), "")
	if err != nil {
		return nil, err
	}
	if len(desc.Annotations) > 0 {
		for k, v := range annotations {
			desc.Annotations[k] = v
		}
	} else {
		desc.Annotations = annotations
	}

	if ps := clicontext.String("platform"); ps != "" {
		p, err := platforms.Parse(ps)
		if err != nil {
			return nil, err
		}
		desc.Platform = &p
	}

	return
}

var editImageCommand = cli.Command{
	Name:        "edit",
	Usage:       "edit image annotations",
	ArgsUsage:   "<image-name> [flags]",
	Description: `edit image annotations and updates media type`,
	Flags: []cli.Flag{
		cli.StringSliceFlag{
			Name:  "manifest-annotation",
			Usage: "Annotations to apply to the manifest",
		},
	},
	Action: func(clicontext *cli.Context) error {
		var (
			ctx = context.Background()
			ref = clicontext.Args().First()
		)
		mdb, err := db.NewDB(clicontext.GlobalString("data-dir"))
		if err != nil {
			return err
		}
		defer mdb.Close(ctx)

		imgdb := db.NewImageStore(mdb)
		img, err := imgdb.Get(ctx, ref)
		if err != nil {
			return fmt.Errorf("image could not be retrieved: %w", err)
		}

		annotations, err := keyValueArgs(clicontext.StringSlice("manifest-annotation"), "")
		if err != nil {
			return err
		}

		info, err := mdb.ContentStore().Info(ctx, img.Target.Digest)
		if err != nil {
			return err
		}

		var copts []content.Opt
		var manifest interface{}
		switch img.Target.MediaType {
		case ocispec.MediaTypeImageIndex, images.MediaTypeDockerSchema2ManifestList:
			b, err := content.ReadBlob(ctx, mdb.ContentStore(), img.Target)
			if err != nil {
				return err
			}
			var idx ocispec.Index
			if err := json.Unmarshal(b, &idx); err != nil {
				return err
			}
			for k, v := range annotations {
				if idx.Annotations == nil {
					idx.Annotations = map[string]string{}
				}
				idx.Annotations[k] = v
			}
			idx.MediaType = ocispec.MediaTypeImageIndex
			img.Target.MediaType = idx.MediaType
			manifest = idx
		case ocispec.MediaTypeImageManifest, images.MediaTypeDockerSchema2Manifest:
			b, err := content.ReadBlob(ctx, mdb.ContentStore(), img.Target)
			if err != nil {
				return err
			}
			var m ocispec.Manifest
			if err := json.Unmarshal(b, &m); err != nil {
				return err
			}
			for k, v := range annotations {
				if m.Annotations == nil {
					m.Annotations = map[string]string{}
				}
				m.Annotations[k] = v
			}
			m.MediaType = ocispec.MediaTypeImageManifest
			img.Target.MediaType = m.MediaType
			manifest = m
		default:
			return fmt.Errorf("media type not supported for making updates: %s", img.Target.MediaType)
		}
		copts = append(copts, content.WithLabels(info.Labels))

		b, err := json.Marshal(manifest)
		if err != nil {
			return err
		}

		img.Target.Size = int64(len(b))
		img.Target.Digest = digest.FromBytes(b)

		if err := content.WriteBlob(ctx, mdb.ContentStore(), img.Target.Digest.String()+"-ingest", bytes.NewReader(b), img.Target, copts...); err != nil {
			return err
		}
		_, err = imgdb.Update(ctx, img)

		return err
	},
}

func keyValueArgs(args []string, defaultValue string) (map[string]string, error) {
	if len(args) == 0 {
		return nil, nil
	}
	kvs := make(map[string]string, len(args))
	for _, arg := range args {
		parts := strings.SplitN(arg, "=", 2)
		key := parts[0]
		value := defaultValue
		if len(parts) == 2 {
			value = parts[1]
		} else if value == "" {
			return nil, fmt.Errorf("invalid key=value format: %v", arg)
		}

		kvs[key] = value
	}

	return kvs, nil
}

func getChildGCLabels(desc ocispec.Descriptor, position int, labels map[string]string) map[string]string {
	prefixes := images.ChildGCLabels(desc)
	if len(prefixes) > 0 {
		if labels == nil {
			labels = map[string]string{}
		}
		for _, key := range prefixes {
			if strings.HasSuffix(key, ".") {
				key = fmt.Sprintf("%s%d", key, position)
			}
			labels[key] = desc.Digest.String()
		}

	}
	return labels
}
