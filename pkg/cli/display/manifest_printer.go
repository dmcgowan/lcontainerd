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

package display

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
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// TreeFormat is used to format tree based output using 4 values.
// Each value must display with the same total width to format correctly.
//
// MiddleDrop is used to show a child element which is not the last child
// LastDrop is used to show the last child element
// SkipLine is used for displaying data from a previous child before the next child
// Spacer is used to display child data for the last child
type TreeFormat struct {
	MiddleDrop string
	LastDrop   string
	SkipLine   string
	Spacer     string
}

// LineTreeFormat uses line drawing characters to format a tree
//
// TreeRoot
// ├── First child       # MiddleDrop =  "├── "
// │   Skipped line      # SkipLine = "│   "
// └── Last child        # LastDrop = "└── "
//     └── Only child    # Spacer="    ", LastDrop = "└── "
var LineTreeFormat = TreeFormat{
	MiddleDrop: "├── ",
	LastDrop:   "└── ",
	SkipLine:   "│   ",
	Spacer:     "    ",
}

type Printer struct {
	verbose bool
	w       io.Writer
	format  TreeFormat
}

type PrintOpt func(*Printer)

func Verbose(p *Printer) {
	p.verbose = true
}

func WithWriter(w io.Writer) PrintOpt {
	return func(p *Printer) {
		p.w = w
	}
}

func WithFormat(format TreeFormat) PrintOpt {
	return func(p *Printer) {
		p.format = format
	}
}

func NewPrinter(opts ...PrintOpt) *Printer {
	p := &Printer{
		verbose: false,
		w:       os.Stdout,
		format:  LineTreeFormat,
	}
	for _, opt := range opts {
		opt(p)
	}

	return p
}

// PrintImageTree prints an image and all its sub elements
func (p *Printer) PrintImageTree(ctx context.Context, img images.Image, store content.Store) error {
	fmt.Fprintln(p.w, img.Name)
	subchild := p.format.SkipLine
	fmt.Fprintf(p.w, "%s Created: %s\n", subchild, img.CreatedAt)
	fmt.Fprintf(p.w, "%s Updated: %s\n", subchild, img.UpdatedAt)
	for k, v := range img.Labels {
		fmt.Fprintf(p.w, "%s Label %q: %q\n", subchild, k, v)
	}
	return p.printManifestTree(ctx, img.Target, store, p.format.LastDrop, p.format.Spacer)
}

// PrintManifestTree prints a manifest and all its sub elements
func (p *Printer) PrintManifestTree(ctx context.Context, desc ocispec.Descriptor, store content.Store) error {
	// start displaying tree from the root descriptor perspective, which is a single child view
	return p.printManifestTree(ctx, desc, store, p.format.LastDrop, p.format.Spacer)
}

func (p *Printer) printManifestTree(ctx context.Context, desc ocispec.Descriptor, store content.Store, prefix, childprefix string) error {
	subprefix := childprefix + p.format.MiddleDrop
	subchild := childprefix + p.format.SkipLine
	fmt.Fprintf(p.w, "%s%s @%s (%d bytes)\n", prefix, desc.MediaType, desc.Digest, desc.Size)

	if desc.Platform != nil && desc.Platform.Architecture != "" {
		// TODO: Use containerd platform library to format
		fmt.Fprintf(p.w, "%s Platform: %s/%s\n", subchild, desc.Platform.OS, desc.Platform.Architecture)
	}
	b, err := content.ReadBlob(ctx, store, desc)
	if err != nil {
		return err
	}
	if err := p.showContent(ctx, store, desc, subchild); err != nil {
		return err
	}

	switch desc.MediaType {
	case images.MediaTypeDockerSchema2Manifest, ocispec.MediaTypeImageManifest:
		var manifest ocispec.Manifest
		if err := json.Unmarshal(b, &manifest); err != nil {
			return err
		}

		if len(manifest.Layers) == 0 {
			subprefix = childprefix + p.format.LastDrop
			subchild = childprefix + p.format.Spacer
		}
		fmt.Fprintf(p.w, "%s%s @%s (%d bytes)\n", subprefix, manifest.Config.MediaType, manifest.Config.Digest, manifest.Config.Size)

		if err := p.showContent(ctx, store, manifest.Config, subchild); err != nil {
			return err
		}

		for i := range manifest.Layers {
			if len(manifest.Layers) == i+1 {
				subprefix = childprefix + p.format.LastDrop
				//subchild = childprefix + p.format.Spacer
			}
			fmt.Fprintf(p.w, "%s%s @%s (%d bytes)\n", subprefix, manifest.Layers[i].MediaType, manifest.Layers[i].Digest, manifest.Layers[i].Size)
		}

	case images.MediaTypeDockerSchema2ManifestList, ocispec.MediaTypeImageIndex:
		var idx ocispec.Index
		if err := json.Unmarshal(b, &idx); err != nil {
			return err
		}

		for i := range idx.Manifests {
			if len(idx.Manifests) == i+1 {
				subprefix = childprefix + p.format.LastDrop
				subchild = childprefix + p.format.Spacer
			}
			if err := p.printManifestTree(ctx, idx.Manifests[i], store, subprefix, subchild); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *Printer) showContent(ctx context.Context, store content.Store, desc ocispec.Descriptor, prefix string) error {
	if p.verbose {
		info, err := store.Info(ctx, desc.Digest)
		if err != nil {
			return err
		}
		if len(info.Labels) > 0 {
			fmt.Fprintf(p.w, "%s┌────────Labels─────────\n", prefix)
			for k, v := range info.Labels {
				fmt.Fprintf(p.w, "%s│%q: %q\n", prefix, k, v)
			}
			fmt.Fprintf(p.w, "%s└───────────────────────\n", prefix)
		}
	}
	if p.verbose && strings.HasSuffix(desc.MediaType, "json") {
		// Print content for config
		cb, err := content.ReadBlob(ctx, store, desc)
		if err != nil {
			return err
		}
		dst := bytes.NewBuffer(nil)
		json.Indent(dst, cb, prefix+"│", "   ")
		fmt.Fprintf(p.w, "%s┌────────Content────────\n", prefix)
		fmt.Fprintf(p.w, "%s│%s\n", prefix, strings.TrimSpace(dst.String()))
		fmt.Fprintf(p.w, "%s└───────────────────────\n", prefix)
	}
	return nil
}
