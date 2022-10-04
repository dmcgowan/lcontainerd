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

package progress

import (
	"context"
	"io"

	transfertypes "github.com/containerd/containerd/api/types/transfer"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/pkg/transfer"
	"google.golang.org/protobuf/proto"
)

func ForwardProto(ctx context.Context, out io.Writer) transfer.ProgressFunc {
	return func(p transfer.Progress) {
		b, err := proto.Marshal(&transfertypes.Progress{
			Event:    p.Event,
			Name:     p.Name,
			Parents:  p.Parents,
			Progress: p.Progress,
			Total:    p.Total,
		})
		if err != nil {
			log.G(ctx).WithError(err).Warnf("event could not be marshaled: %v/%v", p.Event, p.Name)
			return
		}
		if _, err := out.Write(b); err != nil {
			log.G(ctx).WithError(err).Warnf("event could not be written: %v/%v", p.Event, p.Name)
		}
	}
}
