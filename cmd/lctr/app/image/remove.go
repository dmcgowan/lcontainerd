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

	"github.com/containerd/lcontainerd/pkg/db"
	"github.com/urfave/cli"
)

var removeCommand = cli.Command{
	Name:        "remove",
	Aliases:     []string{"rm"},
	Usage:       "remove an image",
	ArgsUsage:   "<image name> [flags]",
	Description: `Removes an image stored locally`,
	Action: func(clicontext *cli.Context) error {
		var (
			ctx = context.Background()
			ref = clicontext.Args().First()
		)
		if ref == "" {
			return fmt.Errorf("no reference given")
		}
		mdb, err := db.NewDB(clicontext.GlobalString("data-dir"))
		if err != nil {
			return err
		}

		imgdb := db.NewImageStore(mdb)
		if err := imgdb.Delete(ctx, ref); err != nil {
			mdb.Close(ctx)
			return err
		}
		if err := mdb.Close(ctx); err != nil {
			return err
		}

		fmt.Printf("%s successfully deleted\n", ref)

		return nil
	},
}
