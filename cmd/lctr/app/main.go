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
	"fmt"
	"os"
	"path/filepath"

	"github.com/containerd/containerd/version"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

var extraCmds = []cli.Command{}

func init() {
	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Println(c.App.Name, version.Package, c.App.Version)
	}
}

// New returns a *cli.App instance.
func New() *cli.App {
	datadir := os.Getenv("XDG_DATA_HOME")
	if datadir == "" {
		hd, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		datadir = filepath.Join(hd, ".local", "share")
	}

	app := cli.NewApp()
	app.Name = "lctr"
	app.Version = version.Version
	app.Description = `
lctr is an unsupported debug and client for utilizing containerd libraries locally.
Because it is unsupported, the commands, options, and operations are not guaranteed
to be backward compatible or stable from release to release of the containerd project.`
	app.Usage = `
    __     __
   / /____/ /______
  / / ___/ __/ ___/
 / / /__/ /_/ /
/_/\___/\__/_/

containerd CLI for library and hacks
`
	app.EnableBashCompletion = true
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "debug",
			Usage: "enable debug output in logs",
		},
		cli.StringFlag{
			Name:  "data-dir, d",
			Usage: "data directory for all metadata",
			Value: filepath.Join(datadir, "lctr"),
		},
	}
	app.Commands = append([]cli.Command{
		pullCommand,
		pushCommand,
		listCommand,
		readCommand,
		createCommand,
		appendCommand,
		removeCommand,
	}, extraCmds...)
	app.Before = func(context *cli.Context) error {
		if context.GlobalBool("debug") {
			logrus.SetLevel(logrus.DebugLevel)
		}
		datadir := context.GlobalString("data-dir")
		if _, err := os.Stat(datadir); os.IsNotExist(err) {
			if err := os.MkdirAll(datadir, 0700); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		return nil
	}
	return app
}
