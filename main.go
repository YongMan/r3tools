package main

import (
	"./command/initialize"
	"github.com/codegangsta/cli"
	"os"
)

func main() {
	cmds := []cli.Command{
		initialize.Command,
	}

	app := cli.NewApp()
	app.Name = "r3tools"
	app.Usage = "Redis3.0 cluster tools"
	app.Commands = cmds
	app.Version = "0.0.1"
	app.Run(os.Args)
}
