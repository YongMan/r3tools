package control

import (
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/nemith/goline"
)

var (
	flags = []cli.Flag{}

	Command = cli.Command{
		Name:   "ctrl",
		Usage:  "control the cluster",
		Action: action,
		Flags:  flags,
	}
)

func helpHandler(l *goline.GoLine) (bool, error) {
	fmt.Println("\nHelp!")
	return false, nil
}

func action(c *cli.Context) {
	gl := goline.NewGoLine(goline.StringPrompt("prompt> "))

	gl.AddHandler('?', helpHandler)

	for {
		data, err := gl.Line()
		if err != nil {
			if err == goline.UserTerminatedError {
				fmt.Println("\nUser terminated.")
				return
			} else {
				panic(err)
			}
		}

		fmt.Printf("\nGot: '%s' (%d)\n", data, len(data))

		if data == "exit" || data == "quit" {
			fmt.Println("Exiting.")
			return
		}

	}
}
