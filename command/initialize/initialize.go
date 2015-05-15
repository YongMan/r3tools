package initialize

import (
	"../../utils"
	"fmt"
	"github.com/codegangsta/cli"
	"os"
	"strings"
)

var (
	flags = []cli.Flag{
		cli.StringFlag{"s,service", "", "the service to initialize"},
		cli.StringFlag{"l,logic", "", "logic machine rooms list"},
	}

	Command = cli.Command{
		Name:   "init",
		Usage:  "initialize a new empty cluster",
		Action: action,
		Flags:  flags,
	}
)

func action(c *cli.Context) {
	s := c.String("s")
	if s == "" {
		fmt.Println("service must be assigned")
		os.Exit(-1)
	}
	l := c.String("l")
	if l == "" {
		fmt.Println("-l logic machine room must be assigned")
		os.Exit(-1)
	}
	allnodes := []*Node{}
	rooms := strings.Split(l, ",")
	for _, room := range rooms {
		service_name := fmt.Sprintf("%s.osp.%s", s, room)
		nodes, err := GetNodes(service_name)
		if err == nil {
			for _, n := range nodes {
				/* set logic mr */
				n.LogicMR = room
			}
		}
		allnodes = append(allnodes, nodes...)
	}
	/* done get allnodes */
	/* check nodes state */
	for _, node := range allnodes {
		node.Alive = IsAlive(node)
		fmt.Println(node.Ip, " ", node.Port, " ", node.Alive)
	}

	/* check and set state */
	for _, node := range allnodes {
		checkAndSetState(node)
	}

	/* validate the state and continue */
	if validateProcess(allnodes) == false {
		fmt.Println("Not all nodes have the right status")
		os.Exit(-1)
	}

}

func validateProcess(nodes []*Node) bool {
	return true
}

func checkAndSetState(node *Node) {
	if node.Alive == false {
		return
	}
	info, err := ClusterNodes(node)
	if err != nil {
		return
	}
	if len(utils.SplitLine(info)) > 1 {
		node.Met = true
		return
	}

	//set info state
	cols := strings.Fields(info)
	if len(cols) != 8 {
		return
	}
	node.Id = cols[0]
	role := cols[2]
	node.Role = strings.Split(role, ",")[1]

}
