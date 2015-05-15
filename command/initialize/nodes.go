package initialize

import (
	//"errors"
	"../../utils"
	"github.com/codeskyblue/go-sh"
	"strings"
)

type Node struct {
	Id         string
	Ip         string
	Port       string
	Tag        string
	LogicMR    string
	Role       string
	MasterId   string
	SlotsRange string
	Alive      bool
	Empty      bool
	Met        bool
}

/* get nodes from osp service */
func GetNodes(service string) ([]*Node, error) {
	res, err := sh.Command("get_instance_by_service", "-i", "-p", service).Output()
	str := string(res)
	str = strings.TrimSpace(str)
	/*hostname ip port*/
	fields := utils.SplitLine(str)
	nodes := []*Node{}
	for _, line := range fields {
		xs := strings.Fields(line)
		node := Node{
			Ip:   xs[1],
			Port: xs[2],
		}
		nodes = append(nodes, &node)
	}
	return nodes, err
}
