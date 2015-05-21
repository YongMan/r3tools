package control

import (
	//zk "../../zookeeper"
	"encoding/json"
	gozk "github.com/samuel/go-zookeeper/zk"
	"strconv"
	"strings"
)

type Meta struct {
	CcPath string
	ZkConn *gozk.Conn
}
type Controller struct {
	Ip       string `json:"Ip", omitempty`
	HttpPort int    `json:"HttpPort", ompitempty`
	WsPort   int    `json:"WsPort", omitempty`
	Region   string `json:"Region", omitempty`
}

func (m *Meta) GetLeaderController() (string, int, error) {
	conn := m.ZkConn

	children, _, err := conn.Children(m.CcPath)

	if err != nil {
		return "", -1, err
	}

	/*choose leader with small id*/
	minSeq := -1
	var leader string
	for _, child := range children {
		xs := strings.Split(child, "_")
		seq, _ := strconv.Atoi(xs[2])
		if minSeq < 0 {
			minSeq = seq
			leader = child
		}
		if seq < minSeq {
			minSeq = seq
			leader = child
		}
	}

	info, _, err := conn.Get(m.CcPath + "/" + leader)
	if err != nil {
		return "", -1, err
	}
	ctrl := Controller{}
	err = json.Unmarshal(info, &ctrl)
	if err != nil {
		return "", -1, err
	}
	return ctrl.Ip, ctrl.HttpPort, nil
}
