package control

import (
	zk "../../zookeeper"
	"fmt"
	gozk "github.com/samuel/go-zookeeper/zk"
	"testing"
)

func TestGetLeader(t *testing.T) {
	addrs := []string{}
	addrs = append(addrs, "127.0.0.1:2181")
	conn, session, err := zk.DialZk(addrs)
	if err != nil {
		fmt.Println("connect failed", err)
	}
	for {
		event := <-session
		if event.State == gozk.StateConnected {
			fmt.Println(event)
			break
		}
	}

	meta := Meta{
		CcPath: "/r3/app/ksarch-test/controller",
		ZkConn: conn,
	}

	ip, port, err := meta.GetLeaderController()
	fmt.Println(ip, " ", port, err)
	conn.Close()
}
