package zookeeper

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"testing"
)

func TestDialZk(t *testing.T) {
	addrs := []string{}
	addrs = append(addrs, "127.0.0.1:2181")
	conn, session, err := DialZk(addrs)
	if err != nil {
		fmt.Println("connect failed", err)
	}
	for {
		event := <-session
		if event.State == zk.StateConnected {
			fmt.Println(event)
			break
		}
	}
	res, _, _ := GetLeaderController(conn, "/r3/app/ksarch-test/controller")
	fmt.Println(res)

	conn.Close()
}
