package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

const (
	DIR_PERM  = zk.PermAll
	FILE_PERM = zk.PermAdmin | zk.PermRead | zk.PermWrite
)

func DialZk(zkAddr []string) (*zk.Conn, <-chan zk.Event, error) {
	zconn, session, err := zk.Connect(zkAddr, 10*time.Second)
	if err != nil {
		zconn.Close()
	}
	return zconn, session, err
}
