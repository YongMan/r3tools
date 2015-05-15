package initialize

import (
	"../../redis"
	"fmt"
)

func IsAlive(node *Node) bool {
	addr := fmt.Sprintf("%s:%s", node.Ip, node.Port)
	return redis.IsAlive(addr)
}

func IsEmpty(node *Node) bool {
	return true
}

func Reset(node *Node) error {
	return nil
}

func ClusterNodes(node *Node) (string, error) {
	addr := fmt.Sprintf("%s:%s", node.Ip, node.Port)
	res, err := redis.ClusterNodes(addr)
	return res, err
}
