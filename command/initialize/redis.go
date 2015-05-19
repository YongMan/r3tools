package initialize

import (
	"../../redis"
	"fmt"
	"strconv"
)

func isAlive(node *Node) bool {
	addr := fmt.Sprintf("%s:%s", node.Ip, node.Port)
	return redis.IsAlive(addr)
}

func isEmpty(node *Node) bool {
	return true
}

func isMaster(node *Node) bool {
	addr := fmt.Sprintf("%s:%s", node.Ip, node.Port)
	ri, _ := redis.FetchInfo(addr, "Replication")
	role := ri.Get("role")
	return role == "master"
}

func resetNodes(nodes []*Node) (string, error) {
	var resp string
	var err error
	for _, node := range nodes {
		addr := fmt.Sprintf("%s:%s", node.Ip, node.Port)
		if isMaster(node) {
			resp, err = redis.FlushAll(addr)
			if err != nil {
				return resp, err
			}
		}
		resp, err = redis.ClusterReset(addr, false)
		if err != nil {
			return resp, err
		}
	}
	return resp, nil
}

func clusterNodes(node *Node) (string, error) {
	addr := fmt.Sprintf("%s:%s", node.Ip, node.Port)
	resp, err := redis.ClusterNodes(addr)
	return resp, err
}

func meetEach(nodes []*Node) {
	for _, n1 := range nodes {
		for _, n2 := range nodes {
			if n1 != n2 {
				addr := fmt.Sprintf("%s:%s", n1.Ip, n1.Port)
				newPort, _ := strconv.Atoi(n2.Port)
				redis.ClusterMeet(addr, n2.Ip, newPort)
			}
		}
	}
}

func addSlotRange(node *Node) (string, error) {
	addr := fmt.Sprintf("%s:%s", node.Ip, node.Port)
	var start int
	var end int
	fmt.Sscanf(node.SlotsRange, "%d-%d", &start, &end)
	return redis.AddSlotRange(addr, start, end)
}

func setReplicas(slaves []*Node) (string, error) {
	var resp string
	var err error
	for _, slave := range slaves {
		addr := fmt.Sprintf("%s:%s", slave.Ip, slave.Port)
		resp, err = redis.ClusterReplicate(addr, slave.MasterId)
		if err != nil {
			return resp, err
		}
	}
	return resp, nil
}

func checkClusterInfo(nodes []*Node) bool {
	var (
		clusterstate           string
		cluster_slots_assigned string
		cluster_slots_ok       string
		cluster_slots_pfail    string
		cluster_slots_fail     string
		cluster_known_nodes    string
		cluster_size           string
	)

	for idx, node := range nodes {
		addr := fmt.Sprintf("%s:%s", node.Ip, node.Port)
		ci, err := redis.FetchClusterInfo(addr)
		if err != nil {
			return false
		}
		if idx == 0 {
			clusterstate = ci.Get("cluster_state")
			cluster_slots_assigned = ci.Get("cluster_slots_assigned")
			cluster_slots_ok = ci.Get("cluster_slots_ok")
			cluster_slots_pfail = ci.Get("cluster_slots_pfail")
			cluster_slots_fail = ci.Get("cluster_slots_fail")
			cluster_known_nodes = ci.Get("cluster_known_nodes")
			cluster_size = ci.Get("cluster_size")
		} else {
			if clusterstate != ci.Get("cluster_state") ||
				cluster_slots_assigned != ci.Get("cluster_slots_assigned") ||
				cluster_slots_ok != ci.Get("cluster_slots_ok") ||
				cluster_slots_pfail != ci.Get("cluster_slots_pfail") ||
				cluster_slots_fail != ci.Get("cluster_slots_fail") ||
				cluster_known_nodes != ci.Get("cluster_known_nodes") ||
				cluster_size != ci.Get("cluster_size") {
				return false
			}
		}
	}
	return true
}
