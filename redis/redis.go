package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	ErrNotCutter   = errors.New("redis: the server is not a cutter")
	ErrConnFailed  = errors.New("redis: connection error")
	ErrPingFailed  = errors.New("redis: ping error")
	ErrServer      = errors.New("redis: server error")
	ErrInvalidAddr = errors.New("redis: invalid address string")
)

const (
	SLOT_MIGRATING = "MIGRATING"
	SLOT_IMPORTING = "IMPORTING"
	SLOT_STABLE    = "STABLE"
	SLOT_NODE      = "NODE"

	NUM_RETRY     = 3
	CONN_TIMEOUT  = 1 * time.Second
	READ_TIMEOUT  = 60 * time.Second
	WRITE_TIMEOUT = 60 * time.Second
)

func dial(addr string) (redis.Conn, error) {
	inner := func(addr string) (redis.Conn, error) {
		return redis.DialTimeout("tcp", addr, CONN_TIMEOUT, READ_TIMEOUT, WRITE_TIMEOUT)
	}
	retry := NUM_RETRY
	var err error
	var resp redis.Conn
	for retry > 0 {
		resp, err = inner(addr)
		if err == nil {
			return resp, nil
		}
		retry--
	}
	return nil, err
}

/// Misc

func IsAlive(addr string) bool {
	conn, err := dial(addr)
	if err != nil {
		return false
	}
	defer conn.Close()
	resp, err := redis.String(conn.Do("PING"))
	if err != nil || resp != "PONG" {
		return false
	}
	return true
}

/// Cluster

func SetAsMasterWaitSyncDone(addr string, waitSyncDone bool) error {
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = redis.String(conn.Do("cluster", "failover", "force"))
	if err != nil {
		return err
	}

	if !waitSyncDone {
		return nil
	}

	for {
		info, err := FetchInfo(addr, "replication")
		if err == nil {
			time.Sleep(5 * time.Second)
			n, err := info.GetInt64("connected_slaves")
			if err != nil {
				continue
			}
			done := true
			for i := int64(0); i < n; i++ {
				repl := info.Get(fmt.Sprintf("slave%d", i))
				if !strings.Contains(repl, "online") {
					done = false
				}
			}
			if done {
				return nil
			}
		}
	}
	return nil
}

func ClusterNodes(addr string) (string, error) {
	inner := func(addr string) (string, error) {
		conn, err := dial(addr)
		if err != nil {
			return "", ErrConnFailed
		}
		defer conn.Close()

		resp, err := redis.String(conn.Do("cluster", "nodes"))
		if err != nil {
			return "", err
		}
		return resp, nil
	}
	retry := NUM_RETRY
	var err error
	var resp string
	for retry > 0 {
		resp, err = inner(addr)
		if err == nil {
			return resp, nil
		}
		retry--
	}
	return "", err
}

func ClusterChmod(addr, id, op string) (string, error) {
	inner := func(addr, id, op string) (string, error) {
		conn, err := dial(addr)
		if err != nil {
			return "", ErrConnFailed
		}
		defer conn.Close()

		resp, err := redis.String(conn.Do("cluster", "chmod", op, id))
		if err != nil {
			return "", err
		}
		return resp, nil
	}
	retry := NUM_RETRY
	var err error
	var resp string
	for retry > 0 {
		resp, err = inner(addr, id, op)
		if err == nil {
			return resp, nil
		}
		retry--
	}
	return "", err
}

func DisableRead(addr, id string) (string, error) {
	return ClusterChmod(addr, id, "-r")
}

func EnableRead(addr, id string) (string, error) {
	return ClusterChmod(addr, id, "+r")
}

func DisableWrite(addr, id string) (string, error) {
	return ClusterChmod(addr, id, "-w")
}

func EnableWrite(addr, id string) (string, error) {
	return ClusterChmod(addr, id, "+w")
}

func ClusterFailover(addr string) (string, error) {
	conn, err := dial(addr)
	if err != nil {
		return "", ErrConnFailed
	}
	defer conn.Close()

	// 先正常Failover试试，如果主挂了再试试Force
	resp, err := redis.String(conn.Do("cluster", "failover"))
	if err != nil {
		if strings.HasPrefix(err.Error(), "ERR Master is down or failed") {
			resp, err = redis.String(conn.Do("cluster", "failover", "force"))
		}
		if err != nil {
			return "", err
		}
	}
	// 30s
	for i := 0; i < 30; i++ {
		info, err := FetchInfo(addr, "Replication")
		if err != nil {
			return resp, err
		}
		if info.Get("role") == "slave" {
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
	return resp, nil
}

func ClusterTakeover(addr string) (string, error) {
	conn, err := dial(addr)
	if err != nil {
		return "", ErrConnFailed
	}
	defer conn.Close()

	resp, err := redis.String(conn.Do("cluster", "failover", "takeover"))
	if err != nil {
		return "", err
	}

	// 30s
	for i := 0; i < 30; i++ {
		info, err := FetchInfo(addr, "Replication")
		if err != nil {
			return resp, err
		}
		if info.Get("role") == "slave" {
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
	return resp, nil
}

func ClusterReplicate(addr, targetId string) (string, error) {
	conn, err := dial(addr)
	if err != nil {
		return "", ErrConnFailed
	}
	defer conn.Close()

	resp, err := redis.String(conn.Do("cluster", "replicate", targetId))
	if err != nil {
		return "", err
	}

	return resp, nil
}

func ClusterMeet(seedAddr, newIp string, newPort int) (string, error) {
	conn, err := redis.Dial("tcp", seedAddr)
	if err != nil {
		return "", ErrConnFailed
	}
	defer conn.Close()

	resp, err := redis.String(conn.Do("cluster", "meet", newIp, newPort))
	if err != nil {
		return "", err
	}

	return resp, nil
}

func ClusterForget(seedAddr, nodeId string) (string, error) {
	conn, err := redis.Dial("tcp", seedAddr)
	if err != nil {
		return "", ErrConnFailed
	}
	defer conn.Close()

	resp, err := redis.String(conn.Do("cluster", "forget", nodeId))
	if err != nil {
		return "", err
	}

	return resp, nil
}

func ClusterReset(addr string, hard bool) (string, error) {
	conn, err := dial(addr)
	if err != nil {
		return "", ErrConnFailed
	}
	defer conn.Close()

	flag := "soft"
	if hard {
		flag = "hard"
	}

	resp, err := redis.String(conn.Do("cluster", "reset", flag))
	if err != nil {
		return "", err
	}

	return resp, nil
}

func AddSlotRange(addr string, start, end int) (string, error) {
	conn, err := dial(addr)
	if err != nil {
		return "connect failed", ErrConnFailed
	}
	defer conn.Close()
	var resp string
	for i := start; i <= end; i++ {
		resp, err = redis.String(conn.Do("cluster", "addslots", i))
		if err != nil {
			return resp, err
		}
	}
	return resp, nil
}

func FlushAll(addr string) (string, error) {
	conn, err := dial(addr)
	if err != nil {
		return "connect failed", ErrConnFailed
	}
	defer conn.Close()
	resp, err := redis.String(conn.Do("flushall"))
	if err != nil {
		return resp, err
	}
	return resp, nil
}

/// Cluster Info
type ClusterInfo map[string]string

func FetchClusterInfo(addr string) (*ClusterInfo, error) {
	inner := func(addr string) (*ClusterInfo, error) {
		conn, err := dial(addr)
		if err != nil {
			return nil, ErrConnFailed
		}
		defer conn.Close()

		resp, err := redis.String(conn.Do("cluster", "info"))
		if err != nil {
			return nil, err
		}
		clusterinfo := map[string]string{}
		lines := strings.Split(resp, "\r\n")
		for _, line := range lines {
			xs := strings.Split(line, ":")
			if len(xs) != 2 {
				continue
			}
			key := xs[0]
			value := xs[1]
			clusterinfo[key] = value
		}

		clusterInfo := ClusterInfo(clusterinfo)
		return &clusterInfo, nil
	}
	retry := NUM_RETRY
	var err error
	var clusterInfo *ClusterInfo
	for retry > 0 {
		clusterInfo, err = inner(addr)
		if err == nil {
			return clusterInfo, err
		}
		retry--
	}
	return nil, err
}

func (info *ClusterInfo) Get(key string) string {
	return (*info)[key]
}

func (info *ClusterInfo) GetInt64(key string) (int64, error) {
	return strconv.ParseInt((*info)[key], 10, 64)
}

/// Info
type RedisInfo map[string]string

func FetchInfo(addr, section string) (*RedisInfo, error) {
	inner := func(addr, section string) (*RedisInfo, error) {
		conn, err := dial(addr)
		if err != nil {
			return nil, ErrConnFailed
		}
		defer conn.Close()

		resp, err := redis.String(conn.Do("info", section))
		if err != nil {
			return nil, err
		}
		infomap := map[string]string{}
		lines := strings.Split(resp, "\r\n")
		for _, line := range lines {
			xs := strings.Split(line, ":")
			if len(xs) != 2 {
				continue
			}
			key := xs[0]
			value := xs[1]
			infomap[key] = value
		}

		redisInfo := RedisInfo(infomap)
		return &redisInfo, nil
	}
	retry := NUM_RETRY
	var err error
	var redisInfo *RedisInfo
	for retry > 0 {
		redisInfo, err = inner(addr, section)
		if err == nil {
			return redisInfo, err
		}
		retry--
	}
	return nil, err
}

func (info *RedisInfo) Get(key string) string {
	return (*info)[key]
}

func (info *RedisInfo) GetInt64(key string) (int64, error) {
	return strconv.ParseInt((*info)[key], 10, 64)
}

/// Migrate

func SetSlot(addr string, slot int, action, toId string) error {
	conn, err := dial(addr)
	if err != nil {
		return ErrConnFailed
	}
	defer conn.Close()

	if action == SLOT_STABLE {
		_, err = redis.String(conn.Do("cluster", "setslot", slot, action))
	} else {
		_, err = redis.String(conn.Do("cluster", "setslot", slot, action, toId))
	}
	if err != nil {
		return err
	}
	return nil
}

func CountKeysInSlot(addr string, slot int) (int, error) {
	inner := func(addr string, slot int) (int, error) {
		conn, err := dial(addr)
		if err != nil {
			return 0, ErrConnFailed
		}
		defer conn.Close()

		resp, err := redis.Int(conn.Do("cluster", "countkeysinslot", slot))
		if err != nil {
			return 0, err
		}
		return resp, nil
	}
	retry := NUM_RETRY
	var err error
	var resp int
	for retry > 0 {
		resp, err = inner(addr, slot)
		if err == nil {
			return resp, nil
		}
		retry--
	}
	return 0, err
}

func GetKeysInSlot(addr string, slot, num int) ([]string, error) {
	inner := func(addr string, slot, num int) ([]string, error) {
		conn, err := dial(addr)
		if err != nil {
			return nil, ErrConnFailed
		}
		defer conn.Close()

		resp, err := redis.Strings(conn.Do("cluster", "getkeysinslot", slot, num))
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	retry := NUM_RETRY
	var err error
	var resp []string
	for retry > 0 {
		resp, err = inner(addr, slot, num)
		if err == nil {
			return resp, nil
		}
		retry--
	}
	return nil, err
}
