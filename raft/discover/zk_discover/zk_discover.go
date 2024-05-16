package zk_discover

import (
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/samuel/go-zookeeper/zk"
	"jdb/jrpc/rpc_common/constants"
	"jdb/jrpc/rpc_core/transport/client"
	"jdb/raft/api/jrpc/jrpc_client"
	"jdb/raft/common/ip_kit"
	"jdb/raft/common/resolver"
	"jdb/raft/starter/config"
	"os"
	"os/signal"
	"strings"
	"time"
)

var (
	conn              *zk.Conn
	ErrLeaderNotFound = "couldn't find a leader"
)

func init() {
	var err error
	conn, _, err = zk.Connect([]string{constants.ZK_SERVER_ADDRESS}, constants.ZK_SESSION_TIMEOUT)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Zookeeper: %v", err))
	}
	fmt.Println("ZK client connected successfully.")
	cleanupHook()
}

func cleanupHook() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			fmt.Println("ZK client is closing.")
			conn.Close()
		}
	}()
}

// SearchLeader 用于查找leader
// 会跳过自身，如果找不到leader，则返回错误，目的是确保不会调用自身的gRPC操作
func SearchLeader(currentNode string) (string, error) {
	nodes, errNodes := searchNodes(currentNode)
	if errNodes != nil {
		return "", errNodes
	}
	for _, node := range nodes {
		grpcAddr := ip_kit.NewAddr(node, config.GrpcPort)
		leader := isLeader(grpcAddr)
		fmt.Printf("判断%v是否为leader：%v\n", leader, grpcAddr)
		if leader {
			return node, nil
		}
	}
	return "", errors.New(ErrLeaderNotFound)
}

func searchNodes(currentNode string) ([]string, error) {
	ips, _, err := conn.Children("/ips")
	if err != nil {
		return nil, fmt.Errorf("failed to get children for %s: %v", "ips", err)
	}
	for idx, ip := range ips {
		if ip == currentNode {
			ips = append(ips[:idx], ips[idx+1:]...)
		}
	}
	return ips, nil
}

func RegisterNode(addr string) error {
	fmt.Println("准备注册addr：", addr)
	servicePath := fmt.Sprintf("%s/%s", "/ips", addr)
	if err := createPath(conn, servicePath, []byte{}); err != nil {
		return err
	}
	return nil
}

func createPath(conn *zk.Conn, path string, data []byte) error {
	parts := strings.Split(path, "/")
	for i := 2; i <= len(parts); i++ {
		subPath := strings.Join(parts[:i], "/")
		fmt.Println("正在查找subPath是否存在：", subPath)
		exists, _, err := conn.Exists(subPath)
		if err != nil {
			return err
		}
		// var flag int32
		// if i == len(parts) {
		// 	flag = 1
		// } else {
		// 	flag = 0
		// }
		if !exists {
			_, err := conn.Create(subPath, data, 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// isLeader 请求一个GRPC地址，并返回是否为Leader
func isLeader(addr string) bool {
	fmt.Println("isLeader创建了jrpc请求地址为：", addr)
	client := transport_client.NewDefaultSocketClientWithAimIp(addr)
	proxy := transport_client.NewRpcClientProxy(client)
	clientService := proxy.NewProxyInstance(&jrpc_client.ClientService{}).(*jrpc_client.ClientService)
	return clientService.IsLeader()
}

// SearchAliveNodes will skip currentNodeID.
func SearchAliveNodes(consensus *raft.Raft, currentNodeID string) []raft.Server {
	const timeout = 300 * time.Millisecond
	var alive []raft.Server
	liveCfg := consensus.GetConfiguration().Configuration()
	cfg := liveCfg.Clone() // Clone CFG to not keep calling it in the for, in case the num of servers is very large
	for _, srv := range cfg.Servers {
		srvID := string(srv.ID)
		if currentNodeID == srvID {
			continue
		}
		if resolver.IsHostAlive(srvID, timeout) {
			alive = append(alive, srv)
		}
	}
	return alive
}
