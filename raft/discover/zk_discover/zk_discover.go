package zk_discover

import (
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/samuel/go-zookeeper/zk"
	"minerdb/minerrpc/rpc_common/constants"
	"minerdb/minerrpc/rpc_core/transport/transport_client"
	"minerdb/raft/api/jrpc/jrpc_client"
	"minerdb/raft/common/ip_kit"
	"minerdb/raft/common/resolver"
	"minerdb/raft/starter/config"
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
	fmt.Println("ZK transport_client connected successfully.")
	cleanupHook()
}

func cleanupHook() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			fmt.Println("ZK transport_client is closing.")
			conn.Close()
		}
	}()
}

// SearchLeader 用于查找leader
// 会跳过自身，如果找不到leader，则返回错误，目的是确保不会调用自身的gRPC操作
func SearchLeader(currentNode string) (string, error) {
	nodes, errNodes := searchNodes(currentNode)
	fmt.Println("searchNodes的结果为：", nodes)
	if errNodes != nil {
		return "", errNodes
	}
	for _, node := range nodes {
		grpcAddr := ip_kit.NewAddr(node, config.GrpcPort)
		fmt.Println("正在请求isLeader！！！")
		leader, err := isLeader(grpcAddr)
		fmt.Println("请求isLeader结束！！!")
		fmt.Printf("判断%v是否为leader：%v\n", leader, grpcAddr)
		if leader {
			return node, err
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
		exists, _, err := conn.Exists(subPath)
		if err != nil {
			return err
		}
		fmt.Printf("%v在zookeeper中是否存在%v\n", subPath, exists)
		if i == len(parts) {
			for i := 0; i < 5; i++ {
				fmt.Printf("暂时存储%v到zookeeper中\n", subPath)
				_, err := conn.Create(subPath, data, 1, zk.WorldACL(zk.PermAll))
				if err == zk.ErrNodeExists {
					fmt.Println("Node already exists, retrying...")
					time.Sleep(5000 * time.Millisecond)
					continue
				} else if err != nil {
					return err
				}
				break
			}
		} else {
			if !exists {
				fmt.Printf("永久存储%v到zookeeper中\n", subPath)
				_, err := conn.Create(subPath, data, 0, zk.WorldACL(zk.PermAll))
				// _, err := conn.Create(subPath, data, 0, zk.WorldACL(zk.PermAll))
				if err != nil {
					return err
				}
				fmt.Println(subPath, "创建成功!")
			}
		}
	}
	return nil
}

// isLeader 请求一个GRPC地址，并返回是否为Leader
func isLeader(addr string) (bool, error) {
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
