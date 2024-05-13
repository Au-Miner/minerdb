package mdnsdiscover

import (
	"errors"
	"github.com/hashicorp/raft"
	"github.com/narvikd/errorskit"
	"github.com/narvikd/mdns"
	"jdb/api/proto"
	"jdb/api/proto/protoclient"
	"jdb/internal/config"
	"jdb/pkg/ipkit"
	"jdb/pkg/resolver"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	// 用于发现的服务名称标识符
	serviceName       = "_jdb._tcp"
	ErrLeaderNotFound = "couldn't find a leader"
)

// ServeAndBlock 使用给定的节点ID和端口创建一个新的发现服务，无限期地阻塞
func ServeAndBlock(nodeID string, port int) {
	const errGen = "Discover serve and block: "
	info := []string{"jdb Discover"}
	ip, errGetIP := getIP(nodeID)
	if errGetIP != nil {
		errorskit.FatalWrap(errGetIP, errGen)
	}
	// 创建 mDNS 服务
	service, errService := mdns.NewMDNSService(nodeID, serviceName, "", "", port, []net.IP{ip}, info)
	if errService != nil {
		errorskit.FatalWrap(errService, errGen+"discover service")
	}
	// 创建 mDNS 服务器
	server, errServer := mdns.NewServer(&mdns.Config{Zone: service})
	if errServer != nil {
		errorskit.FatalWrap(errService, errGen+"discover server")
	}
	// 在函数返回时会关闭服务器，但由于主协程被阻塞，函数实际上不会返回
	defer func(server *mdns.Server) {
		_ = server.Shutdown()
	}(server)
	// 无限期阻塞
	select {}
}

// SearchLeader 用于查找leader
// 会跳过自身，如果找不到leader，则返回错误，目的是确保不会调用自身的gRPC操作
func SearchLeader(currentNode string) (string, error) {
	nodes, errNodes := searchNodes(currentNode)
	if errNodes != nil {
		return "", errNodes
	}
	for _, node := range nodes {
		grpcAddr := ipkit.NewAddr(node, config.GrpcPort)
		leader, err := isLeader(grpcAddr)
		if err != nil {
			errorskit.LogWrap(err, "couldn't contact node while searching for leaders")
			continue
		}
		if leader {
			return node, nil
		}
	}
	return "", errors.New(ErrLeaderNotFound)
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

func getIP(nodeID string) (net.IP, error) {
	hosts, errLookup := net.LookupHost(nodeID)
	if errLookup != nil {
		return nil, errorskit.Wrap(errLookup, "couldn't lookup host")
	}
	return net.ParseIP(hosts[0]), nil
}

// searchNodes 返回所有发现的节点的列表，不包括作为参数传递的节点
func searchNodes(currentNode string) ([]string, error) {
	hosts := make(map[string]bool)
	var lastError error
	// 尝试发现节点3次，以在第一次扫描中添加任何丢失的节点
	for i := 0; i < 3; i++ {
		hostsQuery, err := query()
		if err != nil {
			log.Println(err)
			lastError = err
			continue
		}
		for _, host := range hostsQuery {
			// 一些linux系统它返回"$name."
			host = strings.ReplaceAll(host, ".", "")
			hosts[host] = true
		}
		time.Sleep(100 * time.Millisecond)
	}
	// 跳过当前node.
	result := make([]string, 0, len(hosts))
	for host := range hosts {
		if currentNode == host {
			continue
		}
		result = append(result, host)
	}
	return result, lastError
}

// query 发送一个mDNS query来发现jdb nodes，并返回hosts.
func query() ([]string, error) {
	var mu sync.Mutex
	var hosts []string
	entriesCh := make(chan *mdns.ServiceEntry, 4)
	go func() {
		for entry := range entriesCh {
			mu.Lock()
			hosts = append(hosts, entry.Host)
			mu.Unlock()
		}
	}()
	params := mdns.DefaultParams(serviceName)
	params.DisableIPv6 = true
	params.Entries = entriesCh
	defer close(entriesCh)
	err := mdns.Query(params)
	if err != nil {
		return nil, errorskit.Wrap(err, "discover search")
	}
	mu.Lock()
	defer mu.Unlock()
	return hosts, nil
}

// isLeader 请求一个GRPC地址，并返回是否为Leader
func isLeader(addr string) (bool, error) {
	conn, errConn := protoclient.NewConnection(addr)
	if errConn != nil {
		return false, errConn
	}
	defer conn.Cleanup()
	res, errTalk := conn.Client.IsLeader(conn.Ctx, &proto.Empty{})
	if errTalk != nil {
		return false, errorskit.Wrap(errTalk, "failed to get an ok response from the Node via grpc")
	}
	return res.IsLeader, nil
}
