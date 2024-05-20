package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/narvikd/errorskit"
	"log"
	"minerdb/minerrpc/rpc_core/transport/transport_client"
	"minerdb/raft/api/jrpc/jrpc_client"
	"minerdb/raft/cluster/consensus/fsm"
	"minerdb/raft/discover"
	"time"
)

const (
	errDBCluster      = "consensus returned an error when trying to Apply an order."
	errGrpcTalkLeader = "failed to get an ok response from the Leader via grpc"
)

func Execute(consensus *raft.Raft, payload *fsm.Payload) error {
	payloadData, errMarshal := json.Marshal(&payload)
	if errMarshal != nil {
		return errorskit.Wrap(errMarshal, "couldn't marshal data to send it to the DB cluster")
	}
	if consensus.State() != raft.Leader {
		return forwardLeaderFuture(consensus, payload)
	}
	return ApplyLeaderFuture(consensus, payloadData)
}

// ApplyLeaderFuture 在leader中apply一个cmd，只有当前node为leader才能执行
func ApplyLeaderFuture(consensus *raft.Raft, payloadData []byte) error {
	const timeout = 500 * time.Millisecond
	if consensus.State() != raft.Leader {
		return errors.New("node is not a leader")
	}
	future := consensus.Apply(payloadData, timeout)
	if future.Error() != nil {
		return errorskit.Wrap(future.Error(), errDBCluster+" At future")
	}
	response := future.Response().(*fsm.ApplyRes)
	if response.Error != nil {
		return errorskit.Wrap(response.Error, errDBCluster+" At response")
	}
	return nil
}

func forwardLeaderFuture(consensus *raft.Raft, payload *fsm.Payload) error {
	_, leaderID := consensus.LeaderWithID()
	if string(leaderID) == "" {
		return errors.New("leader id was empty")
	}
	leaderGrpcAddr := discover.NewGrpcAddress(string(leaderID))
	log.Printf("[proto] payload for leader received in this node, forwarding to leader '%s' @ '%s'\n",
		leaderID, leaderGrpcAddr,
	)
	payloadData, errMarshal := json.Marshal(&payload)
	if errMarshal != nil {
		return errorskit.Wrap(errMarshal, "couldn't marshal data to send it to the Leader's DB cluster")
	}
	fmt.Println("forwardLeaderFuture创建了jrpc请求地址为：", leaderGrpcAddr)
	client := transport_client.NewDefaultSocketClientWithAimIp(leaderGrpcAddr)
	proxy := transport_client.NewRpcClientProxy(client)
	clientService := proxy.NewProxyInstance(&jrpc_client.ClientService{}).(*jrpc_client.ClientService)
	errTalk := clientService.ExecuteOnLeader(payloadData)
	if errTalk != nil {
		return errorskit.Wrap(errTalk, errGrpcTalkLeader)
	}
	return nil
}

func ConsensusJoin(nodeID string, nodeConsensusAddr string, leaderGrpcAddr string) error {
	fmt.Println("ConsensusJoin创建了jrpc请求地址为：", leaderGrpcAddr)
	client := transport_client.NewDefaultSocketClientWithAimIp(leaderGrpcAddr)
	proxy := transport_client.NewRpcClientProxy(client)
	clientService := proxy.NewProxyInstance(&jrpc_client.ClientService{}).(*jrpc_client.ClientService)
	errTalk := clientService.ConsensusJoin(nodeID, nodeConsensusAddr)
	if errTalk != nil {
		return errorskit.Wrap(errTalk, errGrpcTalkLeader)
	}
	return nil
}

func ConsensusRemove(nodeID string, leaderGrpcAddr string) error {
	fmt.Println("ConsensusRemove创建了jrpc请求地址为：", leaderGrpcAddr)
	client := transport_client.NewDefaultSocketClientWithAimIp(leaderGrpcAddr)
	proxy := transport_client.NewRpcClientProxy(client)
	clientService := proxy.NewProxyInstance(&jrpc_client.ClientService{}).(*jrpc_client.ClientService)
	errTalk := clientService.ConsensusRemove(nodeID)
	if errTalk != nil {
		return errorskit.Wrap(errTalk, errGrpcTalkLeader)
	}
	return nil
}

// RequestNodeReinstall 被调用以防止节点被卡住太久的情况
func RequestNodeReinstall(nodeGrpcAddr string) error {
	fmt.Println("RequestNodeReinstall创建了jrpc请求地址为：", nodeGrpcAddr)
	client := transport_client.NewDefaultSocketClientWithAimIp(nodeGrpcAddr)
	proxy := transport_client.NewRpcClientProxy(client)
	clientService := proxy.NewProxyInstance(&jrpc_client.ClientService{}).(*jrpc_client.ClientService)
	return clientService.ReinstallNode()
}
