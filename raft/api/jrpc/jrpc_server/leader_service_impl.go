package jrpc_server

import (
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"jdb/raft/cluster"
)

// ExecuteOnLeader 在Raft leader上执行一个cmd
func (srv *server) ExecuteOnLeader(payloadData []byte) error {
	fmt.Println("[proto] (ExecuteOnLeader) request received, processing...")
	// 在leader中apply一个cmd，只有当前node为leader才能执行
	errExecute := cluster.ApplyLeaderFuture(srv.Node.Consensus, payloadData)
	if errExecute != nil {
		return errExecute
	}
	fmt.Println("[proto] (ExecuteOnLeader) request successful")
	return nil
}

func (srv *server) IsLeader() (bool, error) {
	fmt.Println("[proto] (IsLeader) request received, processing...")
	if srv.Node.Consensus == nil {
		return false, errors.New("consensus is not initialized")
	}
	fmt.Println("srv.Node.Consensus.State(): ", srv.Node.Consensus.State())
	fmt.Println("jrpc的IsLeader中的srv.Node.Consensus.State(): ", srv.Node.Consensus.State())
	is := srv.Node.Consensus.State() == raft.Leader
	fmt.Println("[proto] (IsLeader) request successful")
	fmt.Println("返回结果为：", is)
	return is, nil
}

// ConsensusJoin 增加一个新节点给Raft consensus network
func (srv *server) ConsensusJoin(nodeID string, nodeConsensusAddr string) error {
	fmt.Println("[proto] (ConsensusJoin) request received, processing...")
	// 检查是否节点已经是网络的一部分
	consensusCfg := srv.Node.Consensus.GetConfiguration().Configuration()
	for _, s := range consensusCfg.Servers {
		if nodeID == string(s.ID) {
			return errors.New("node was already part of the network")
		}
	}
	// 增加新节点给raft network
	future := srv.Node.Consensus.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(nodeConsensusAddr), 0, 0)
	if future.Error() != nil {
		return future.Error()
	}
	fmt.Println("[proto] (ConsensusJoin) request successful")
	return nil
}

// ConsensusRemove 从Raft consensus network中删除一个节点
func (srv *server) ConsensusRemove(nodeID string) error {
	fmt.Println("[proto] (ConsensusRemove) request received, processing...")
	future := srv.Node.Consensus.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if future.Error() != nil {
		return future.Error()
	}
	fmt.Println("[proto] (ConsensusRemove) request successful")
	return nil
}
