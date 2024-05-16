package jrpc_server

import (
	"errors"
	"github.com/hashicorp/raft"
	"jdb/raft/cluster"
	"log"
)

// ExecuteOnLeader 在Raft leader上执行一个cmd
func (srv *server) ExecuteOnLeader(payloadData []byte) error {
	log.Println("[proto] (ExecuteOnLeader) request received, processing...")
	// 在leader中apply一个cmd，只有当前node为leader才能执行
	errExecute := cluster.ApplyLeaderFuture(srv.Node.Consensus, payloadData)
	if errExecute != nil {
		return errExecute
	}
	log.Println("[proto] (ExecuteOnLeader) request successful")
	return nil
}

func (srv *server) IsLeader() bool {
	log.Println("[proto] (IsLeader) request received, processing...")
	is := srv.Node.Consensus.State() == raft.Leader
	log.Println("[proto] (IsLeader) request successful")
	return is
}

// ConsensusJoin 增加一个新节点给Raft consensus network
func (srv *server) ConsensusJoin(nodeID string, nodeConsensusAddr string) error {
	log.Println("[proto] (ConsensusJoin) request received, processing...")
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
	log.Println("[proto] (ConsensusJoin) request successful")
	return nil
}

// ConsensusRemove 从Raft consensus network中删除一个节点
func (srv *server) ConsensusRemove(nodeID string) error {
	log.Println("[proto] (ConsensusRemove) request received, processing...")
	future := srv.Node.Consensus.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if future.Error() != nil {
		return future.Error()
	}
	log.Println("[proto] (ConsensusRemove) request successful")
	return nil
}
