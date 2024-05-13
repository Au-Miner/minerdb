// Package cluster is responsible for doing operations on the cluster.
package cluster

import (
	"encoding/json"
	"errors"
	"github.com/hashicorp/raft"
	"github.com/narvikd/errorskit"
	"jdb/api/proto"
	"jdb/api/proto/protoclient"
	"jdb/cluster/consensus/fsm"
	"jdb/discover"
	"log"
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
	conn, errConn := protoclient.NewConnection(leaderGrpcAddr)
	if errConn != nil {
		return errConn
	}
	defer conn.Cleanup()
	_, errTalk := conn.Client.ExecuteOnLeader(conn.Ctx, &proto.ExecuteOnLeaderRequest{
		Payload: payloadData,
	})
	if errTalk != nil {
		return errorskit.Wrap(errTalk, errGrpcTalkLeader)
	}
	return nil
}

func ConsensusJoin(nodeID string, nodeConsensusAddr string, leaderGrpcAddr string) error {
	conn, errConn := protoclient.NewConnection(leaderGrpcAddr)
	if errConn != nil {
		return errConn
	}
	defer conn.Cleanup()
	_, errTalk := conn.Client.ConsensusJoin(conn.Ctx, &proto.ConsensusRequest{
		NodeID:            nodeID,
		NodeConsensusAddr: nodeConsensusAddr,
	})
	if errTalk != nil {
		return errorskit.Wrap(errTalk, errGrpcTalkLeader)
	}
	return nil
}

func ConsensusRemove(nodeID string, leaderGrpcAddr string) error {
	conn, errConn := protoclient.NewConnection(leaderGrpcAddr)
	if errConn != nil {
		return errConn
	}
	defer conn.Cleanup()
	_, errTalk := conn.Client.ConsensusRemove(conn.Ctx, &proto.ConsensusRequest{
		NodeID: nodeID,
	})
	if errTalk != nil {
		return errorskit.Wrap(errTalk, errGrpcTalkLeader)
	}
	return nil
}

// RequestNodeReinstall 被调用以防止节点被卡住太久的情况
func RequestNodeReinstall(nodeGrpcAddr string) error {
	conn, errConn := protoclient.NewConnection(nodeGrpcAddr)
	if errConn != nil {
		return errConn
	}
	defer conn.Cleanup()

	_, _ = conn.Client.ReinstallNode(conn.Ctx, &proto.Empty{}) // Ignore the error
	return nil
}
