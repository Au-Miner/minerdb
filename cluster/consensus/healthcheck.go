package consensus

import (
	"github.com/hashicorp/raft"
	"jdb/discover"
	"math"
	"strconv"
)

// IsHealthy checks if the node and the cluster are in a healthy state by validating various factors.
func (n *Node) IsHealthy() bool {
	const prefixErr = "[NODE UNHEALTHY] - "

	consensusServers := n.Consensus.GetConfiguration().Configuration().Servers
	stats := n.Consensus.Stats()

	if len(consensusServers) <= 1 {
		n.logger.Warn(prefixErr + "only one server in configuration")
		return false
	}

	if n.Consensus.State() != raft.Leader && stats["last_contact"] == "never" {
		n.logger.Warn(prefixErr + "only one server in configuration")
		return false
	}

	leaderAddr, leaderID := n.Consensus.LeaderWithID()
	if leaderAddr == "" {
		n.logger.Warn(prefixErr + "local consensus reports that leader address is empty")
		return false
	}

	if leaderID == "" {
		n.logger.Warn(prefixErr + "local consensus reports that leader id is empty")
		return false
	}

	if !n.IsQuorumPossible(false) {
		n.logger.Warn(prefixErr + "quorum is not possible due to lack of available nodes")
		return false
	}

	return true
}

// IsQuorumPossible 是否可以达成共识
// preAlert模式下检查是否可以达到75%的节点数，即(onlineNodes >= (consensusNodes/2)+1)
// default模式下检查是否可以达到50%的节点数
func (n *Node) IsQuorumPossible(preAlert bool) bool {
	const (
		half       = 0.5
		percentile = 0.75
	)
	consensusNodes, _ := strconv.ParseFloat(n.Consensus.Stats()["num_peers"], 64)
	necessaryForQuorum := math.Round(consensusNodes * half)
	if preAlert {
		necessaryForQuorum = math.Round(consensusNodes * percentile)
	}
	onlineNodes := len(discover.SearchAliveNodes(n.Consensus, n.ID))
	return float64(onlineNodes) >= necessaryForQuorum
}
