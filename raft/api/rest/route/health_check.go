package route

import (
	"fmt"
	"minerdb/jin"
	"minerdb/raft/api/rest/json_response"
	"strconv"
)

func (a *ApiCtx) healthCheck(ctx *jin.Context) {
	if !a.Node.IsHealthy() {
		json_response.ServerError(ctx, "")
	}
	json_response.OK(ctx, "", nil)
}

func (a *ApiCtx) consensusState(ctx *jin.Context) {
	stats := a.Node.Consensus.Stats()
	address, id := a.Node.Consensus.LeaderWithID()
	stats["leader"] = fmt.Sprintf("Address: %s Leader ID: %s", address, id)
	stats["node_id"] = a.Config.CurrentNode.ID
	stats["node_host"] = a.Config.CurrentNode.Host
	stats["is_quorum_possible"] = strconv.FormatBool(a.Node.IsQuorumPossible(false))
	json_response.OK(ctx, "consensus state retrieved successfully", stats)
}
