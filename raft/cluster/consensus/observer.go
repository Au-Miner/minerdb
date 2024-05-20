package consensus

import (
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/narvikd/errorskit"
	"github.com/narvikd/filekit"
	"log"
	"minerdb/raft/cluster"
	"minerdb/raft/discover"
	"time"
)

// registerObservers 注册所有observer channels
func (n *Node) registerObservers() {
	n.registerNodeChangesChan()
	n.registerLeaderChangesChan()
	n.registerFailedHBChangesChan()
	n.registerRequestVoteRequestChan()
}

// registerNewObserver 创建并注册一个新的observer用于过滤T
func registerNewObserver[T any](consensus *raft.Raft, blocking bool, channel chan raft.Observation) {
	observer := raft.NewObserver(channel, blocking, func(o *raft.Observation) bool {
		_, ok := o.Data.(T)
		return ok
	})
	consensus.RegisterObserver(observer)
}

// registerNodeChangesChan 注册node自身状态变更chan
func (n *Node) registerNodeChangesChan() {
	n.chans.nodeChanges = make(chan raft.Observation, 4)
	// 创建并注册一个observer，用于过滤raft state observations并将其发送到channel
	registerNewObserver[raft.RaftState](n.Consensus, false, n.chans.nodeChanges)
	go func() {
		// 阻塞直到channel有数据
		for o := range n.chans.nodeChanges {
			n.logger.Info("Node Changed to role: " + o.Data.(raft.RaftState).String())
			fmt.Println("Node 的状态改变为: " + o.Data.(raft.RaftState).String())
			go n.checkIfNodeNeedsReinstall()
		}
	}()
}

// registerLeaderChangesChan 注册leader信息变更chan
func (n *Node) registerLeaderChangesChan() {
	n.chans.leaderChanges = make(chan raft.Observation, 4)
	registerNewObserver[raft.LeaderObservation](n.Consensus, false, n.chans.leaderChanges)
	go func() {
		for o := range n.chans.leaderChanges {
			obs := o.Data.(raft.LeaderObservation)
			leaderID := string(obs.LeaderID)
			if leaderID != "" {
				n.logger.Info("New Leader: " + leaderID)
			} else {
				n.logger.Info("No Leader available in the Cluster")
				fmt.Println("注意No Leader available in the Cluster！！")
				go n.checkIfNodeNeedsReinstall()
			}
		}
	}()
}

// registerLeaderChangesChan 注册失败心跳chan
func (n *Node) registerFailedHBChangesChan() {
	n.chans.failedHBChanges = make(chan raft.Observation, 4)
	registerNewObserver[raft.FailedHeartbeatObservation](n.Consensus, false, n.chans.failedHBChanges)
	go func() {
		for o := range n.chans.failedHBChanges {
			obs := o.Data.(raft.FailedHeartbeatObservation)
			warnMsg := fmt.Sprintf("REMOVING NODE '%v' from the Leader due to being offline...", obs.PeerID)
			n.logger.Warn(warnMsg)
			n.Consensus.RemoveServer(obs.PeerID, 0, 0)
			n.logger.Warn("NODE SUCCESSFULLY REMOVED FROM STATE CONSENSUS")
		}
	}()
}

// registerLeaderChangesChan 注册requestVote请求chan
func (n *Node) registerRequestVoteRequestChan() {
	n.chans.requestVoteRequest = make(chan raft.Observation, 4)
	registerNewObserver[raft.RequestVoteRequest](n.Consensus, false, n.chans.requestVoteRequest)
	go func() {
		for o := range n.chans.requestVoteRequest {
			data := o.Data.(raft.RequestVoteRequest)
			idRequester := string(data.ID)
			// 判断节点是否在集群中
			if !n.isNodeInConsensusServers(idRequester) {
				msgWarn := fmt.Sprintf(
					"Foreign node (%s) attempted to request a vote, but node is not in the configuration. Requesting foreign node's reinstall",
					idRequester,
				)
				n.logger.Warn(msgWarn)
				// 不在的话，请求重新安装该node
				err := cluster.RequestNodeReinstall(discover.NewGrpcAddress(idRequester))
				if err != nil {
					msgErr := errorskit.Wrap(err, "couldn't reinstall foreign node")
					n.logger.Error(msgErr.Error())
				}
			}
		}
	}()
}

func (n *Node) checkIfNodeNeedsReinstall() {
	const timeout = 1 * time.Minute
	if n.IsReinstallCheckInProgress() {
		n.logger.Warn("UNBLOCKING CHECK ALREADY IN PROGRESS")
		return
	}
	n.SetReinstallCheckInProgress(true)
	defer n.SetReinstallCheckInProgress(false)
	_, leaderID := n.Consensus.LeaderWithID()
	if leaderID != "" {
		return
	}
	time.Sleep(timeout)
	_, leaderID = n.Consensus.LeaderWithID()
	if leaderID != "" {
		return
	}
	n.ReinstallNode()
}

// ReinstallNode 如果卡的时间过长，优雅删除节点node并restart
func (n *Node) ReinstallNode() {
	fmt.Println("当前节点需要被reinstall了！！！！！")
	const errPanic = "COULDN'T GRACEFULLY REINSTALL NODE. "
	if n.IsReinstallInProgress() {
		n.logger.Warn("UNBLOCKING ALREADY IN PROGRESS")
		return
	}
	n.SetReinstallInProgress(true)
	defer n.SetReinstallInProgress(false)
	fmt.Println("node got stuck for too long... Node reinstall in progress...")
	future := n.Consensus.Shutdown()
	if future.Error() != nil {
		errorskit.FatalWrap(future.Error(), errPanic+"couldn't shut down")
	}
	leader, errSearchLeader := discover.SearchLeader(n.ID)
	if errSearchLeader != nil {
		errorskit.FatalWrap(errSearchLeader, errPanic+"couldn't search for leader")
	}
	leaderGrpcAddress := discover.NewGrpcAddress(leader)
	// cluster删除n.ID
	errConsensusRemove := cluster.ConsensusRemove(n.ID, leaderGrpcAddress)
	if errConsensusRemove != nil {
		errorskit.FatalWrap(errConsensusRemove, errPanic+"couldn't remove from consensus")
	}
	errDeleteDirs := filekit.DeleteDirs(n.MainDir)
	if errDeleteDirs != nil {
		errorskit.FatalWrap(errDeleteDirs, errPanic+"couldn't delete dirs")
	}
	// 关闭当前进程，触发docker的auto restart
	log.Fatalln("Node successfully reset. Restarting...")
}

func (n *Node) isNodeInConsensusServers(id string) bool {
	srvs := n.Consensus.GetConfiguration().Configuration().Servers
	if len(srvs) <= 0 {
		return false
	}
	for _, server := range srvs {
		if server.ID == raft.ServerID(id) {
			return true
		}
	}
	return false
}
