package consensus

import (
	"errors"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb/v2"
	"github.com/narvikd/errorskit"
	"github.com/narvikd/filekit"
	"jdb/cluster"
	"jdb/cluster/consensus/fsm"
	"jdb/discover"
	"jdb/internal/config"
	"jdb/pkg/filterwriter"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Node struct defines the properties of a node
type Node struct {
	sync.RWMutex
	Consensus                 *raft.Raft
	FSM                       *fsm.DatabaseFSM
	ID                        string `json:"id" validate:"required"`
	ConsensusAddress          string `json:"address"`
	MainDir                   string
	storageDir                string
	snapshotsDir              string
	consensusDBPath           string
	logger                    hclog.Logger
	chans                     *Chans
	unBlockingCheckInProgress bool
	unBlockingInProgress      bool
}

// Chans struct defines the channels used for the observers
type Chans struct {
	nodeChanges        chan raft.Observation
	leaderChanges      chan raft.Observation
	failedHBChanges    chan raft.Observation
	requestVoteRequest chan raft.Observation
}

// IsUnBlockingInProgress returns a boolean value indicating whether the node is already trying to unblock itself
func (n *Node) IsUnBlockingInProgress() bool {
	n.RLock()
	defer n.RUnlock()
	return n.unBlockingInProgress
}

// SetUnBlockingInProgress sets the unBlockingInProgress property of the Node struct
// which indicates whether the node is trying to unblock itself
func (n *Node) SetUnBlockingInProgress(b bool) {
	n.Lock()
	defer n.Unlock()
	n.unBlockingInProgress = b
}

// IsUnBlockingCheckInProgress returns a boolean value indicating whether the node is already
// trying to check if it needs to unblock itself
func (n *Node) IsUnBlockingCheckInProgress() bool {
	n.RLock()
	defer n.RUnlock()
	return n.unBlockingCheckInProgress
}

// SetUnBlockingCheckInProgress 表明设置节点是否正在尝试检查是否需要解除对自身的阻塞
func (n *Node) SetUnBlockingCheckInProgress(b bool) {
	n.Lock()
	defer n.Unlock()
	n.unBlockingCheckInProgress = b
}

// New 新建并初始化一个node（主要包含db和raft）
func New(cfg config.Config) (*Node, error) {
	n, errNode := newNode(cfg)
	if errNode != nil {
		return nil, errNode
	}
	errRaft := n.setRaft()
	if errRaft != nil {
		return nil, errRaft
	}
	return n, nil
}

// newNode initializes and returns a new Node with the given id and address
func newNode(cfg config.Config) (*Node, error) {
	dir := path.Join("data", cfg.CurrentNode.ID)
	storageDir := path.Join(dir, "localdb")
	// 创建DatabaseFSM实例
	f, errDB := fsm.New(storageDir)
	if errDB != nil {
		return nil, errDB
	}
	n := &Node{
		FSM:              f,
		ID:               cfg.CurrentNode.ID,
		ConsensusAddress: cfg.CurrentNode.ConsensusAddress,
		MainDir:          dir,
		storageDir:       storageDir,
		snapshotsDir:     dir, // This isn't a typo, it will create a snapshots dir inside the dir automatically
		consensusDBPath:  filepath.Join(dir, "consensus.db"),
		chans:            new(Chans),
	}
	errDir := filekit.CreateDirs(n.MainDir, false)
	if errDir != nil {
		return nil, errDir
	}
	return n, nil
}

// setRaft 使用节点的配置初始化并启动一个新的共识实例
func (n *Node) setRaft() error {
	const (
		timeout            = 10 * time.Second
		maxConnectionsPool = 10
		retainedSnapshots  = 3
		// TODO: Find appropriate value
		snapshotThreshold = 2
	)
	tcpAddr, errAddr := net.ResolveTCPAddr("tcp", n.ConsensusAddress)
	if errAddr != nil {
		return errorskit.Wrap(errAddr, "couldn't resolve addr")
	}
	// 创建transport
	transport, errTransport := raft.NewTCPTransport(n.ConsensusAddress, tcpAddr, maxConnectionsPool, timeout, os.Stderr)
	if errTransport != nil {
		return errorskit.Wrap(errTransport, "couldn't create transport")
	}
	// 创建log DB
	dbStore, errRaftStore := raftboltdb.NewBoltStore(n.consensusDBPath)
	if errRaftStore != nil {
		return errorskit.Wrap(errRaftStore, "couldn't create consensus db")
	}
	// 创建snapshot store
	snaps, errSnapStore := raft.NewFileSnapshotStore(n.snapshotsDir, retainedSnapshots, os.Stderr)
	if errSnapStore != nil {
		return errorskit.Wrap(errSnapStore, "couldn't create consensus snapshot storage")
	}
	// 设置共识的其他配置
	nodeID := raft.ServerID(n.ID)
	cfg := raft.DefaultConfig()
	cfg.LocalID = nodeID
	cfg.SnapshotInterval = timeout
	cfg.SnapshotThreshold = snapshotThreshold
	n.setConsensusLogger(cfg)
	// 创建一个新的Raft instance
	r, errRaft := raft.NewRaft(cfg, n.FSM, dbStore, dbStore, snaps, transport)
	if errRaft != nil {
		return errorskit.Wrap(errRaft, "couldn't create new consensus")
	}
	n.Consensus = r
	// 启动consensus process
	errStartConsensus := n.startConsensus(string(nodeID))
	if errStartConsensus != nil {
		return errStartConsensus
	}
	// 判断集群是否准备就绪
	errClusterReadiness := n.waitForClusterReadiness()
	if errClusterReadiness != nil {
		return errClusterReadiness
	}
	// 注册observers
	n.registerObservers()
	return nil
}

func (n *Node) setConsensusLogger(cfg *raft.Config) {
	// Setups a filter-writer which suppresses raft's errors that should have been debug errors instead
	const errCannotSnapshotNow = "snapshot now, wait until the configuration entry at"
	filters := []string{raft.ErrNothingNewToSnapshot.Error(), errCannotSnapshotNow}
	fw := filterwriter.New(os.Stderr, filters)

	l := hclog.New(&hclog.LoggerOptions{
		Name:   "consensus",
		Level:  hclog.LevelFromString("DEBUG"),
		Output: fw,
	})
	n.logger = l
	cfg.LogOutput = fw
	cfg.Logger = l
}

// startConsensus 通过将节点添加到现有或新的集群中，启动节点的共识进程
func (n *Node) startConsensus(currentNodeID string) error {
	const bootstrappingLeader = "bootstrap-node"
	// 检查是否共识已经bootstrapped
	consensusCfg := n.Consensus.GetConfiguration().Configuration()

	if len(consensusCfg.Servers) >= 2 {
		n.logger.Info("consensus already bootstrapped")
		return nil
	}
	// 定义bootstrapping servers列表
	bootstrappingServers := newConsensusServerList(bootstrappingLeader)
	// 查找真正的leader，会跳过当前节点
	leaderID, errSearchLeader := discover.SearchLeader(currentNodeID)
	if errSearchLeader == nil {
		bootstrappingServers = newConsensusServerList(leaderID)
	}
	// 启动共识进程
	future := n.Consensus.BootstrapCluster(raft.Configuration{Servers: bootstrappingServers})
	if future.Error() != nil {
		if !strings.Contains(future.Error().Error(), "not a voter") {
			return nil
		}
	}
	// 如果没有错误发生，说明当前节点成功作为领导者启动了共识进程（因为它没有接收到任何错误，包括 "not a voter"）
	if future.Error() == nil {
		return nil
	}
	// 如果存在 "not a voter" 错误，尝试让当前节点加入现有的共识。如果加入过程中报告节点已经是网络的一部分，这种情况可以忽略
	errJoin := joinNodeToExistingConsensus(currentNodeID)
	if errJoin != nil {
		errLower := strings.ToLower(errJoin.Error())
		if strings.Contains(errLower, "was already part of the network") {
			return nil
		}
		return errorskit.Wrap(errJoin, "while bootstrapping")
	}
	return nil
}

func joinNodeToExistingConsensus(nodeID string) error {
	leaderID, errSearchLeader := discover.SearchLeader(nodeID)
	if errSearchLeader != nil {
		return errSearchLeader
	}
	return cluster.ConsensusJoin(nodeID, discover.NewConsensusAddr(nodeID), discover.NewGrpcAddress(leaderID))
}

func newConsensusServerList(nodeID string) []raft.Server {
	return []raft.Server{
		{
			ID:      raft.ServerID(nodeID),
			Address: raft.ServerAddress(discover.NewConsensusAddr(nodeID)),
		},
	}
}

// waitForClusterReadiness 等待集群是否准备就绪
func (n *Node) waitForClusterReadiness() error {
	const (
		maxRetryCount = 7
		sleepTime     = 1 * time.Minute
	)
	currentTry := 0
	for {
		currentTry++
		if currentTry > maxRetryCount {
			return errors.New("quorum retry max reached")
		}
		if n.IsQuorumPossible(true) {
			n.logger.Info("quorum possible.")
			break
		}
		n.logger.Error("it is not possible to reach Quorum due to lack of nodes. Retrying...")
		time.Sleep(sleepTime)
	}
	return nil
}
