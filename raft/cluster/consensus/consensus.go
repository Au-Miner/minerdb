package consensus

import (
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb/v2"
	"github.com/narvikd/errorskit"
	"github.com/narvikd/filekit"
	"jdb/raft/cluster"
	"jdb/raft/cluster/consensus/fsm"
	"jdb/raft/common/filter_writer"
	"jdb/raft/discover"
	"jdb/raft/starter/config"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Node raft节点
type Node struct {
	sync.RWMutex
	Consensus                *raft.Raft
	FSM                      *fsm.DatabaseFSM
	ID                       string `json:"id" validate:"required"`
	ConsensusAddress         string `json:"address"`
	MainDir                  string
	storageDir               string
	snapshotsDir             string
	consensusDBPath          string
	logger                   hclog.Logger
	chans                    *Chans
	reinstallCheckInProgress bool // 是否正在尝试检查是否需要解除对自身的阻塞
	reinstallInProgress      bool // 是否处在尝试解除自身的阻塞
}

// Chans 定义了observers使用的chan
type Chans struct {
	nodeChanges        chan raft.Observation
	leaderChanges      chan raft.Observation
	failedHBChanges    chan raft.Observation
	requestVoteRequest chan raft.Observation
}

func (n *Node) IsReinstallInProgress() bool {
	n.RLock()
	defer n.RUnlock()
	return n.reinstallInProgress
}

func (n *Node) SetReinstallInProgress(b bool) {
	n.Lock()
	defer n.Unlock()
	n.reinstallInProgress = b
}

func (n *Node) IsReinstallCheckInProgress() bool {
	n.RLock()
	defer n.RUnlock()
	return n.reinstallCheckInProgress
}

func (n *Node) SetReinstallCheckInProgress(b bool) {
	n.Lock()
	defer n.Unlock()
	n.reinstallCheckInProgress = b
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
	fmt.Println("errStartConsensus：", errStartConsensus)
	if errStartConsensus != nil {
		return errStartConsensus
	}
	// 判断集群是否准备就绪
	errClusterReadiness := n.waitForClusterReadiness()
	fmt.Println("errClusterReadiness：", errClusterReadiness)
	if errClusterReadiness != nil {
		return errClusterReadiness
	}
	// 注册observers
	n.registerObservers()
	return nil
}

// setConsensusLogger 设置一个筛选器编写器，以抑制本应是调试错误的raft错误
func (n *Node) setConsensusLogger(cfg *raft.Config) {
	const errCannotSnapshotNow = "snapshot now, wait until the configuration entry at"
	filters := []string{raft.ErrNothingNewToSnapshot.Error(), errCannotSnapshotNow}
	fw := filter_writer.New(os.Stderr, filters)
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
	fmt.Println(currentNodeID, "准备开始startConsensus")
	const bootstrappingLeader = "bootstrap-node"
	// 检查是否共识已经bootstrapped
	consensusCfg := n.Consensus.GetConfiguration().Configuration()
	fmt.Println("consensusCfg.Servers的个数有", len(consensusCfg.Servers))
	if len(consensusCfg.Servers) >= 2 {
		n.logger.Info("consensus already bootstrapped")
		n.logger.Info("对于", currentNodeID, "而言，consensus已经启动了")
		// time.Sleep(5 * time.Second)
		return nil
	}
	// 定义bootstrapping servers列表
	bootstrappingServers := newConsensusServerList(bootstrappingLeader)
	// 查找真正的leader，会跳过当前节点
	leaderID, errSearchLeader := discover.SearchLeader(currentNodeID)
	fmt.Println("SearchLeader的结果为：", leaderID, " errSearchLeader: ", errSearchLeader)
	if errSearchLeader == nil {
		bootstrappingServers = newConsensusServerList(leaderID)
	}
	// 只有leader可以启动共识进程
	future := n.Consensus.BootstrapCluster(raft.Configuration{Servers: bootstrappingServers})
	fmt.Println("future.Error().Error(): ", future.Error().Error())
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
	fmt.Println(currentNodeID, "节点准备加入共识网络...")
	// time.Sleep(5 * time.Second)
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
