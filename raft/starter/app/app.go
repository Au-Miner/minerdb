package app

import (
	"log"
	"minerdb/jin"
	"minerdb/raft/cluster/consensus"
	"minerdb/raft/discover"
	"minerdb/raft/starter/config"
)

// App 是一个简单的结构，包括应用程序可能需要操作的工具集合
type App struct {
	HttpEngine *jin.Engine
	HttpGroup  *jin.RouterGroup
	Node       *consensus.Node
	Config     config.Config
}

func NewApp(cfg config.Config) *App {
	// 设置DiscoverStrategy
	errDiscover := discover.SetMode(cfg.Cluster.DiscoverStrategy)
	if errDiscover != nil {
		log.Fatalln(errDiscover)
	}
	// 根据cfg配置创建一个新的Node（内部封装了db和raft）
	node, errConsensus := consensus.New(cfg)
	if errConsensus != nil {
		log.Fatalln(errConsensus)
	}
	httpEngine := jin.New()
	httpGroup := httpEngine.Group("/api")
	return &App{
		HttpEngine: httpEngine,
		HttpGroup:  httpGroup,
		Node:       node,
		Config:     cfg,
	}
}
