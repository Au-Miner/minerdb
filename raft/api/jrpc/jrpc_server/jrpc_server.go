package jrpc_server

import (
	"fmt"
	transportServer "jdb/jrpc/rpc_core/transport/server"
	"jdb/raft/cluster/consensus"
	"jdb/raft/starter/config"
)

type server struct {
	Config config.Config
	Node   *consensus.Node
}

// Start 启动gRPC jrpc_server
func Start(cfg config.Config) {
	fmt.Println("a.Config.CurrentNode.GrpcAddress准备开始注册：", cfg.CurrentNode.GrpcAddress)
	srv, err := transportServer.NewDefaultSocketServer(cfg.CurrentNode.GrpcAddress)
	if err != nil {
		panic(err)
	}
	srv.Register(&server{})
	go srv.Start()
}
