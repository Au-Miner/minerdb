package jrpc_server

import (
	"fmt"
	transportServer "jdb/jrpc/rpc_core/transport/server"
	"jdb/raft/cluster/consensus"
	"jdb/raft/starter/app"
	"jdb/raft/starter/config"
)

type server struct {
	Config config.Config
	Node   *consensus.Node
}

// Start 启动gRPC jrpc_server
func Start(a *app.App) {
	fmt.Println("a.Config.CurrentNode.GrpcAddress准备开始注册：", a.Config.CurrentNode.GrpcAddress)
	srv, err := transportServer.NewDefaultSocketServer(a.Config.CurrentNode.GrpcAddress)
	if err != nil {
		panic(err)
	}
	srv.Register(&server{
		Config: a.Config,
		Node:   a.Node,
	})
	go srv.Start()
}
