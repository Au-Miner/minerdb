package jrpc_server

import (
	"fmt"
	transportServer "minerdb/minerrpc/rpc_core/transport/transport_server"
	"minerdb/raft/cluster/consensus"
	"minerdb/raft/starter/app"
	"minerdb/raft/starter/config"
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
