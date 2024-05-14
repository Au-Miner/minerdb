// Package proto_server provides jdb's gRPC server implementation.
package proto_server

import (
	"google.golang.org/grpc"
	"jdb/raft/api/proto"
	"jdb/raft/cluster/consensus"
	"jdb/raft/starter/app"
	"jdb/raft/starter/config"
	"net"
)

// server gRPC server
type server struct {
	proto.UnimplementedServiceServer
	Config config.Config
	Node   *consensus.Node
}

// Start 启动gRPC server
func Start(a *app.App) error {
	listen, errListen := net.Listen("tcp", a.Config.CurrentNode.GrpcAddress)
	if errListen != nil {
		return errListen
	}
	srvModel := &server{
		Config: a.Config,
		Node:   a.Node,
	}
	protoServer := grpc.NewServer()
	// 注册server model
	proto.RegisterServiceServer(protoServer, srvModel)
	return protoServer.Serve(listen)
}
