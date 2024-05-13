package protoclient

import (
	"context"
	"github.com/narvikd/errorskit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"jdb/api/proto"
	"time"
)

// ConnCleaner 是用于清理连接的接口
type ConnCleaner interface {
	Cleanup()
}

// Connection 表示到gRPC服务器的连接
type Connection struct {
	Conn          *grpc.ClientConn
	Client        proto.ServiceClient
	Ctx           context.Context
	cancelCtxCall context.CancelFunc
}

// Cleanup 关闭连接并取消上下文
func (c *Connection) Cleanup() {
	_ = c.Conn.Close()
	c.cancelCtxCall()
}

// NewConnection 创建到gRPC服务器的新连接
func NewConnection(addr string) (*Connection, error) {
	const (
		dialTimeout       = 1 * time.Second
		timeoutGrpcCall   = 3 * time.Second
		errGrpcConnection = "grpc connection failed"
	)
	// 创建一个具有超时的上下文，用于拨号服务器
	ctxDial, cancelCtxDial := context.WithTimeout(context.Background(), dialTimeout)
	defer cancelCtxDial()
	// 使用上下文拨号服务器
	connDial, errDial := grpc.DialContext(ctxDial, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if errDial != nil {
		return nil, errorskit.Wrap(errDial, errGrpcConnection)
	}
	// 为执行gRPC调用创建一个具有超时的上下文
	ctxExecuteCall, cancelCtxExecuteCall := context.WithTimeout(context.Background(), timeoutGrpcCall)
	return &Connection{
		Conn:          connDial,                         // set the gRPC connection
		Client:        proto.NewServiceClient(connDial), // create a new service client
		Ctx:           ctxExecuteCall,                   // set the context for executing gRPC calls
		cancelCtxCall: cancelCtxExecuteCall,             // set the cancel function for the context included before
	}, nil
}
