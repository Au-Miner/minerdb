package transport_server

import (
	"fmt"
	"jdb/jrpc/rpc_core/handler"
	"jdb/jrpc/rpc_core/provider"
	"jdb/jrpc/rpc_core/serializer"
	transportUtils "jdb/jrpc/rpc_core/transport/utils"
	"log"
	"net"
)

type SocketServer struct {
	ServicesProvider provider.ServiceProvider
	Serializer       serializer.CommonSerializer
	RequestHandler   handler.RequestHandler
	Addr             *net.TCPAddr
}

func NewDefaultSocketServer(addrStr string) (*SocketServer, error) {
	return NewSocketServer(addrStr, DEFAULT_SERIALIZER)
}

func NewSocketServer(addrStr string, serializerId int) (*SocketServer, error) {
	addr, err := net.ResolveTCPAddr("tcp", addrStr)
	// fmt.Printf("addrStr: %v, addr: %v\n", addrStr, addr)
	if err != nil {
		return nil, err
	}
	return &SocketServer{
		ServicesProvider: provider.NewServiceProvider(),
		Serializer:       serializer.GetByCode(serializerId),
		RequestHandler:   handler.NewRequestHandlerImpl(),
		Addr:             addr,
	}, nil
}

func (ss *SocketServer) Register(iClass interface{}) {
	ss.ServicesProvider.AddServiceProvider(iClass, ss.Addr)
}

func (ss *SocketServer) Start() {
	l, err := net.Listen("tcp", ss.Addr.String())
	if err != nil {
		log.Printf("listen on %s err: %v\n", ss.Addr, err)
		return
	}
	for {
		fmt.Println("等待监听conn")
		conn, err := l.Accept()
		if err != nil {
			log.Printf("accept err: %v\n", err)
			continue
		}
		// 每接收到一个rpc请求，就开启一个goroutine处理
		go func() {
			nowObjReader := transportUtils.NewObjectReader(conn)
			nowObjWriter := transportUtils.NewObjectWriter(conn)
			transportUtils.NewObjectWriter(conn)
			for {
				fmt.Println("server准备接受decReq")
				decReq, err := nowObjReader.ReadObject()
				if err != nil {
					log.Printf("read err: %v\n", err)
					return
				}
				fmt.Println("===接收到了decReq: ", decReq)
				fmt.Println("===接收到了err: ", err)
				f, err := ss.ServicesProvider.GetFunc(decReq.Name)
				if err != nil {
					log.Printf("service provider err: %v\n", err)
					return
				}
				fmt.Println("===准备执行Execute")
				resp := ss.RequestHandler.Execute(decReq, f)
				fmt.Println("===执行完毕准备发送: ", resp)
				err = nowObjWriter.WriteObject(resp, ss.Serializer)
				if err != nil {
					log.Printf("write err: %v\n", err)
					return
				}
			}
		}()
	}
}
