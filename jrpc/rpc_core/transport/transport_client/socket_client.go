package transport_client

import (
	"fmt"
	"minerdb/jrpc/rpc_common/entities"
	"minerdb/jrpc/rpc_core/load_balancer"
	"minerdb/jrpc/rpc_core/serializer"
	"minerdb/jrpc/rpc_core/services/services_discovery"
	"minerdb/jrpc/rpc_core/transport/transport_utils"
	"net"
)

type SocketClient struct {
	ServiceDiscovery services_discovery.ServiceDiscovery
	Serializer       serializer.CommonSerializer
	AimIp            string
}

func NewDefaultSocketClient() *SocketClient {
	return NewSocketClient(DEFAULT_SERIALIZER, &load_balancer.RandomLoadBalancer{}, "")
}

func NewDefaultSocketClientWithAimIp(aimIp string) *SocketClient {
	return NewSocketClient(DEFAULT_SERIALIZER, &load_balancer.RandomLoadBalancer{}, aimIp)
}

func NewSocketClient(serializerId int, loadBalancer load_balancer.LoadBalancer, aimIp string) *SocketClient {
	return &SocketClient{
		Serializer:       serializer.GetByCode(serializerId),
		ServiceDiscovery: services_discovery.NewZkServiceDiscovery(loadBalancer),
		AimIp:            aimIp,
	}
}

func (client *SocketClient) SendRequest(req entities.RPCdata) (*entities.RPCdata, error) {
	var addr *net.TCPAddr
	var err error
	if client.AimIp == "" {
		fmt.Println("走的是ServiceDiscovery")
		addr, err = client.ServiceDiscovery.LookupService(req.Name)
		fmt.Println("结果为addr", addr)
	} else {
		fmt.Println("走的是ResolveTCPAddr")
		fmt.Println("请求的client.AimIp为", client.AimIp)
		addr, err = net.ResolveTCPAddr("tcp", client.AimIp)
		fmt.Println("结果为addr", addr)
	}
	fmt.Println(111)
	if err != nil {
		fmt.Println("err为：", err)
		return nil, fmt.Errorf("failed to lookup service: %w", err)
	}
	fmt.Println(222)
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		fmt.Println("err为：", err)
		return nil, err
	}
	fmt.Println(333)
	err = transport_utils.NewObjectWriter(conn).WriteObject(&req, client.Serializer)
	fmt.Println(444)
	if err != nil {
		fmt.Println("err为：", err)
		return nil, err
	}
	fmt.Println(555)
	resp, err := transport_utils.NewObjectReader(conn).ReadObject()
	fmt.Println(666)
	if err != nil {
		fmt.Println("err为：", err)
		return nil, err
	}
	fmt.Println(777)
	return resp, nil
}
