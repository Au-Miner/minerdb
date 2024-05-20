package load_balancer

import "net"

type LoadBalancer interface {
	Select(services []*net.TCPAddr) (*net.TCPAddr, error)
}
