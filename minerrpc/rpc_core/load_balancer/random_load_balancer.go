package load_balancer

import (
	"errors"
	"math/rand"
	"net"
	"time"
)

type RandomLoadBalancer struct{}

func (r *RandomLoadBalancer) Select(services []*net.TCPAddr) (*net.TCPAddr, error) {
	if len(services) == 0 {
		return nil, errors.New("no services available to select")
	}
	rand.NewSource(time.Now().UnixNano())
	index := rand.Intn(len(services))
	return services[index], nil
}
