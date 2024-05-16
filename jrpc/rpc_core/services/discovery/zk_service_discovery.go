package services_discovery

import (
	"jdb/jrpc/rpc_common/utils"
	"jdb/jrpc/rpc_core/load_balancer"
	"net"
)

type ZkServiceDiscovery struct {
	LoadBalancer load_balancer.LoadBalancer
}

func NewZkServiceDiscovery(lb load_balancer.LoadBalancer) *ZkServiceDiscovery {
	if lb == nil {
		return &ZkServiceDiscovery{LoadBalancer: &load_balancer.RandomLoadBalancer{}}
	} else {
		return &ZkServiceDiscovery{LoadBalancer: lb}
	}
}

func (zsd *ZkServiceDiscovery) LookupService(serviceName string) (*net.TCPAddr, error) {
	instances, err := utils.GetAllInstances(serviceName)
	if err != nil {
		return nil, err
	}
	return zsd.LoadBalancer.Select(instances)
}
