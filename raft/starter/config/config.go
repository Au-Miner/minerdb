package config

import (
	"fmt"
	"github.com/narvikd/errorskit"
	"minerdb/raft/common/ip_kit"
	"minerdb/raft/common/resolver"
	"os"
	"time"
)

const (
	ApiPort         = 3001
	ConsensusPort   = 3002
	GrpcPort        = 3003
	DiscoverDefault = "default"
)

type Config struct {
	CurrentNode NodeCfg
	Cluster     Cluster
}

type NodeCfg struct {
	ID               string
	Host             string
	ApiPort          int
	ApiAddress       string
	ConsensusPort    int
	ConsensusAddress string
	GrpcPort         int
	GrpcAddress      string
}

type Cluster struct {
	DiscoverStrategy string
}

func New() (Config, error) {
	nodeHost, err := newNodeID()
	if err != nil {
		return Config{}, err
	}
	fmt.Println("new config的nodeHost：", nodeHost)
	nodeCfg := NodeCfg{
		ID:               nodeHost,
		Host:             nodeHost,
		ApiPort:          ApiPort,
		ApiAddress:       ip_kit.NewAddr(nodeHost, ApiPort),
		ConsensusPort:    ConsensusPort,
		ConsensusAddress: ip_kit.NewAddr(nodeHost, ConsensusPort),
		GrpcPort:         GrpcPort,
		GrpcAddress:      ip_kit.NewAddr(nodeHost, GrpcPort),
	}
	return Config{CurrentNode: nodeCfg, Cluster: newClusterCfg()}, nil
}

func newNodeID() (string, error) {
	const resolverTimeout = 300 * time.Millisecond
	hostname, errHostname := os.Hostname()
	if errHostname != nil {
		return "", errorskit.Wrap(errHostname, "couldn't get hostname on Config generation")
	}
	if !resolver.IsHostAlive(hostname, resolverTimeout) {
		return "", fmt.Errorf("no host found for: %s", hostname)
	}
	return hostname, nil
}

func newClusterCfg() Cluster {
	clusterCfg := Cluster{
		DiscoverStrategy: DiscoverDefault,
	}
	return clusterCfg
}
