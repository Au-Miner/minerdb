package discover

import (
	"errors"
	"github.com/hashicorp/raft"
	"jdb/discover/mdnsdiscover"
	"jdb/internal/config"
	"jdb/pkg/ipkit"
)

var mode = config.DiscoverDefault

func SetMode(m string) error {
	if mode != config.DiscoverDefault {
		return errors.New("discover mode not recognized")
	}
	mode = m
	return nil
}

// SearchAliveNodes will skip currentNodeID.
func SearchAliveNodes(consensus *raft.Raft, currentNodeID string) []raft.Server {
	switch mode {
	case config.DiscoverDefault:
		return mdnsdiscover.SearchAliveNodes(consensus, currentNodeID)
	default:
		return []raft.Server{}
	}
}

// SearchLeader 用于查找leader
// 会跳过自身，如果找不到leader，则返回错误，目的是确保不会调用自身的gRPC操作
func SearchLeader(currentNode string) (string, error) {
	switch mode {
	case config.DiscoverDefault:
		return mdnsdiscover.SearchLeader(currentNode)
	default:
		return "", nil
	}
}

func NewConsensusAddr(nodeHost string) string {
	switch mode {
	case config.DiscoverDefault:
		return ipkit.NewAddr(nodeHost, config.ConsensusPort)
	default:
		return ""
	}
}

func NewGrpcAddress(nodeHost string) string {
	switch mode {
	case config.DiscoverDefault:
		return ipkit.NewAddr(nodeHost, config.GrpcPort)
	default:
		return ""
	}
}
