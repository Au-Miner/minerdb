package jrpc_client

type ClientService struct {
	ExecuteOnLeader func([]byte) error
	IsLeader        func() (bool, error)
	ConsensusJoin   func(string, string) error
	ConsensusRemove func(string) error
	ReinstallNode   func() error
}
