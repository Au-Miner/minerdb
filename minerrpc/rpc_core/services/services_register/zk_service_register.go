package services_register

import (
	"minerdb/minerrpc/rpc_common/utils"
	"net"
)

type ZkServiceRegister struct{}

func NewZkServiceRegister() *ZkServiceRegister {
	return &ZkServiceRegister{}
}

func (zsr *ZkServiceRegister) Register(serviceName string, addr *net.TCPAddr) error {
	return utils.RegisterService(serviceName, addr)
}
