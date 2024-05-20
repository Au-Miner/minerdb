package services_register

import "net"

type ServiceRegister interface {
	Register(serviceName string, addr *net.TCPAddr) error
}
