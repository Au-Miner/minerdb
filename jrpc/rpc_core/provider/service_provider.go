package provider

import (
	"net"
	"reflect"
)

type ServiceProvider interface {
	AddServiceProvider(iClass interface{}, addr *net.TCPAddr)
	GetFunc(funcName string) (reflect.Value, error)
}
