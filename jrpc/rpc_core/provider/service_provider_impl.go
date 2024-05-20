package provider

import (
	"fmt"
	servicesRegister "minerdb/jrpc/rpc_core/services/services_register"
	"net"
	"reflect"
)

type ServiceProviderImpl struct {
	funcs           map[string]reflect.Value
	ServiceRegister servicesRegister.ServiceRegister
}

func NewServiceProvider() *ServiceProviderImpl {
	return &ServiceProviderImpl{
		funcs:           make(map[string]reflect.Value),
		ServiceRegister: servicesRegister.NewZkServiceRegister(),
	}
}

func (sp *ServiceProviderImpl) AddServiceProvider(iClass interface{}, addr *net.TCPAddr) {
	rType := reflect.TypeOf(iClass)
	rClass := reflect.ValueOf(iClass)
	for idx := 0; idx < rClass.NumMethod(); idx++ {
		rFuncType := rType.Method(idx)
		rFuncClass := rClass.Method(idx)
		if rFuncType.Type.Kind() == reflect.Func {
			if _, ok := sp.funcs[rFuncType.Name]; ok {
				continue
			}
			sp.funcs[rFuncType.Name] = rFuncClass
			err := sp.ServiceRegister.Register(rFuncType.Name, addr)
			if err != nil {
				fmt.Printf("ServiceRegister Failed: %v\n", err)
			}
		}
	}
}

func (sp *ServiceProviderImpl) GetFunc(funcName string) (reflect.Value, error) {
	f, ok := sp.funcs[funcName]
	if !ok {
		return reflect.Value{}, fmt.Errorf("func not found")
	} else {
		return f, nil
	}
}
