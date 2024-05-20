package transport_client

import (
	"errors"
	"fmt"
	"minerdb/jrpc/rpc_common/entities"
	"reflect"
)

type RpcClientProxy struct {
	client  RpcClient
	aimless bool
	to      string
}

func NewRpcClientProxy(client RpcClient) RpcClientProxy {
	return RpcClientProxy{client: client}
}

func (rcp RpcClientProxy) NewProxyInstance(iClass interface{}) interface{} {
	rType := reflect.TypeOf(iClass)
	rClass := reflect.ValueOf(iClass).Elem()
	if rType.Kind() != reflect.Ptr {
		panic("Need a pointer of interface struct")
	}
	if rType.Elem().Kind() != reflect.Struct {
		panic("Need a pointer of interface struct")
	}
	rType = rType.Elem()
	for idx := 0; idx < rClass.NumField(); idx++ {
		rElemType := rType.Field(idx)
		rElemClass := rClass.Field(idx)

		// fmt.Println("注意rElemType.Name: ", rElemType.Name)
		// fmt.Println("注意rElemType.Type: ", rElemType.Type)
		if rElemType.Type.Kind() == reflect.Func {
			if !rElemClass.CanSet() {
				continue
			}
			proxyFunc := func(req []reflect.Value) []reflect.Value {

				errorHandler := func(err error) []reflect.Value {
					outArgs := make([]reflect.Value, rElemClass.Type().NumOut())
					for i := 0; i < len(outArgs)-1; i++ {
						outArgs[i] = reflect.Zero(rElemClass.Type().Out(i))
					}
					outArgs[len(outArgs)-1] = reflect.ValueOf(&err).Elem()
					return outArgs
				}

				// Process input parameters
				inArgs := make([]interface{}, 0, len(req))
				for _, arg := range req {
					inArgs = append(inArgs, arg.Interface())
				}

				// ReqRPC
				reqRPC := entities.RPCdata{Name: rElemType.Name, Args: inArgs}

				// client执行请求reqRPC
				fmt.Println("client准备发送reqRPC")
				rspDecode, err := rcp.client.SendRequest(reqRPC)
				fmt.Printf("client接受到了rspDecode: %v, err: %v\n", rspDecode, err)
				if err != nil {
					return errorHandler(err)
				}

				if rspDecode.Err != "" { // remote transport_server error
					return errorHandler(errors.New(rspDecode.Err))
				}
				if len(rspDecode.Args) == 0 {
					rspDecode.Args = make([]interface{}, rElemClass.Type().NumOut())
				}
				// unpack response arguments
				// 获取返回值的数量
				numOut := rElemClass.Type().NumOut()
				// 遍历rspDecode(rspRPC)的Args存入outArgs中作为返回值
				outArgs := make([]reflect.Value, numOut)
				for i := 0; i < numOut; i++ {
					if i != numOut-1 { // unpack arguments (except error)
						if rspDecode.Args[i] == nil { // if argument is nil (gob will ignore "Zero" in transmission), set "Zero" value
							// container.Type().Out(i)为返回值的类型
							outArgs[i] = reflect.Zero(rElemClass.Type().Out(i))
						} else {
							outArgs[i] = reflect.ValueOf(rspDecode.Args[i])
						}
					} else { // unpack error argument
						outArgs[i] = reflect.Zero(rElemClass.Type().Out(i))
					}
				}
				return outArgs
			}
			rElemClass.Set(reflect.MakeFunc(rElemClass.Type(), proxyFunc))
		}
	}
	return iClass
}
