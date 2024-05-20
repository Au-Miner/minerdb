package handler

import (
	"jdb/jrpc/rpc_common/entities"
	"log"
	"reflect"
)

type RequestHandlerImpl struct{}

func NewRequestHandlerImpl() *RequestHandlerImpl {
	return &RequestHandlerImpl{}
}

func (rh *RequestHandlerImpl) Execute(req *entities.RPCdata, f reflect.Value) *entities.RPCdata {
	log.Printf("func %s is called\n", req.Name)
	// unpack request arguments
	inArgs := make([]reflect.Value, len(req.Args))
	for i := range req.Args {
		inArgs[i] = reflect.ValueOf(req.Args[i])
	}
	// invoke requested method
	out := f.Call(inArgs)
	resArgs := make([]interface{}, len(out)-1)
	for i := 0; i < len(out)-1; i++ {
		// 将前len(out)-1个返回值转换为interface{}类型
		resArgs[i] = out[i].Interface()
	}
	// pack error argument
	var er string
	if _, ok := out[len(out)-1].Interface().(error); ok {
		// convert the error into error string value
		er = out[len(out)-1].Interface().(error).Error()
	}
	return &entities.RPCdata{Name: req.Name, Args: resArgs, Err: er}
}
