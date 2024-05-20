package handler

import (
	"jdb/jrpc/rpc_common/entities"
	"reflect"
)

type RequestHandler interface {
	Execute(req *entities.RPCdata, f reflect.Value) *entities.RPCdata
}
