package transport_server

import "minerdb/jrpc/rpc_core/serializer"

const (
	DEFAULT_SERIALIZER = serializer.JSONSerializer
)

type RpcServer interface {
	Start()
	Register(iClass interface{})
}
