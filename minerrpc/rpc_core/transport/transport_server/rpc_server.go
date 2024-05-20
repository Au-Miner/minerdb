package transport_server

import "minerdb/minerrpc/rpc_core/serializer"

const (
	DEFAULT_SERIALIZER = serializer.JSONSerializer
)

type RpcServer interface {
	Start()
	Register(iClass interface{})
}
