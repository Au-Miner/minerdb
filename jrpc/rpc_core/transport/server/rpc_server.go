package transport_server

import "jdb/jrpc/rpc_core/serializer"

const (
	DEFAULT_SERIALIZER = serializer.JSONSerializer
)

type RpcServer interface {
	Start()
	Register(iClass interface{})
}
