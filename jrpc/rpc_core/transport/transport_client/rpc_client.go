package transport_client

import (
	"minerdb/jrpc/rpc_common/entities"
	"minerdb/jrpc/rpc_core/serializer"
)

const (
	DEFAULT_SERIALIZER = serializer.JSONSerializer
)

type RpcClient interface {
	SendRequest(req entities.RPCdata) (*entities.RPCdata, error)
}
