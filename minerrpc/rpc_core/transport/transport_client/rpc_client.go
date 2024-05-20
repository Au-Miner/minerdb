package transport_client

import (
	"minerdb/minerrpc/rpc_common/entities"
	"minerdb/minerrpc/rpc_core/serializer"
)

const (
	DEFAULT_SERIALIZER = serializer.JSONSerializer
)

type RpcClient interface {
	SendRequest(req entities.RPCdata) (*entities.RPCdata, error)
}
