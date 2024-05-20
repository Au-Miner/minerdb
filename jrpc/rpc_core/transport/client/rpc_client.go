package transport_client

import (
	"jdb/jrpc/rpc_common/entities"
	"jdb/jrpc/rpc_core/serializer"
)

const (
	DEFAULT_SERIALIZER = serializer.JSONSerializer
)

type RpcClient interface {
	SendRequest(req entities.RPCdata) (*entities.RPCdata, error)
}
