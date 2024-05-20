package route

import (
	"minerdb/min"
	"minerdb/raft/cluster/consensus"
	"minerdb/raft/starter/app"
	"minerdb/raft/starter/config"
)

// ApiCtx 是一个简单的结构，包括路由可能需要操作的工具集合。和app.App结构对应
type ApiCtx struct {
	HttpEngine *min.Engine
	HttpGroup  *min.RouterGroup
	Config     config.Config
	Node       *consensus.Node
}

// newRouteCtx 返回ApiCtx的新实例的指针
func newRouteCtx(app *app.App) *ApiCtx {
	routeCtx := ApiCtx{
		Config:     app.Config,
		HttpEngine: app.HttpEngine,
		HttpGroup:  app.HttpGroup,
		Node:       app.Node,
	}
	return &routeCtx
}

// Register 注册路由
func Register(app *app.App) {
	routes(app.HttpGroup, newRouteCtx(app))
}

func routes(httpGroup *min.RouterGroup, route *ApiCtx) {
	httpGroup.GET("/store", route.storeGet)
	httpGroup.GET("/store/keys", route.storeGetKeys)

	httpGroup.POST("/store", route.storeSet)
	// httpGroup.Delete("/store", route.storeDelete)

	httpGroup.GET("/store/backup", route.storeBackup)
	httpGroup.POST("/store/restore", route.restoreBackup)

	httpGroup.GET("/consensus", route.consensusState)
	httpGroup.GET("/healthcheck", route.healthCheck)
}
