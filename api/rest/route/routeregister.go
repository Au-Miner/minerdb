package route

import (
	"github.com/gofiber/fiber/v2"
	"jdb/cluster/consensus"
	"jdb/internal/app"
	"jdb/internal/config"
)

// ApiCtx 是一个简单的结构，包括路由可能需要操作的工具集合。和app.App结构对应
type ApiCtx struct {
	Config     config.Config
	HttpServer *fiber.App
	Node       *consensus.Node
}

// newRouteCtx 返回ApiCtx的新实例的指针
func newRouteCtx(app *app.App) *ApiCtx {
	routeCtx := ApiCtx{
		Config:     app.Config,
		HttpServer: app.HttpServer,
		Node:       app.Node,
	}
	return &routeCtx
}

// Register 注册路由
func Register(app *app.App) {
	routes(app.HttpServer, newRouteCtx(app))
}

func routes(app *fiber.App, route *ApiCtx) {
	app.Get("/store", route.storeGet)
	app.Get("/store/keys", route.storeGetKeys)

	app.Post("/store", route.storeSet)
	app.Delete("/store", route.storeDelete)

	app.Get("/store/backup", route.storeBackup)
	app.Post("/store/restore", route.restoreBackup)

	app.Get("/consensus", route.consensusState)
	app.Get("/healthcheck", route.healthCheck)
}
