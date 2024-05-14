package app

import (
	"github.com/gofiber/fiber/v2"
	"github.com/narvikd/fiberparser"
	"jdb/raft/cluster/consensus"
	"jdb/raft/discover"
	"jdb/raft/starter/config"
	"log"
	"time"
)

// App 是一个简单的结构，包括应用程序可能需要操作的工具集合
type App struct {
	HttpServer *fiber.App
	Node       *consensus.Node
	Config     config.Config
}

func NewApp(cfg config.Config) *App {
	// 设置DiscoverStrategy
	errDiscover := discover.SetMode(cfg.Cluster.DiscoverStrategy)
	if errDiscover != nil {
		log.Fatalln(errDiscover)
	}
	// 根据cfg配置创建一个新的Node（内部封装了db和raft）
	node, errConsensus := consensus.New(cfg)
	if errConsensus != nil {
		log.Fatalln(errConsensus)
	}
	serv := fiber.New(fiber.Config{
		AppName:           "jdb",
		EnablePrintRoutes: false,
		IdleTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		ErrorHandler: func(ctx *fiber.Ctx, err error) error {
			return fiberparser.RegisterErrorHandler(ctx, err)
		},
		BodyLimit: 200 * 1024 * 1024, // In MB
	})
	return &App{
		HttpServer: serv,
		Node:       node,
		Config:     cfg,
	}
}
