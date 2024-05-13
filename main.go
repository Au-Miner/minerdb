package main

import (
	"github.com/gofiber/fiber/v2"
	"jdb/api/proto/protoserver"
	"jdb/api/rest/middleware"
	"jdb/api/rest/route"
	"jdb/discover/mdnsdiscover"
	"jdb/internal/app"
	"jdb/internal/config"
	"log"
	"runtime"
	"sync"
)

func init() {
	if runtime.GOOS == "windows" {
		log.Fatalln("jdb is only compatible with Mac and Linux")
	}
}

func main() {
	cfg, errCfg := config.New()
	if errCfg != nil {
		log.Fatalln(errCfg)
	}
	start(app.NewApp(cfg))
}

func start(a *app.App) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		startApiRest(a)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		startApiProto(a)
	}()
	if a.Config.Cluster.DiscoverStrategy == config.DiscoverDefault {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mdnsdiscover.ServeAndBlock(a.Config.CurrentNode.Host, 8001)
		}()
	}
	wg.Wait()
}

// 启动restful api监听（目前使用fiber框架）
func startApiRest(a *app.App) {
	errListen := newApiRest(a).Listen(a.Config.CurrentNode.ApiAddress)
	if errListen != nil {
		log.Fatalln("api can't be started:", errListen)
	}
}

func startApiProto(a *app.App) {
	log.Println("[proto] Starting proto server...")
	err := protoserver.Start(a)
	if err != nil {
		log.Fatalln("proto api can't be started:", err)
	}
}

func newApiRest(a *app.App) *fiber.App {
	// 注册fiber的中间件和路由
	middleware.InitMiddlewares(a.HttpServer)
	route.Register(a)
	return a.HttpServer
}
