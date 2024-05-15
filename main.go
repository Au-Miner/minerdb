package main

import (
	"fmt"
	"jdb/jin"
	"jdb/raft/api/proto/proto_server"
	"jdb/raft/api/rest/middle_ware"
	"jdb/raft/api/rest/route"
	"jdb/raft/discover/mdns_discover"
	"jdb/raft/starter/app"
	"jdb/raft/starter/config"
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
	cfg, err := config.New()
	if err != nil {
		log.Fatalln(err)
	}
	start(app.NewApp(cfg))
}

func start(a *app.App) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 创建fiber的http监听服务（负责解析http请求）
		startApiRest(a)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 创建grpc的监听服务（负责node之间的通信）
		startApiProto(a)
	}()
	if a.Config.Cluster.DiscoverStrategy == config.DiscoverDefault {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 创建mdns服务
			mdns_discover.ServeAndBlock(a.Config.CurrentNode.Host, 8001)
		}()
	}
	wg.Wait()
}

func startApiRest(a *app.App) {
	fmt.Println("！！！！！a.Config.CurrentNode.ApiAddress: ", a.Config.CurrentNode.ApiAddress)
	errListen := newApiRest(a).Run(a.Config.CurrentNode.ApiAddress)
	if errListen != nil {
		log.Fatalln("api can't be started:", errListen)
	}
}

func newApiRest(a *app.App) *jin.Engine {
	// 注册fiber的中间件和路由
	middle_ware.InitMiddlewares(a.HttpGroup)
	route.Register(a)
	return a.HttpEngine
}

func startApiProto(a *app.App) {
	log.Println("[proto] Starting proto server...")
	err := proto_server.Start(a)
	if err != nil {
		log.Fatalln("proto api can't be started:", err)
	}
}
