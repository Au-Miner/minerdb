package main

import (
	"fmt"
	"jdb/jin"
	"jdb/raft/api/jrpc/jrpc_server"
	"jdb/raft/api/rest/middle_ware"
	"jdb/raft/api/rest/route"
	"jdb/raft/discover/zk_discover"
	"jdb/raft/starter/app"
	"jdb/raft/starter/config"
	"log"
	"runtime"
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

	startApiJrpc(cfg)
	fmt.Println("startApiJrpc结束")
	a := app.NewApp(cfg)
	fmt.Println("NewApp结束")
	startApiRest(a)
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

func startApiJrpc(cfg config.Config) {
	log.Println("[proto] Starting proto jrpc_server...")
	err := zk_discover.RegisterNode(cfg.CurrentNode.ID)
	if err != nil {
		log.Fatalln("ips can't be started:", err)
	}
	jrpc_server.Start(cfg)
}
