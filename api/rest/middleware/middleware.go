package middleware

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/recover"
)

// InitMiddlewares 使用 Fiber 框架来初始化中间件，用于配置跨域资源共享（CORS）和错误恢复
func InitMiddlewares(app *fiber.App) {
	initCorsMW(app)
	initRecoverMW(app)
}

// initCorsMW 初始化 CORS 中间件
func initCorsMW(app *fiber.App) {
	app.Use(
		cors.New(cors.Config{
			// 允许客户端在跨域请求中发送凭证（如 cookies、授权头等）
			AllowCredentials: true,
		}),
	)
}

// initRecoverMW 初始化恢复中间件
// 捕捉应用程序中的所有 panic，并返回一个 500 内部服务器错误响应给客户端。避免将错误信息暴露给客户端
func initRecoverMW(app *fiber.App) {
	app.Use(recover.New())
}
