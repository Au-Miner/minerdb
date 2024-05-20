package middle_ware

import (
	"minerdb/min"
)

// InitMiddlewares 使用 Fiber 框架来初始化中间件，用于配置跨域资源共享（CORS）和错误恢复
func InitMiddlewares(httpGroup *min.RouterGroup) {
	// 捕捉应用程序中的所有 panic，并返回一个 500 内部服务器错误响应给客户端。避免将错误信息暴露给客户端
	httpGroup.Use(min.Recovery(), min.Logger())
}
