package entities

type RPCdata struct {
	Name string        // 请求方法名
	To   string        // 目标ip
	Args []interface{} // 发送/返回参数
	Err  string
}
