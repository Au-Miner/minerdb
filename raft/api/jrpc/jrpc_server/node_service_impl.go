package jrpc_server

import "fmt"

// ReinstallNode 删除node
func (srv *server) ReinstallNode() {
	fmt.Println("[proto] (Reset Node) request received, processing...")
	go srv.Node.ReinstallNode()
	fmt.Println("[proto] (Reset Node) request successful")
}
