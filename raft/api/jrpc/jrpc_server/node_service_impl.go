package jrpc_server

import (
	"log"
)

// ReinstallNode 删除node
func (srv *server) ReinstallNode() {
	log.Println("[proto] (Reset Node) request received, processing...")
	go srv.Node.ReinstallNode()
	log.Println("[proto] (Reset Node) request successful")
}
