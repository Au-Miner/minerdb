package ip_kit

import "fmt"

func NewAddr(nodeID string, port int) string {
	return fmt.Sprintf("%s:%v", nodeID, port)
}
