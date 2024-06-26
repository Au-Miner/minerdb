package resolver

import (
	"bytes"
	"net"
	"os/exec"
	"strings"
	"time"
)

// IsHostAlive 通过发ping来检查是否主机是活着的
func IsHostAlive(host string, timeout time.Duration) bool {
	var out bytes.Buffer
	if !isHostResolvable(host, timeout) {
		return false
	}
	cmd := exec.Command("timeout", "0.3", "ping", "-c", "1", host)
	cmd.Stdout = &out
	isAlive := cmd.Run() == nil || strings.Contains(out.String(), "bytes from")
	return isAlive
}

// isHostResolvable 检查是否主机可解析为ip地址
func isHostResolvable(host string, timeout time.Duration) bool {
	t := time.After(timeout)
	result := make(chan error)
	go func() {
		_, err := net.LookupHost(host)
		result <- err
	}()
	select {
	case <-t:
		return false
	case err := <-result:
		return err == nil
	}
}
