package utils

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"minerdb/jrpc/rpc_common/constants"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"
)

var (
	conn *zk.Conn
)

func init() {
	var err error
	conn, _, err = zk.Connect([]string{constants.ZK_SERVER_ADDRESS}, constants.ZK_SESSION_TIMEOUT)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Zookeeper: %v", err))
	}
	fmt.Println("ZK transport_client connected successfully.")
	cleanupHook()
}

func cleanupHook() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			fmt.Println("ZK transport_client is closing.")
			conn.Close()
		}
	}()
}

func GetAllInstances(serviceName string) ([]*net.TCPAddr, error) {
	servicePath := fmt.Sprintf("%s/%s", constants.ZK_BASE_PATH, serviceName)
	children, _, err := conn.Children(servicePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get children for %s: %v", servicePath, err)
	}

	var instances []*net.TCPAddr
	for _, child := range children {
		addr, err := net.ResolveTCPAddr("tcp", child)
		if err != nil {
			fmt.Printf("Invalid address %s: %v\n", child, err)
			continue
		}
		instances = append(instances, addr)
	}
	// fmt.Printf("找到的instances为：%v\n", instances)
	return instances, nil
}

func RegisterService(serviceName string, addr *net.TCPAddr) error {
	servicePath := fmt.Sprintf("%s/%s/%s", constants.ZK_BASE_PATH, serviceName, addr.String())
	if err := createPath(conn, servicePath, []byte{}); err != nil {
		return err
	}
	return nil
}

func createPath(conn *zk.Conn, path string, data []byte) error {
	parts := strings.Split(path, "/")
	for i := 2; i <= len(parts); i++ {
		subPath := strings.Join(parts[:i], "/")
		exists, _, err := conn.Exists(subPath)
		if err != nil {
			return err
		}
		fmt.Printf("%v在zookeeper中是否存在%v\n", subPath, exists)
		if i == len(parts) {
			for i := 0; i < 5; i++ {
				fmt.Printf("暂时存储%v到zookeeper中\n", subPath)
				_, err := conn.Create(subPath, data, 1, zk.WorldACL(zk.PermAll))
				if err == zk.ErrNodeExists {
					fmt.Println("Node already exists, retrying...")
					time.Sleep(5000 * time.Millisecond)
					continue
				} else if err != nil {
					return err
				}
				break
			}
		} else {
			if !exists {
				fmt.Printf("永久存储%v到zookeeper中\n", subPath)
				_, err := conn.Create(subPath, data, 0, zk.WorldACL(zk.PermAll))
				// _, err := conn.Create(subPath, data, 0, zk.WorldACL(zk.PermAll))
				if err != nil {
					return err
				}
				fmt.Println(subPath, "创建成功!")
			}
		}
	}
	return nil
}
