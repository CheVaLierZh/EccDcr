package EccDcr

import (
	"log"
	"net"
	"net/rpc"
	"strings"
)

const (
	CLOUD_PORT = ":9876"
)

func Call(address string, rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatal("dialing", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		return false
	}
	return true
}

func GetOutBoundIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		return "", err
	}

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return strings.Split(localAddr.String(), ":")[0], nil
}
