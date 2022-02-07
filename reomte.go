package roada

import (
	"fmt"
	"log"
	"sync"
)

type remoteNode struct {
	nodeFullName string
	nodeAddr     string
	mutex        sync.Mutex
	client       *rpcClient
}
type reomtePool struct {
	routeMap sync.Map
}

func (node *remoteNode) connect() (*rpcClient, error) {
	log.Printf("[remote] dail, nodeFullName=%s, nodeAddr=%s\n", node.nodeFullName, node.nodeAddr)
	client, err := rpcDialHTTP("tcp", node.nodeAddr)
	if err != nil {
		log.Printf("[remote] dail failed, nodeFullName=%s, nodeAddr=%s, error=%s\n", node.nodeFullName, node.nodeAddr, err.Error())
		return nil, err
	}
	return client, nil
}

func (node *remoteNode) Call(nodeFullName string, rkey string, method string, args interface{}, reply interface{}) error {
	route := fmt.Sprintf("%s.%s", rkey, method)
	client, err := node.getClient()
	if err != nil {
		return err
	}
	if err := client.Call(route, args, reply); err != nil {
		return err
	}
	return nil
}

func (node *remoteNode) getClient() (*rpcClient, error) {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	if node.client != nil && node.client.isClose() {
		node.client = nil
	}
	if node.client == nil {
		var client, err = node.connect()
		if err != nil {
			return nil, err
		}
		node.client = client
	}
	return node.client, nil
}

func newClientPool() *reomtePool {
	var self = &reomtePool{
		routeMap: sync.Map{},
	}
	return self
}

func (pool *reomtePool) RouteAdd(nodeFullName string, nodeAddr string) error {
	var node = &remoteNode{
		nodeFullName: nodeFullName,
		nodeAddr:     nodeAddr,
	}
	pool.routeMap.Store(nodeFullName, node)
	return nil
}

func (pool *reomtePool) Call(nodeFullName string, rkey string, method string, args interface{}, reply interface{}) error {
	route := fmt.Sprintf("%s.%s", rkey, method)
	value, ok := pool.routeMap.Load(nodeFullName)
	if !ok {
		return fmt.Errorf("[remote] Call router not found, nodeFullName=%s, route=%s", nodeFullName, route)
	}
	node := value.(*remoteNode)
	return node.Call(nodeFullName, rkey, method, args, reply)
}
