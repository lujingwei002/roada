package roada

import (
	"log"
	"sync"
	"time"
)

type SlaveRegistry struct {
	Registry
	mutex sync.Mutex
}

func newSlaveRegistry() *SlaveRegistry {
	registry := &SlaveRegistry{
		Registry: Registry{
			version:      0,
			nodeDict:     make(map[string]*rNode),
			elemDict:     make(map[string]*rElement),
			deadNodeDict: make(map[string]*rNode),
		},
	}
	return registry
}

//首次克隆
func (registry *SlaveRegistry) Clone(nodeArr []*rNode, deadNodeArr []*rNode, version int64) error {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	t1 := time.Now()
	for _, node := range nodeArr {
		registry.nodeDict[node.NodeFullName] = node
		if err := registry.restoreNode(node); err != nil {
			return err
		}
	}
	for _, node := range deadNodeArr {
		registry.deadNodeDict[node.NodeFullName] = node
	}
	registry.version = version
	log.Printf("[registry] clone, node=%d, version=%d, cost=%+v\n",
		len(nodeArr), version, time.Since(t1))
	registry.Trace()
	return nil
}

func (registry *SlaveRegistry) NodeAdd(nodeFullName string, nodeAddr string) error {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	var node = &rNode{
		NodeFullName: nodeFullName,
		NodeAddr:     nodeAddr,
		UniqueDict:   make(map[string]bool),
		GroupDict:    make(map[string]bool),
	}
	registry.nodeDict[nodeFullName] = node
	log.Printf("[registry] node_add|%s|%s\n", nodeFullName, nodeAddr)
	return nil
}

//拉取最新
func (registry *SlaveRegistry) Merge(recordArr []string, version int64) error {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	t1 := time.Now()
	if registry.version+int64(len(recordArr)) != version {
		log.Printf("[registry] Merge failed, slave version=%d, len(recordArr)=%d, coord version=%d",
			registry.version, len(recordArr), version)
		return ErrMergeVersionErr
	}
	//log.Printf("[registry] pull begin, len(recordArr)=%d, version=%d\n", len(recordArr), version)
	for _, line := range recordArr {
		if err := registry.merge(line); err != nil {
			return err
		}
	}
	registry.version = version
	log.Printf("[registry] Merge, changed=%d, version=%d, cost=%+v\n",
		len(recordArr), version, time.Since(t1))
	if len(recordArr) > 0 {
		registry.Trace()
	}
	return nil
}

func (registry *SlaveRegistry) Set(rkey string, nodeFullName string) error {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	if err := registry.set(rkey, nodeFullName); err != nil {
		return err
	}
	return nil
}

func (registry *SlaveRegistry) Get(rkey string) (*rElement, bool) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	return registry.get(rkey)
}

func (registry *SlaveRegistry) Del(rkey string, nodeFullName string) error {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	if err := registry.del(rkey, nodeFullName); err != nil {
		return err
	}
	return nil
}

func (registry *SlaveRegistry) GroupAdd(rkey string, member string, nodeFullName string) error {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	if err := registry.groupAdd(rkey, member, nodeFullName); err != nil {
		return err
	}
	return nil
}

func (registry *SlaveRegistry) GroupDel(rkey string, member string, nodeFullName string) error {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	if err := registry.groupDel(rkey, member, nodeFullName); err != nil {
		return err
	}
	return nil
}
