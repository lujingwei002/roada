package roada

import (
	"fmt"
	"log"
	"sync"
)

type lrElement struct {
	typ   int8
	group *lrGroup
}

type lrGroup struct {
	memberDict map[string]bool
	memberArr  []string
}

//注册表
type LocalRegistry struct {
	elemDict map[string]*lrElement
	mutex    sync.Mutex
}

func newLocalRegistry() *LocalRegistry {
	registry := &LocalRegistry{
		elemDict: make(map[string]*lrElement),
	}
	return registry
}

func (registry *LocalRegistry) Set(rkey string) error {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	if _, ok := registry.elemDict[rkey]; ok {
		return ErrKeyDuplicate
	}
	value := &lrElement{
		typ: rTypeUnique,
	}
	registry.elemDict[rkey] = value
	log.Printf("[local_registry] set|%s\n", rkey)
	return nil
}

func (registry *LocalRegistry) Get(rkey string) (*lrElement, bool) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	value, ok := registry.elemDict[rkey]
	if !ok {
		return nil, false
	}
	return value, true
}

func (registry *LocalRegistry) Del(rkey string) error {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	_, ok := registry.elemDict[rkey]
	if !ok {
		log.Printf("[local_registry] Del key not found, rkey=%s\n", rkey)
		return ErrKeyNotFound
	}
	delete(registry.elemDict, rkey)
	log.Printf("[local_registry] Del, rkey=%s\n", rkey)
	return nil
}

func (registry *LocalRegistry) GroupAdd(rkey string, member string) error {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	elem, ok := registry.elemDict[rkey]
	if !ok {
		elem := &lrElement{
			typ: rTypeGroup,
			group: &lrGroup{
				memberDict: map[string]bool{member: true},
				memberArr:  []string{member},
			},
		}
		registry.elemDict[rkey] = elem
		log.Printf("[local_registry] group_add|%s|%s\n", rkey, member)
		return nil
	} else if elem.typ == rTypeGroup {
		group := elem.group
		group.memberDict[member] = true
		group.memberArr = append(group.memberArr, member)
		log.Printf("[local_registry] group_add|%s|%s\n", rkey, member)
		return nil
	} else {
		return fmt.Errorf("[local_registry] GroupAdd, key type err, rkey=%s, member=%s", rkey, member)
	}
}
