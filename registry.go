package roada

import (
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	nodeStatusNormal  = 0
	nodeStatusDel     = 1
	nodeStatusTimeout = 2
)

const (
	rTypeUnique = 1
	rTypeGroup  = 2
)

type coordDataDir struct {
	path    string
	version int64
}

type coordDataDirSlice []*coordDataDir

func (self coordDataDirSlice) Len() int {
	return len(self)
}

func (self coordDataDirSlice) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self coordDataDirSlice) Less(i, j int) bool {
	return self[i].version < self[j].version
}

func newCoordDataDir(path string) *coordDataDir {
	name := filepath.Base(path)
	args := strings.Split(name, "#")
	if len(args) != 2 {
		return nil
	}
	version, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return nil
	}
	return &coordDataDir{
		path:    path,
		version: version,
	}
}

type rGroup struct {
	memberDict map[string]*rNode
	memberArr  []string
}

type rNode struct {
	Version           int64
	NodeName          string
	NodeFullName      string
	NodeAddr          string
	Status            int
	StartUpTime       int64
	DeadTime          int64
	LastHeartbeatTime int64
	UniqueDict        map[string]bool
	GroupDict         map[string]bool
}

type rElement struct {
	typ   int8
	node  *rNode
	group *rGroup
}

//注册表
type Registry struct {
	version      int64
	slaveVer     int64 //所有节点中最小的版本
	nameVer      int64
	nodeNameVer  int64
	vlog         *vlog
	nodeDict     map[string]*rNode
	road         *Road
	elemDict     map[string]*rElement
	deadNodeDict map[string]*rNode
	dataDir      string //保存目录
}

func newRegistry(road *Road) *Registry {
	registry := &Registry{
		road:         road,
		version:      0,
		nameVer:      time.Now().Unix(),
		nodeNameVer:  time.Now().Unix(),
		nodeDict:     make(map[string]*rNode),
		elemDict:     make(map[string]*rElement),
		deadNodeDict: make(map[string]*rNode),
		vlog:         newVlog(road),
	}
	return registry
}

func (registry *Registry) Create() error {
	registry.dataDir = fmt.Sprintf("./coord#%d", registry.nameVer)
	if err := registry.vlog.Create(registry.nameVer); err != nil {
		return err
	}
	return nil
}

func (registry *Registry) Restore() error {
	//如果末指定版本，则从目录里搜索最高的版本来恢复
	dataRootDir := "./"
	files, err := ioutil.ReadDir(dataRootDir)
	if err != nil {
		return err
	}
	dataDirArr := make(coordDataDirSlice, 0)
	for _, file := range files {
		if !file.IsDir() {
			continue
		} else {
			dataDir := newCoordDataDir(fmt.Sprintf("%s/%s", dataRootDir, file.Name()))
			if dataDir == nil {
				continue
			}
			dataDirArr = append(dataDirArr, dataDir)
		}
	}
	if len(dataDirArr) == 0 {
		return ErrRestoreDataDirNotFound
	}
	sort.Sort(dataDirArr)
	registry.nameVer = dataDirArr[len(dataDirArr)-1].version
	registry.dataDir = fmt.Sprintf("./coord#%d", registry.nameVer)
	log.Printf("[registry] restore, nameVer=%d dataDir=%s\n", registry.nameVer, registry.dataDir)
	//从文件恢复
	if err := registry.restoreFromBin(); err != nil {
		return err
	}
	//从日志恢复
	if err := registry.restoreFromLog(); err != nil {
		return err
	}
	if err := registry.vlog.Create(registry.nameVer); err != nil {
		return err
	}
	if err := registry.vlog.RestoreCache(registry.slaveVer, registry.version); err != nil {
		return err
	}
	return nil
}

func (registry *Registry) restoreFromLog() error {
	t1 := time.Now()
	if _, err := os.Stat(registry.dataDir); err != nil && os.IsNotExist(err) {
		//if coord#version dir not exist
		return err
	}
	//rename current log file name
	if err := registry.renameLogFile(registry.dataDir, registry.nameVer); err != nil {
		return err
	}
	//从日志中恢复
	files, err := ioutil.ReadDir(registry.dataDir)
	if err != nil {
		return err
	}
	logFileArr := make(vlogFileSlice, 0)
	for _, file := range files {
		if file.IsDir() {
			continue
		} else {
			logFile := newVlogFile(fmt.Sprintf("%s/%s", registry.dataDir, file.Name()))
			if logFile == nil {
				continue
			}
			if logFile.maxVer <= registry.version {
				continue
			}
			logFileArr = append(logFileArr, logFile)
		}
	}
	sort.Sort(logFileArr)
	recordArr := make([]string, 0)
	for _, logFile := range logFileArr {
		lineArr, err := logFile.readLog(registry.version)
		if err != nil {
			continue
		}
		recordArr = append(recordArr, lineArr...)
	}
	//lastVersion := registry.version
	//按顺序执行一次
	for _, record := range recordArr {
		if err := registry.merge(record); err != nil {
			return err
		}
	}
	log.Printf("[registry] restoreFromLog, len(recordArr)=%d, version=%d, cost=%+v\n",
		len(recordArr), registry.version, time.Since(t1))
	return nil
}

func (registry *Registry) restoreFromBin() error {
	t1 := time.Now()
	filePath := fmt.Sprintf("%s/coord#%d.bin", registry.dataDir, registry.nameVer)
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	encoder := gob.NewDecoder(f)
	if err := encoder.Decode(&registry.version); err != nil {
		log.Printf("[registry] restoreFromBin failed, version decode err, err=%s\n", err.Error())
		return err
	}
	if err := encoder.Decode(&registry.slaveVer); err != nil {
		log.Printf("[registry] restoreFromBin failed, slaveVer decode err, err=%s\n", err.Error())
		return err
	}
	var nameVer int64
	if err := encoder.Decode(&nameVer); err != nil {
		log.Printf("[registry] restoreFromBin failed, nameVe decoder err, err=%s\n", err.Error())
		return err
	}
	if nameVer != registry.nameVer {
		return fmt.Errorf("registry restore failed, exptect nameVer=%d, nameVer=%d", registry.nameVer, nameVer)
	}
	if err := encoder.Decode(&registry.nodeNameVer); err != nil {
		log.Printf("[registry] restoreFromBin failed, nodeNameVer decode err, err=%s\n", err.Error())
		return err
	}
	if err := encoder.Decode(&registry.nodeDict); err != nil {
		log.Printf("[registry] restoreFromBin failed, nodeDict decode err, err=%s\n", err.Error())
		return err
	}
	if err := encoder.Decode(&registry.deadNodeDict); err != nil {
		log.Printf("[registry] restoreFromBin failed, deadNodeDict decode err, err=%s\n", err.Error())
		return err
	}
	//don't recover coord node
	delete(registry.nodeDict, CoordNodeName)
	for _, node := range registry.nodeDict {
		if err := registry.restoreNode(node); err != nil {
			return err
		}
	}
	now := time.Now().Unix()
	for _, node := range registry.nodeDict {
		node.LastHeartbeatTime = now
	}
	log.Printf("[registry] restoreFromBin, node=%d, slaveVer=%d, version=%d, cost=%+v\n",
		len(registry.nodeDict), registry.slaveVer, registry.version, time.Since(t1))
	return nil
}

func (registry *Registry) restoreNode(node *rNode) error {
	t1 := time.Now()
	for rkey := range node.UniqueDict {
		if _, ok := registry.elemDict[rkey]; ok {
			log.Printf("[registry] restoreNode failed, key duplicate, rkey=%s, nodeFullName=%s\n",
				rkey, node.NodeFullName)
			return ErrKeyDuplicate
		}
		registry.elemDict[rkey] = &rElement{
			typ:  rTypeUnique,
			node: node,
		}
	}
	for rkey := range node.GroupDict {
		pats := strings.Split(rkey, "|")
		if len(pats) < 2 {
			continue
		}
		rkey = pats[0]
		member := pats[1]
		elem, ok := registry.elemDict[rkey]
		if !ok {
			elem := &rElement{
				typ: rTypeGroup,
				group: &rGroup{
					memberDict: map[string]*rNode{member: node},
					memberArr:  []string{member},
				},
			}
			registry.elemDict[rkey] = elem
		} else if elem.typ != rTypeGroup {
			log.Printf("[registry] restoreNode failed, key type err, rkey=%s, member=%s, nodeFullName=%s\n",
				rkey, member, node.NodeFullName)
			return ErrKeyTypeNotGroup
		} else {
			group := elem.group
			group.memberDict[member] = node
			group.memberArr = append(group.memberArr, member)
		}
	}
	log.Printf("[registry] restoreNode, nodeFullName=%s, cost=%+v\n", node.NodeFullName, time.Since(t1))
	return nil
}

func (registry *Registry) merge(record string) error {
	log.Printf("[registry] merge %s\n", record)
	args := strings.Split(record, "|")
	if len(args) < 2 {
		return ErrRegistryRestoreFormat
	}
	typ := args[0]
	version, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return err
	}
	createTime, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return err
	}
	if registry.version+1 != version {
		return ErrMergeVersionErr
	}
	if typ == "node_add" {
		if len(args) != 6 {
			return ErrRegistryRestoreFormat
		}
		if err := registry.nodeAdd(version, createTime, args[3], args[4], args[5]); err != nil {
			log.Printf("[registry] merge failed, nodeAdd, record=%s, error=%s\n", record, err.Error())
			return err
		}
	} else if typ == "node_del" {
		if len(args) != 5 {
			return ErrRegistryRestoreFormat
		}
		reason, err := strconv.Atoi(args[4])
		if err != nil {
			return err
		}
		if err := registry.nodeDel(args[3], reason, createTime); err != nil {
			log.Printf("[registry] merge failed, node_del, record=%s, error=%s\n", record, err.Error())
			return err
		}
	} else if typ == "node_dead" {
		if len(args) != 4 {
			return ErrRegistryRestoreFormat
		}
		if err := registry.nodeDead(args[3]); err != nil {
			log.Printf("[registry] merge failed, node_dead, record=%s, error=%s\n", record, err.Error())
			return err
		}
	} else if typ == "set" {
		if len(args) != 5 {
			return ErrRegistryRestoreFormat
		}
		if err := registry.set(args[3], args[4]); err != nil {
			log.Printf("[registry] merge failed, set, record=%s, error=%s\n", record, err.Error())
			return err
		}
	} else if typ == "del" {
		if len(args) != 5 {
			return ErrRegistryRestoreFormat
		}
		if err := registry.del(args[3], args[4]); err != nil {
			log.Printf("[registry] merge failed, set, record=%s, error=%s\n", record, err.Error())
			return err
		}
	} else if typ == "group_add" {
		if len(args) != 6 {
			return ErrRegistryRestoreFormat
		}
		if err := registry.groupAdd(args[3], args[4], args[5]); err != nil {
			log.Printf("[registry] merge failed, group_add, record=%s, error=%s\n", record, err.Error())
			return err
		}
	} else if typ == "group_del" {
		if len(args) != 6 {
			return ErrRegistryRestoreFormat
		}
		if err := registry.groupDel(args[3], args[4], args[5]); err != nil {
			log.Printf("[registry] merge failed, group_del, record=%s, error=%s\n", record, err.Error())
			return err
		}
	}
	registry.version = version
	return nil
}

func (registry *Registry) Save() error {
	t1 := time.Now()
	filePath := fmt.Sprintf("%s/coord#%d.bin", registry.dataDir, registry.nameVer)
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Printf("[registry] save failed, error=%s\n", err.Error())
		return err
	}
	defer f.Close()
	encoder := gob.NewEncoder(f)
	if err := encoder.Encode(registry.version); err != nil {
		log.Printf("[registry] save failed, error=%s\n", err.Error())
		return err
	}
	if err := encoder.Encode(registry.slaveVer); err != nil {
		log.Printf("[registry] save failed, error=%s\n", err.Error())
		return err
	}
	if err := encoder.Encode(registry.nameVer); err != nil {
		log.Printf("[registry] save failed, error=%s\n", err.Error())
		return err
	}
	if err := encoder.Encode(registry.nodeNameVer); err != nil {
		log.Printf("[registry] save failed, error=%s\n", err.Error())
		return err
	}
	if err := encoder.Encode(registry.nodeDict); err != nil {
		log.Printf("[registry] save failed, error=%s\n", err.Error())
		return err
	}
	if err := encoder.Encode(registry.deadNodeDict); err != nil {
		log.Printf("[registry] save failed, error=%s\n", err.Error())
		return err
	}
	if err := registry.vlog.Discard(registry.slaveVer, registry.version); err != nil {
		log.Printf("[registry] vlog discard failed, error=%s\n", err.Error())
	}
	log.Printf("[registry] save, cost=%+v\n", time.Since(t1))
	return nil
}

func (registry *Registry) Trace() {
	log.Println("=============registry==================")
	log.Printf("nameVer:%d\n", registry.nameVer)
	log.Printf("version:%d\n", registry.version)
	log.Printf("slaveVer:%d\n", registry.slaveVer)
	for _, node := range registry.deadNodeDict {
		log.Printf("dead:node=%s status=%d, version=%d, dead=%s\n",
			node.NodeFullName, node.Status, node.Version,
			time.Unix(node.DeadTime, 0).Format("2006-01-02 15:04:05"))
	}
	for _, node := range registry.nodeDict {
		log.Printf("alive:%s status=%d, version=%d, startup=%s\n",
			node.NodeFullName, node.Status, node.Version,
			time.Unix(node.StartUpTime, 0).Format("2006-01-02 15:04:05"))
	}
	count := 0
	for rkey, elem := range registry.elemDict {
		if elem.typ == rTypeUnique {
			log.Printf("rkey:%s|%s\n", elem.node.NodeFullName, rkey)
		} else if elem.typ == rTypeGroup {
			for member, node := range elem.group.memberDict {
				log.Printf("rkey:%s|%s|%s\n", node.NodeFullName, rkey, member)
			}
		}
		count = count + 1
		if count >= 10 {
			break
		}
	}
	rkeyTotal := len(registry.elemDict)
	if rkeyTotal-count > 0 {
		log.Printf("......%d remain\n", len(registry.elemDict)-count)
	}
	log.Println("=============registry==================")
}

func (registry *Registry) GC() error {
	t1 := time.Now()
	now := t1.Unix()
	removeNodeArr := make([]*rNode, 0)
	for _, node := range registry.deadNodeDict {
		if now-node.DeadTime > 600 {
			removeNodeArr = append(removeNodeArr, node)
			log.Printf("[registry] gc, node remove, nodeFullName=%s, DeadTime=%s\n",
				node.NodeFullName, time.Unix(node.DeadTime, 0).Format("2006-01-02 15:04:05"))
		}
	}
	for _, node := range removeNodeArr {
		registry.NodeDead(node.NodeFullName)
	}
	log.Printf("[registry] gc, cost=%+v\n", time.Since(t1))
	return nil
}

func (registry *Registry) CheckHeartbeat() error {
	//删除过期节点
	now := time.Now().Unix()
	for _, node := range registry.nodeDict {
		if now-node.LastHeartbeatTime > 120 {
			registry.NodeTimeout(node.NodeFullName)
			log.Printf("[registry] checkHeartbeat, node expire, nodeFullName=%s, lastHeartbeatTime=%d\n",
				node.NodeFullName, node.LastHeartbeatTime)
		}
	}
	//更新slaveVer
	var slaveVer int64 = registry.version
	for _, node := range registry.nodeDict {
		if node.Version < slaveVer {
			slaveVer = node.Version
		}
	}
	registry.slaveVer = slaveVer
	return nil
}

func (registry *Registry) Pull(version int64) ([]string, int64) {
	recordArr := registry.vlog.Pull(version)
	return recordArr, registry.version
}

func (registry *Registry) Heartbeat(nodeFullName string, version int64) error {
	log.Printf("[registry] heartbeat, nodeFullName=%s, version=%d\n", nodeFullName, version)
	node, ok := registry.nodeDict[nodeFullName]
	if !ok {
		return fmt.Errorf("[registry] Heartbeat, node not found, nodeFullName=%s", nodeFullName)
	}
	node.Version = version
	node.LastHeartbeatTime = time.Now().Unix()
	node.Status = nodeStatusNormal
	return nil
}

func (registry *Registry) NodeAdd(nodeName string, nodeAddr string) (*rNode, error) {
	var nodeFullName = nodeName
	registry.nodeNameVer = registry.nodeNameVer + 1
	registry.version++
	nodeFullName = fmt.Sprintf("%s#%d", nodeName, registry.nodeNameVer)
	var node = &rNode{
		NodeName:          nodeName,
		NodeFullName:      nodeFullName,
		NodeAddr:          nodeAddr,
		Status:            nodeStatusNormal,
		UniqueDict:        make(map[string]bool),
		GroupDict:         make(map[string]bool),
		LastHeartbeatTime: time.Now().Unix(),
		Version:           registry.version,
		StartUpTime:       time.Now().Unix(),
	}
	registry.nodeDict[nodeFullName] = node
	registry.vlog.NodeAdd(registry.version, nodeName, nodeFullName, nodeAddr)
	return node, nil
}

func (registry *Registry) nodeAdd(version int64, createTime int64, nodeName string, nodeFullName string, nodeAddr string) error {
	var node = &rNode{
		NodeName:          nodeName,
		NodeFullName:      nodeFullName,
		NodeAddr:          nodeAddr,
		Status:            nodeStatusNormal,
		UniqueDict:        make(map[string]bool),
		GroupDict:         make(map[string]bool),
		LastHeartbeatTime: time.Now().Unix(),
		Version:           version,
		StartUpTime:       createTime,
	}
	registry.nodeDict[nodeFullName] = node
	return nil
}

func (registry *Registry) NodeDead(nodeFullName string) error {
	if err := registry.nodeDead(nodeFullName); err != nil {
		return err
	}
	registry.version++
	registry.vlog.NodeDead(registry.version, nodeFullName)
	return nil
}

func (registry *Registry) nodeDead(nodeFullName string) error {
	node, ok := registry.deadNodeDict[nodeFullName]
	if !ok {
		log.Printf("[registry] nodeDead failed, node not found, nodeFullName=%s\n",
			nodeFullName)
		return ErrNodeNotFound
	}
	delete(registry.deadNodeDict, node.NodeFullName)
	return nil
}

func (registry *Registry) NodeDel(nodeFullName string) error {
	if err := registry.nodeDel(nodeFullName, nodeStatusDel, time.Now().Unix()); err != nil {
		return err
	}
	registry.version++
	registry.vlog.NodeDel(registry.version, nodeFullName, nodeStatusDel)
	return nil
}

func (registry *Registry) NodeTimeout(nodeFullName string) error {
	if err := registry.nodeDel(nodeFullName, nodeStatusTimeout, time.Now().Unix()); err != nil {
		return err
	}
	registry.version++
	registry.vlog.NodeDel(registry.version, nodeFullName, nodeStatusTimeout)
	return nil
}

func (registry *Registry) nodeDel(nodeFullName string, reason int, createTime int64) error {
	t1 := time.Now()
	node, ok := registry.nodeDict[nodeFullName]
	if !ok {
		log.Printf("[registry] nodeDel failed, node not found, nodeFullName=%s\n",
			nodeFullName)
		return ErrNodeNotFound
	}
	rkeyTotal := 0
	removeElemArr := make([]string, 0)
	for rkey := range node.UniqueDict {
		removeElemArr = append(removeElemArr, rkey)
	}
	for _, rkey := range removeElemArr {
		rkeyTotal++
		elem, ok := registry.elemDict[rkey]
		if !ok {
			log.Printf("[registry] nodeDel failed, key not found, rkey=%s,  nodeFullName=%s\n",
				rkey, node.NodeFullName)
			continue
		}
		if elem.typ != rTypeUnique {
			log.Printf("[registry] nodeDel failed, key type err, rkey=%s,  nodeFullName=%s\n",
				rkey, node.NodeFullName)
			continue
		}
		delete(registry.elemDict, rkey)
		//log.Printf("[registry] nodeDel, nodeFullName=%s, rkey=%s\n", node.NodeFullName, rkey)
	}
	removeElemArr = make([]string, 0)
	for rkey := range node.GroupDict {
		removeElemArr = append(removeElemArr, rkey)
	}
	for _, rkey := range removeElemArr {
		//log.Printf("[registry] nodeDel, nodeFullName=%s, rkey=%s\n", node.NodeFullName, rkey)
		rkeyTotal++
		pats := strings.Split(rkey, "|")
		if len(pats) == 2 {
			rkey = pats[0]
			member := pats[1]
			elem, ok := registry.elemDict[rkey]
			if !ok {
				log.Printf("[registry] nodeDel failed, key not found, rkey=%s, member=%s, nodeFullName=%s\n",
					rkey, member, node.NodeFullName)
				continue
			}
			if elem.typ != rTypeGroup {
				log.Printf("[registry] nodeDel failed, key type err, rkey=%s, member=%s, nodeFullName=%s\n",
					rkey, member, node.NodeFullName)
				continue
			}
			group := elem.group
			memberIndex := -1
			for index, _member := range group.memberArr {
				if _member == member {
					memberIndex = index
					break
				}
			}
			if memberIndex == -1 {
				log.Printf("[registry] nodeDel failed, member not found, rkey=%s, member=%s, nodeFullName=%s\n",
					rkey, member, node.NodeFullName)
				continue
			}
			group.memberArr = append(group.memberArr[:memberIndex], group.memberArr[memberIndex+1:]...)
			delete(group.memberDict, member)
		}
	}
	delete(registry.nodeDict, node.NodeFullName)
	delete(registry.nodeDict, node.NodeName)
	registry.deadNodeDict[node.NodeFullName] = node
	node.Status = reason
	node.DeadTime = createTime
	log.Printf("[registry] nodeDel, nodeFullName=%s, rkey=%d, cost=%+v\n",
		nodeFullName, rkeyTotal, time.Since(t1))
	return nil
}

func (registry *Registry) Set(rkey string, nodeFullName string) error {
	if err := registry.set(rkey, nodeFullName); err != nil {
		return err
	}
	registry.version++
	registry.vlog.Set(registry.version, rkey, nodeFullName)
	return nil
}

func (registry *Registry) set(rkey string, nodeFullName string) error {
	var node, ok = registry.nodeDict[nodeFullName]
	if !ok {
		return fmt.Errorf("[registry] Set, node not found, nodeFullName=%s, rkey=%s", nodeFullName, rkey)
	}
	if lastElem, ok := registry.elemDict[rkey]; ok {
		if lastElem.node.NodeFullName != nodeFullName {
			return ErrKeyDuplicate
		}
		return nil
	}
	elem := &rElement{
		typ:  rTypeUnique,
		node: node,
	}
	registry.elemDict[rkey] = elem
	node.UniqueDict[rkey] = true
	return nil
}

func (registry *Registry) Get(rkey string) (*rElement, bool) {
	return registry.get(rkey)
}

func (registry *Registry) get(rkey string) (*rElement, bool) {
	elem, ok := registry.elemDict[rkey]
	if !ok {
		return nil, false
	}
	return elem, true
}

func (registry *Registry) Del(rkey string, nodeFullName string) error {
	if err := registry.del(rkey, nodeFullName); err != nil {
		return err
	}
	registry.version++
	registry.vlog.Del(registry.version, rkey, nodeFullName)
	return nil
}

func (registry *Registry) del(rkey string, nodeFullName string) error {
	elem, ok := registry.elemDict[rkey]
	if !ok {
		log.Printf("[registry] del failed, key not found, rkey=%s, nodeFullName=%s\n", rkey, nodeFullName)
		return ErrKeyNotFound
	}
	if elem.typ != rTypeUnique {
		return ErrKeyTypeNotUnique
	}
	delete(registry.elemDict, rkey)
	return nil
}

func (registry *Registry) GroupAdd(rkey string, member string, nodeFullName string) error {
	if err := registry.groupAdd(rkey, member, nodeFullName); err != nil {
		return err
	}
	registry.version++
	registry.vlog.GroupAdd(registry.version, rkey, member, nodeFullName)
	return nil
}

func (registry *Registry) groupAdd(rkey string, member string, nodeFullName string) error {
	var node, ok = registry.nodeDict[nodeFullName]
	if !ok {
		log.Printf("[registry] groupAdd failed, node not found,  rkey=%s, member=%s, nodeFullName=%s\n",
			rkey, member, nodeFullName)
		return ErrNodeNotFound
	}
	elem, ok := registry.elemDict[rkey]
	if !ok {
		elem := &rElement{
			typ: rTypeGroup,
			group: &rGroup{
				memberDict: map[string]*rNode{member: node},
				memberArr:  []string{member},
			},
		}
		registry.elemDict[rkey] = elem
		node.GroupDict[fmt.Sprintf("%s|%s", rkey, member)] = true
		return nil
	} else if elem.typ == rTypeGroup {
		group := elem.group
		if lastNode, ok := group.memberDict[member]; ok {
			if lastNode.NodeFullName != nodeFullName {
				log.Printf("[registry] groupAdd failed, member duplicate, rkey=%s, member=%s\n",
					rkey, member, nodeFullName)
				return ErrGroupMemberDuplicate
			}
		}
		group.memberDict[member] = node
		group.memberArr = append(group.memberArr, member)
		node.GroupDict[fmt.Sprintf("%s|%s", rkey, member)] = true
		return nil
	} else {
		log.Printf("[registry] groupAdd failed, key type err, rkey=%s, member=%s, nodeFullName=%s\n",
			rkey, member, nodeFullName)
		return ErrKeyTypeNotGroup
	}
}

func (registry *Registry) GroupDel(rkey string, member string, nodeFullName string) error {
	if err := registry.groupDel(rkey, member, nodeFullName); err != nil {
		return err
	}
	registry.version++
	registry.vlog.GroupDel(registry.version, rkey, member, nodeFullName)
	return nil
}

func (registry *Registry) groupDel(rkey string, member string, nodeFullName string) error {
	elem, ok := registry.elemDict[rkey]
	if !ok {
		log.Printf("[registry] groupDel failed, key not found, rkey=%s, member=%s, nodeFullName=%s\n",
			rkey, member, nodeFullName)
		return ErrKeyNotFound
	}
	if elem.typ != rTypeGroup {
		log.Printf("[registry] groupDel failed, key type err, rkey=%s, member=%s, nodeFullName=%s\n",
			rkey, member, nodeFullName)
		return ErrKeyTypeNotGroup
	}
	group := elem.group
	if node, ok := group.memberDict[member]; !ok {
		log.Printf("[registry] groupDel failed, member not found, rkey=%s, member=%s, nodeFullName=%s\n",
			rkey, member, nodeFullName)
		return ErrMemberNotFound
	} else if node.NodeFullName != nodeFullName {
		log.Printf("[registry] groupDel failed, member not own, rkey=%s, member=%s, nodeFullName=%s\n",
			rkey, member, nodeFullName)
		return ErrMemberNotOwn
	} else {
		memberIndex := -1
		for index, _member := range group.memberArr {
			if _member == member {
				memberIndex = index
				break
			}
		}
		if memberIndex == -1 {
			log.Printf("[registry] groupDel failed, member not found, rkey=%s, member=%s, nodeFullName=%s\n",
				rkey, member, nodeFullName)
			return ErrMemberNotFound
		}
		group.memberArr = append(group.memberArr[:memberIndex], group.memberArr[memberIndex+1:]...)
		delete(group.memberDict, member)
	}
	return nil
}
