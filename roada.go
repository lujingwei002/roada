package roada

import (
	//	"harbor"

	"log"
	"math/rand"
	"net"
	"net/http"
)

const (
	CoordNodeName         = "coord"
	CoordServiceName      = "coord"
	CoordSlaveServiceName = "coord_slave"
	NodeServiceName       = "node"
)

const EmptyString = ""

const (
	RKeyTypeString = 0
	RkeyTypeSet    = 1
	RKeyTypeLocal  = 2
)

type Road struct {
	NodeFullName  string
	NodeName      string
	coord         *CoordClient
	localRegistry *LocalRegistry
	slaveRegistry *SlaveRegistry
	remote        *reomtePool
	server        *rpcServer
	router        *Router
	callTimeout   int
}

var roada = NewRoad()

func NewRoad() *Road {
	var road = &Road{
		localRegistry: newLocalRegistry(),
		slaveRegistry: newSlaveRegistry(),
		remote:        newClientPool(),
		callTimeout:   5,
	}
	road.server = rpcNewServer(road)
	road.coord = newCoordClient(road)
	road.router = newRouter(road)
	return road
}

func (road *Road) GroupAdd(rkey string, member string) error {
	if err := road.coord.GroupAdd(rkey, member); err != nil {
		return err
	}
	return nil
}

func (road *Road) GroupDel(rkey string, member string) error {
	if err := road.coord.GroupDel(rkey, member); err != nil {
		return err
	}
	return nil
}

func (road *Road) Set(rkey string) error {
	if err := road.coord.Set(rkey); err != nil {
		return err
	}
	return nil
}

func (road *Road) Del(rkey string) error {
	if err := road.coord.Del(rkey); err != nil {
		return err
	}
	return nil
}

func (road *Road) Get(rkey string) (string, error) {
	nodeFullName, err := road.coord.Get(rkey)
	if err != nil {
		return EmptyString, err
	}
	return nodeFullName, nil
}

func (road *Road) LocalSet(rkey string) error {
	if err := road.localRegistry.Set(rkey); err != nil {
		return err
	}
	return nil
}

func (road *Road) LocalDel(rkey string) error {
	if err := road.localRegistry.Del(rkey); err != nil {
		return err
	}
	return nil
}

func (road *Road) LocalGroupAdd(rkey string, member string) error {
	if err := road.localRegistry.GroupAdd(rkey, member); err != nil {
		return err
	}
	return nil
}

func (road *Road) Register(svr interface{}) (*Service, error) {
	return road.router.Register(svr)
}

func (road *Road) Handle(pattern string, h HandlerInterface) error {
	return road.router.Handle(pattern, h)
}

func (road *Road) DelHandle(pattern string, h HandlerInterface) error {
	return road.router.DelHandle(pattern, h)
}

func (r *Road) Master(localAddr string, restore bool) error {
	nodeName := CoordNodeName
	//coordAddr := localAddr
	r.server.HandleHTTP()
	lis, err := net.Listen("tcp", localAddr)
	if err != nil {
		return err
	}
	go http.Serve(lis, nil)

	r.NodeName = nodeName
	//defer r.Quit()
	if true {
		nodeFullName, err := newCoordService(r, restore)
		if err != nil {
			return err
		}
		//链接coord节点
		/*if err := r.slaveRegistry.NodeAdd(CoordNodeName, coordAddr); err != nil {
			return err
		}
		if err := r.remote.RouteAdd(CoordNodeName, coordAddr); err != nil {
			return err
		}*/
		if err := r.localRegistry.Set(CoordServiceName); err != nil {
			return err
		}
		//创建节点
		/*reply, err := r.coord.nodeAdd(nodeName, localAddr)
		if err != nil {
			return err
		}
		r.NodeFullName = reply.NodeFullName*/
		r.NodeFullName = nodeFullName
		//创建node服务
		/*if err := newNodeService(r); err != nil {
			return err
		}*/
	}
	log.Printf("[roada] %s started\n", r.NodeFullName)
	return nil
}

func (r *Road) Slave(nodeName string, localAddr string, coordAddr string) error {
	r.server.HandleHTTP()
	lis, err := net.Listen("tcp", localAddr)
	if err != nil {
		return err
	}
	go http.Serve(lis, nil)

	r.NodeName = nodeName
	//defer r.Quit()
	if true {
		if err := newSlaveService(r); err != nil {
			return err
		}
		//链接coord节点
		if err := r.slaveRegistry.NodeAdd(CoordNodeName, coordAddr); err != nil {
			return err
		}
		if err := r.remote.RouteAdd(CoordNodeName, coordAddr); err != nil {
			return err
		}
		if err := r.slaveRegistry.Set(CoordServiceName, CoordNodeName); err != nil {
			return err
		}
		//注册节点
		reply, err := r.coord.NodeAdd(nodeName, localAddr)
		if err != nil {
			return err
		}
		r.NodeFullName = reply.NodeFullName
		//创建node服务
		if err := newNodeService(r); err != nil {
			return err
		}
	}
	log.Printf("[roada] %s started\n", r.NodeFullName)
	return nil
}

func (r *Road) SearchRemote(rkey string) (string, string, error) {
	//本地注册表查找
	elem, ok := r.slaveRegistry.Get(rkey)
	if !ok {
		//到coord查找
		nodeFullName, err := r.coord.Get(rkey)
		if err != nil {
			log.Printf("[roada] SearchRemote failed, error=%s\n", err.Error())
			return "", "", ErrKeyNotFound
		}
		return rkey, nodeFullName, nil
	} else if elem.typ == rTypeGroup {
		group := elem.group
		if len(group.memberArr) <= 0 {
			log.Printf("[roada] SearchRemote failed, rkey not found, rkey=%s\n", rkey)
			return "", "", ErrKeyNotFound
		}
		index := rand.Intn(len(group.memberArr))
		memberName := group.memberArr[index]
		node := group.memberDict[memberName]
		nodeFullName := node.NodeFullName
		return memberName, nodeFullName, nil
	} else if elem.typ == rTypeUnique {
		nodeFullName := elem.node.NodeFullName
		return rkey, nodeFullName, nil
	} else {
		log.Printf("[roada] SearchRemote failed, rkey not found, rkey=%s\n", rkey)
		return "", "", ErrKeyNotFound
	}
}

func (r *Road) SearchLocal(rkey string) (string, error) {
	//本地注册表查找
	elem, ok := r.localRegistry.Get(rkey)
	if !ok {
		return "", ErrKeyNotFound
	} else if elem.typ == rTypeGroup {
		group := elem.group
		if len(group.memberArr) <= 0 {
			log.Printf("[roada] SearchLocal failed, rkey not found, rkey=%s\n", rkey)
			return "", ErrKeyNotFound
		}
		index := rand.Intn(len(group.memberArr))
		memberName := group.memberArr[index]
		return memberName, nil
	} else if elem.typ == rTypeUnique {
		return rkey, nil
	} else {
		log.Printf("[roada] SearchLocal failed, rkey not found, rkey=%s\n", rkey)
		return "", ErrKeyNotFound
	}
}

func (r *Road) Call(rkey string, method string, args interface{}, reply interface{}) error {
	if rkey, err := r.SearchLocal(rkey); err == nil {
		//本地key,直接通过router调用
		if err := r.router.LocalCall(rkey, method, args, reply); err != nil {
			return err
		}
		return nil
	}
	rkey, nodeFullName, err := r.SearchRemote(rkey)
	if err != nil {
		return err
	}
	//log.Printf("roada.Call method:%s nodeFullName:%s selfNodeFullName:%s\n", route, nodeFullName, r.NodeFullName)
	if nodeFullName == r.NodeFullName {
		//本地key,直接通过router调用
		err = r.router.LocalCall(rkey, method, args, reply)
		if err != nil {
			return err
		}
	} else {
		//远程key
		err = r.remote.Call(nodeFullName, rkey, method, args, reply)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Road) Post(rkey string, method string, args interface{}) error {
	rkey, err := r.SearchLocal(rkey)
	if err == nil {
		//本地key,直接通过router调用
		if err := r.router.LocalPost(rkey, method, args); err != nil {
			return err
		}
		return nil
	}
	rkey, nodeFullName, err := r.SearchRemote(rkey)
	if err != nil {
		return err
	}
	//log.Printf("roada.Call method:%s nodeFullName:%s selfNodeFullName:%s\n", route, nodeFullName, self.NodeFullName)
	if nodeFullName == r.NodeFullName {
		//本地key,直接通过router调用
		err = r.router.LocalPost(rkey, method, args)
		if err != nil {
			return err
		}
	} else {
		//远程key
		/*err = r.remote.Call(nodeFullName, route, args, reply)
		if err != nil {
			return err
		}*/
	}
	return nil
}

func (r *Road) Shutdown() {
	r.destory()
}

func (r *Road) destory() error {
	log.Println("[roada] shutdown")
	if len(r.NodeFullName) == 0 {
		return nil
	}
	if r.NodeName == CoordNodeName {
		if err := r.coord.Save(); err != nil {
			log.Printf("[roada] coord save failed, error=%s\n", err.Error())
		}
	} else {
		if _, err := r.coord.NodeDel(); err != nil {
			log.Printf("[roada] coord NodeDel failed,, error=%s\n", err.Error())
		}
	}
	return nil
}

func GroupAdd(rkey string, member string) error {
	return roada.GroupAdd(rkey, member)
}

func GroupDel(rkey string, member string) error {
	return roada.GroupDel(rkey, member)
}

func Set(rkey string) error {
	return roada.Set(rkey)
}

func Del(rkey string) error {
	return roada.Del(rkey)
}

func Get(rkey string) (string, error) {
	return roada.Get(rkey)
}

func LocalSet(rkey string) error {
	return roada.LocalSet(rkey)
}

func LocalDel(rkey string) error {
	return roada.LocalDel(rkey)
}

func LocalGroupAdd(rkey string, member string) error {
	return roada.LocalGroupAdd(rkey, member)
}

func Register(svr interface{}) (*Service, error) {
	return roada.Register(svr)
}

func Handle(pattern string, h HandlerInterface) error {
	return roada.Handle(pattern, h)
}

func Master(localAddr string, restore bool) error {
	return roada.Master(localAddr, restore)
}

func Slave(nodeName string, localAddr string, coordAddr string) error {
	return roada.Slave(nodeName, localAddr, coordAddr)
}

func Call(rkey string, method string, args interface{}, reply interface{}) error {
	return roada.Call(rkey, method, args, reply)
}

func Post(rkey string, method string, args interface{}) error {
	return roada.Post(rkey, method, args)
}

/*func Run() {
	roada.Run()
}*/

func Default() *Road {
	return roada
}
