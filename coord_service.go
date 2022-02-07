package roada

import (
	"fmt"
	"time"

	sched "github.com/roada-go/util/scheduler"
)

type CoordService struct {
	road      *Road
	chRoad    chan *Request
	service   *Service
	registry  *Registry
	scheduler *sched.Scheduler
}

type Coord_NodeAddArgs struct {
	NodeName string
	NodeAddr string
}

type Coord_NodeAddReply struct {
	NodeFullName string
	Version      int64
	NodeArr      []*rNode
	DeadNodeArr  []*rNode
}

type Coord_NodeDelArgs struct {
	NodeFullName string
}

type Coord_NodeDelReply struct {
}

type Coord_HeartbeatArgs struct {
	NodeFullName string
	Version      int64
}

type Coord_HeartbeatReply struct {
	RecordArr []string
	Version   int64
}

type Coord_GroupAddArgs struct {
	NodeFullName string
	Key          string
	Member       string
}

type Coord_GroupAddReply struct {
}

type Coord_GroupDelArgs struct {
	NodeFullName string
	Key          string
	Member       string
}

type Coord_GroupDelReply struct {
}

type Coord_SetArgs struct {
	Version      int64
	NodeFullName string
	Rkey         string
}
type Coord_SetReply struct {
	RecordArr []string
	Version   int64
}

type Coord_DelArgs struct {
	NodeFullName string
	Rkey         string
}
type Coord_DelReply struct {
}

type Coord_GetArgs struct {
	NodeFullName string
	Rkey         string
}

type Coord_GetReply struct {
	NodeFullName string
}

func newCoordService(road *Road, restore bool) (string, error) {
	var coord = &CoordService{
		road:      road,
		chRoad:    make(chan *Request, 1),
		scheduler: sched.NewScheduler(),
		registry:  newRegistry(road),
	}
	//restore = false
	if restore {
		if err := coord.registry.Restore(); err == ErrRestoreDataDirNotFound {
			if err := coord.registry.Create(); err != nil {
				return "", err
			}
		} else if err != nil {
			return "", err
		}
	} else {
		if err := coord.registry.Create(); err != nil {
			return "", err
		}
	}
	coord.registry.Trace()
	road.Handle(CoordServiceName, coord)
	service, err := road.Register(coord)
	if err != nil {
		return "", err
	}
	coord.service = service
	nodeFullName := fmt.Sprintf("%s#%d", CoordNodeName, coord.registry.nameVer)
	go coord.loop()
	return nodeFullName, nil
}

func (coord *CoordService) loop() {
	tick := time.NewTicker(1 * time.Second)
	defer func() {
		tick.Stop()
		coord.scheduler.Close()
	}()
	coord.scheduler.NewTimer(60*time.Second, coord.registryGc)
	coord.scheduler.NewTimer(30*time.Second, coord.checkHeartbeat)
	coord.scheduler.NewTimer(60*time.Second, coord.registrySave)
	coord.scheduler.NewTimer(60*time.Second, coord.registryTrace)
	for {
		select {
		case r := <-coord.chRoad:
			{
				coord.service.ServeRPC(coord, r)
			}
		case <-tick.C:
			{
				coord.scheduler.Sched()
			}
		case task := <-coord.scheduler.T:
			{
				coord.scheduler.Invoke(task)
			}
		}
	}
}

func (coord *CoordService) ServeRPC(r *Request) {
	coord.chRoad <- r
	r.Wait(5)
}

func (coord *CoordService) checkHeartbeat() {
	coord.registry.CheckHeartbeat()
}

func (coord *CoordService) registryGc() {
	coord.registry.GC()
	coord.registry.Trace()

}

func (coord *CoordService) registryTrace() {
	coord.registry.Trace()
}

func (coord *CoordService) registrySave() {
	coord.registry.Save()
}

func (coord *CoordService) Save(r *Request, args *int, reply *int) error {
	coord.registry.Save()
	return nil
}

func (coord *CoordService) NodeAdd(r *Request, args *Coord_NodeAddArgs, reply *Coord_NodeAddReply) error {
	var nodeName = args.NodeName
	var nodeAddr = args.NodeAddr
	node, err := coord.registry.NodeAdd(nodeName, nodeAddr)
	if err != nil {
		return err
	}
	reply.NodeFullName = node.NodeFullName
	nodeArr := make([]*rNode, 0)
	for _, node := range coord.registry.nodeDict {
		nodeArr = append(nodeArr, node)
	}
	reply.NodeArr = nodeArr
	nodeArr = make([]*rNode, 0)
	for _, node := range coord.registry.deadNodeDict {
		nodeArr = append(nodeArr, node)
	}
	reply.DeadNodeArr = nodeArr
	reply.Version = coord.registry.version
	coord.registry.Trace()
	return nil
}

func (coord *CoordService) NodeDel(r *Request, args *Coord_NodeDelArgs, reply *Coord_NodeDelReply) error {
	var nodeFullName = args.NodeFullName
	err := coord.registry.NodeDel(nodeFullName)
	if err != nil {
		return err
	}
	return nil
}

func (coord *CoordService) Heartbeat(r *Request, args *Coord_HeartbeatArgs, reply *Coord_HeartbeatReply) error {
	var nodeFullName = args.NodeFullName
	err := coord.registry.Heartbeat(nodeFullName, args.Version)
	if err != nil {
		return err
	}
	reply.RecordArr, reply.Version = coord.registry.Pull(args.Version)
	return nil
}

func (coord *CoordService) Set(r *Request, args *Coord_SetArgs, reply *Coord_SetReply) error {
	var nodeFullName = args.NodeFullName
	var rkey = args.Rkey
	err := coord.registry.Set(rkey, nodeFullName)
	if err != nil {
		return err
	}
	return nil
}

func (coord *CoordService) Del(r *Request, args *Coord_DelArgs, reply *Coord_DelReply) error {
	var nodeFullName = args.NodeFullName
	var rkey = args.Rkey
	err := coord.registry.Del(rkey, nodeFullName)
	if err != nil {
		return err
	}
	return nil
}

func (coord *CoordService) GroupAdd(r *Request, args *Coord_GroupAddArgs, reply *Coord_GroupAddReply) error {
	var nodeFullName = args.NodeFullName
	var rkey = args.Key
	var member = args.Member
	err := coord.registry.GroupAdd(rkey, member, nodeFullName)
	if err != nil {
		return err
	}
	return nil
}

func (coord *CoordService) GroupDel(r *Request, args *Coord_GroupDelArgs, reply *Coord_GroupDelReply) error {
	var nodeFullName = args.NodeFullName
	var rkey = args.Key
	var member = args.Member
	err := coord.registry.GroupDel(rkey, member, nodeFullName)
	if err != nil {
		return err
	}
	return nil
}
