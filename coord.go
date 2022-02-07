package roada

import (
	"log"
	"time"
)

type CoordClient struct {
	road *Road
}

func newCoordClient(road *Road) *CoordClient {
	var self = &CoordClient{
		road: road,
	}
	return self
}

func (coord *CoordClient) NodeAdd(nodeName string, nodeAddr string) (*Coord_NodeAddReply, error) {
	var reply = &Coord_NodeAddReply{}
	var args = &Coord_NodeAddArgs{
		NodeName: nodeName,
		NodeAddr: nodeAddr,
	}
	r := coord.road
	err := r.Call("coord", "NodeAdd", args, reply)
	if err != nil {
		log.Printf("[coord] coord.NodeAdd failed, nodeName=%s, nodeAddr=%s, error=%s\n", nodeName, nodeAddr, err.Error())
		return nil, err
	}
	if err := r.slaveRegistry.Clone(reply.NodeArr, reply.DeadNodeArr, reply.Version); err != nil {
		return nil, err
	}
	//定时心跳
	go coord.forkHeartbeat()
	//log.Printf("[coord] coord.NodeAdd reply, nodeName=%s, nodeAddr=%s, reply=%+v\n", nodeName, nodeAddr, reply)
	return reply, nil
}

func (coord *CoordClient) NodeDel() (*Coord_NodeDelReply, error) {
	var reply = &Coord_NodeDelReply{}
	var args = &Coord_NodeDelArgs{
		NodeFullName: coord.road.NodeFullName,
	}
	err := coord.road.Call("coord", "NodeDel", args, reply)
	if err != nil {
		log.Printf("[coord] coord.NodeDel, error=%s\n", err.Error())
		return nil, err
	}
	return reply, nil
}

func (coord *CoordClient) Set(rkey string) error {
	var reply = &Coord_SetReply{}
	var args = &Coord_SetArgs{
		Version:      coord.road.slaveRegistry.version,
		NodeFullName: coord.road.NodeFullName,
		Rkey:         rkey,
	}
	err := coord.road.Call("coord", "Set", args, reply)
	if err != nil {
		log.Printf("[coord] coord.Set failed, nodeFullName=%s, rkey=%s, error=%s\n", coord.road.NodeFullName, rkey, err.Error())
		return err
	}
	coord.road.slaveRegistry.Set(rkey, coord.road.NodeFullName)
	//log.Printf("[coord] coord.Set reply, nodeFullName=%s, rkey=%s, reply=%+v\n", cc.road.NodeFullName, rkey, reply)
	return nil
}

func (coord *CoordClient) Del(rkey string) error {
	var reply = &Coord_DelReply{}
	var args = &Coord_DelArgs{
		NodeFullName: coord.road.NodeFullName,
		Rkey:         rkey,
	}
	err := coord.road.Call("coord", "Del", args, reply)
	if err != nil {
		log.Println("coord.Del error:", err)
		return err
	}
	coord.road.slaveRegistry.Del(rkey, coord.road.NodeFullName)
	return nil
}

func (coord *CoordClient) Get(rkey string) (string, error) {
	var reply = &Coord_GetReply{}
	var args = &Coord_GetArgs{
		NodeFullName: coord.road.NodeFullName,
		Rkey:         rkey,
	}
	err := coord.road.Call("coord", "Get", args, reply)
	if err != nil {
		log.Println("coord", "Get error:", err)
		return EmptyString, err
	}
	return reply.NodeFullName, nil
}

func (coord *CoordClient) GroupAdd(rkey string, member string) error {
	var reply = &Coord_GroupAddReply{}
	var args = &Coord_GroupAddArgs{
		NodeFullName: coord.road.NodeFullName,
		Key:          rkey,
		Member:       member,
	}
	err := coord.road.Call("coord", "GroupAdd", args, reply)
	if err != nil {
		log.Println("[coord] coord.GroupAdd failed, nodeFullName=%s, rkey=%s, member=%s, error=%s\n", coord.road.NodeFullName, rkey, member, err.Error())
		return err
	}
	coord.road.slaveRegistry.GroupAdd(rkey, member, coord.road.NodeFullName)
	//log.Printf("[coord] coord.GroupAdd reply, nodeFullName=%s, rkey=%s, member=%s, reply=%+v\n", cc.road.NodeFullName, rkey, member, reply)
	return nil
}

func (coord *CoordClient) GroupDel(rkey string, member string) error {
	var reply = &Coord_GroupDelReply{}
	var args = &Coord_GroupDelArgs{
		NodeFullName: coord.road.NodeFullName,
		Key:          rkey,
		Member:       member,
	}
	err := coord.road.Call("coord", "GroupDel", args, reply)
	if err != nil {
		log.Printf("[coord] coord.GroupDel, nodeFullName=%s, rkey=%s, member=%s, error=%s\n",
			coord.road.NodeFullName, rkey, member, err.Error())
		return err
	}
	coord.road.slaveRegistry.GroupDel(rkey, member, coord.road.NodeFullName)
	//log.Printf("[coord] coord.GroupDel, nodeFullName=%s, rkey=%s, member=%s, reply=%+v\n", cc.road.NodeFullName, rkey, member, reply)
	return nil
}

func (coord *CoordClient) Save() error {
	var reply int
	var args int
	err := coord.road.Call("coord", "Save", &args, &reply)
	if err != nil {
		log.Printf("coord.Save failed, error=%s\n", err.Error())
		return err
	}
	return nil
}

func (coord *CoordClient) Heartbeat() (*Coord_HeartbeatReply, error) {
	var reply = &Coord_HeartbeatReply{}
	var args = &Coord_HeartbeatArgs{
		NodeFullName: coord.road.NodeFullName,
		Version:      coord.road.slaveRegistry.version,
	}
	err := coord.road.Call("coord", "Heartbeat", args, reply)
	if err != nil {
		//log.Println("coord.Heartbeat error:", err)
		return nil, err
	}
	log.Printf("[coord] heartbeat, version=%d\n", args.Version)
	if err := coord.road.slaveRegistry.Merge(reply.RecordArr, reply.Version); err != nil {
		return nil, err
	}
	return reply, nil
}

func (coord *CoordClient) forkHeartbeat() error {
	for {
		time.Sleep(30 * time.Second)
		_, err := coord.Heartbeat()
		if err != nil {
			log.Printf("[coord] Heartbeat failed, error=%s\n", err.Error())
		}
		//log.Println("coord.Heartbeat reply", reply)
	}
	return nil
}
