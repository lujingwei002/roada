package roada

import "log"

type NodeService struct {
	road    *Road
	service *Service
}

func newNodeService(road *Road) error {
	if err := road.Set(road.NodeFullName); err != nil {
		return err
	}
	if err := road.GroupAdd(NodeServiceName, road.NodeFullName); err != nil {
		return err
	}
	var self = &NodeService{
		road: road,
	}
	if err := roada.Handle(NodeServiceName, self); err != nil {
		return err
	}
	service, err := road.Register(self)
	if err != nil {
		return err
	}
	self.service = service
	return nil
}

func (self *NodeService) ServeRPC(req *Request) {

}

func (self *NodeService) Test(req *Request, args *int, reply *int) error {
	log.Printf("[node] test\n")
	return nil
}
