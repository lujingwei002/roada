package roada

type NodeStub struct {
	road *Road
}

func newNodeStub(road *Road) *NodeStub {
	nodeStub := &NodeStub{
		road: road,
	}
	return nodeStub
}

func (node *NodeStub) Test(rkey string) {
	var args int
	var reply int
	node.road.Call(rkey, "Test", args, reply)
}
