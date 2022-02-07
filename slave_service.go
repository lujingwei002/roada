package roada

type SlaveService struct {
	road    *Road
	service *Service
}

func newSlaveService(road *Road) error {
	var self = &SlaveService{
		road: road,
	}
	roada.Handle(CoordSlaveServiceName, self)
	service, err := road.Register(self)
	if err != nil {
		return err
	}
	self.service = service
	return nil
}

func (self *SlaveService) ServeRPC(req *Request) {

}

func (self *SlaveService) Sync(req *Request, args *int) error {
	return nil
}
