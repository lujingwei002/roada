package roada

import (
	"fmt"
	"log"
	"strings"
)

type Router struct {
	road        *Road
	handlerDict map[string]HandlerInterface
}

func newRouter(road *Road) *Router {
	var self = &Router{
		road:        road,
		handlerDict: make(map[string]HandlerInterface),
	}
	self.road.server.Handle(self)
	return self
}

func (self *Router) Handle(pattern string, h HandlerInterface) error {
	self.handlerDict[pattern] = h
	return nil
}

func (self *Router) DelHandle(pattern string, h HandlerInterface) error {
	if last, ok := self.handlerDict[pattern]; ok && last == h {
		delete(self.handlerDict, pattern)
	} else {
		return fmt.Errorf("[router] DelHandle failed, pattern=%s", pattern)
	}
	return nil
}

func (self *Router) Register(svr interface{}) (*Service, error) {
	service := newService(self.road, svr)
	if err := service.extractHandler(); err != nil {
		return nil, err
	}
	return service, nil
}

func (self *Router) LocalCall(rkey string, method string, arg interface{}, reply interface{}) error {
	route := fmt.Sprintf("%s.%s", rkey, method)
	handler, found := self.handlerDict[rkey]
	if !found {
		log.Println(fmt.Sprintf("[router] handler not found, route:%s", route))
		return fmt.Errorf("[router] handler not found, route:%s", route)
	}
	r := &Request{
		Key:    rkey,
		Method: method,
		Route:  route,
		Arg:    arg,
		Reply:  reply,
		Type:   rRequestTypeLocalCall,
	}
	self.safeRPC(handler, r)
	return r.err
}

func (self *Router) LocalPost(rkey string, method string, arg interface{}) error {
	route := fmt.Sprintf("%s.%s", rkey, method)
	handler, found := self.handlerDict[rkey]
	if !found {
		log.Println(fmt.Sprintf("[router] handler not found, route:%s", route))
		return fmt.Errorf("[router] handler not found, route:%s", route)
	}
	r := &Request{
		Key:    rkey,
		Method: method,
		Route:  route,
		Arg:    arg,
		Type:   rRequestTypeLocalPost,
	}
	self.safeRPC(handler, r)
	return r.err
}

func (self *Router) ServeRPC(r *Request) {
	var route = r.Route
	index := strings.LastIndex(route, ".")
	if index < 0 {
		log.Println(fmt.Sprintf("[router] invalid route, route:%s", route))
		r.Error(fmt.Errorf("[router] invalid route, route:%s", route))
		return
	}
	rkey := route[:index]
	handler, found := self.handlerDict[rkey]
	if !found {
		log.Println(fmt.Sprintf("[router] handler not found, route:%s", route))
		r.Error(fmt.Errorf("[router] handler not found, route:%s", route))
	}
	self.safeRPC(handler, r)
}

func (self *Router) safeRPC(handler HandlerInterface, r *Request) error {
	defer func() {
		if err := recover(); err != nil {
			//log.Println(fmt.Sprintf("Handle message panic: %+v\n%s", err, debug.Stack()))
			r.Error(fmt.Errorf("[router] rpc panic, route=%s, error=%+v", r.Route, err))
		}
	}()
	handler.ServeRPC(r)
	return nil
}
