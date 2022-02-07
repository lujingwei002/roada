package roada

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"go/token"
	"log"
	"reflect"
	"strings"
)

var typeOfError = reflect.TypeOf((*error)(nil)).Elem()
var typeOfRequest = reflect.TypeOf(&Request{})

type Service struct {
	road     *Road
	name     string
	receiver reflect.Value
	typ      reflect.Type
	handlers map[string]*rpcHandlerMethod
}

type rpcHandlerMethod struct {
	method    reflect.Method
	argType   reflect.Type
	replyType reflect.Type
}

func newService(road *Road, svr interface{}) *Service {
	sname := reflect.Indirect(reflect.ValueOf(svr)).Type().Name()
	self := &Service{
		name:     sname,
		road:     road,
		typ:      reflect.TypeOf(svr),
		receiver: reflect.ValueOf(svr),
	}
	return self
}

func (self *Service) extractHandler() error {
	self.handlers = self.suitableMethods(self.typ)
	/*if len(self.handlers) == 0 {
		str := ""
		handlers := self.suitableMethods(reflect.PtrTo(self.typ))
		if len(handlers) != 0 {
			str = fmt.Sprintf("type %s has no exported methods of suitable type (hint: pass a pointer to value of that type)", self.name)
		} else {
			str = fmt.Sprintf("type %s has no exported methods of suitable type", self.name)
		}
		log.Print(str)
		return errors.New(str)
	}*/
	return nil
}

func (self *Service) isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return token.IsExported(t.Name()) || t.PkgPath() == ""
}

func (self *Service) suitableMethods(typ reflect.Type) map[string]*rpcHandlerMethod {
	methods := make(map[string]*rpcHandlerMethod)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name
		if method.PkgPath != "" {
			continue
		}
		if mtype.NumIn() == 4 {
			if t1 := mtype.In(1); t1.Kind() != reflect.Ptr || t1 != typeOfRequest {
				continue
			}
			argType := mtype.In(2)
			if !self.isExportedOrBuiltinType(argType) {
				continue
			}
			replyType := mtype.In(3)
			if replyType.Kind() != reflect.Ptr {
				continue
			}
			if !self.isExportedOrBuiltinType(replyType) {
				continue
			}
			if mtype.NumOut() != 1 {
				continue
			}
			if returnType := mtype.Out(0); returnType != typeOfError {
				continue
			}
			methods[mname] = &rpcHandlerMethod{method: method, argType: argType, replyType: replyType}
		} else if mtype.NumIn() == 3 {
			if t1 := mtype.In(1); t1.Kind() != reflect.Ptr || t1 != typeOfRequest {
				continue
			}
			argType := mtype.In(2)
			if !self.isExportedOrBuiltinType(argType) {
				continue
			}
			if mtype.NumOut() != 1 {
				continue
			}
			if returnType := mtype.Out(0); returnType != typeOfError {
				continue
			}
			methods[mname] = &rpcHandlerMethod{method: method, argType: argType}
		}
	}
	return methods
}

func (self *Service) ServeRPC(receiver interface{}, r *Request) {
	if err := self.unpack(receiver, r); err != nil {
		r.Error(err)
		return
	}
	if r.Type == rRequestTypeLocalCall || r.Type == rRequestTypeCall {
		self.serveCall(receiver, r)
	} else if r.Type == rRequestTypeLocalPost || r.Type == rRequestTypePost {
		self.servePost(receiver, r)
	} else {
		return
	}
}

func (self *Service) unpack(receiver interface{}, r *Request) error {
	if r.Type != rRequestTypeCall && r.Type != rRequestTypePost {
		return nil
	}
	route := r.Route
	index := strings.LastIndex(route, ".")
	if index < 0 {
		return fmt.Errorf("[service] invalid route, route:%s", route)
	}
	handlerName := route[index+1:]
	handler, found := self.handlers[handlerName]
	if !found {
		return fmt.Errorf("[service] handler not found, route:%s", route)
	}
	var argv reflect.Value
	var replyv reflect.Value
	argIsValue := false
	if handler.argType.Kind() == reflect.Ptr {
		argv = reflect.New(handler.argType.Elem())
	} else {
		argv = reflect.New(handler.argType)
		argIsValue = true
	}
	decoder := gob.NewDecoder(bytes.NewReader(r.Payload))
	if err := decoder.Decode(argv.Interface()); err != nil {
		return err
	}
	if argIsValue {
		argv = argv.Elem()
	}
	replyv = reflect.New(handler.replyType.Elem())
	switch handler.replyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(handler.replyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(handler.replyType.Elem(), 0, 0))
	}
	r.Arg = argv.Interface()
	r.Reply = replyv.Interface()
	return nil
}

func (self *Service) serveCall(receiver interface{}, r *Request) {
	route := r.Route
	index := strings.LastIndex(route, ".")
	if index < 0 {
		log.Println(fmt.Sprintf("[service] invalid route, route:%s", route))
		r.Error(fmt.Errorf("[service] invalid route, route:%s", route))
		return
	}
	handlerName := route[index+1:]
	handler, found := self.handlers[handlerName]
	if !found {
		log.Println(fmt.Sprintf("[service] handler not found, route:%s", route))
		r.Error(fmt.Errorf("[service] handler not found, route:%s", route))
		return
	}
	var argv reflect.Value = reflect.ValueOf(r.Arg)
	var replyv reflect.Value = reflect.ValueOf(r.Reply)
	function := handler.method.Func
	var returnValues []reflect.Value = function.Call([]reflect.Value{reflect.ValueOf(receiver), reflect.ValueOf(r), argv, replyv})
	errInterface := returnValues[0].Interface()
	if errInterface != nil {
		r.Error(errors.New(errInterface.(error).Error()))
		return
	}
	r.Response()
}

func (self *Service) servePost(receiver interface{}, r *Request) {
	route := r.Route
	index := strings.LastIndex(route, ".")
	if index < 0 {
		log.Println(fmt.Sprintf("[service] invalid route, route:%s", route))
		r.Error(fmt.Errorf("[service] invalid route, route:%s", route))
		return
	}
	handlerName := route[index+1:]
	handler, found := self.handlers[handlerName]
	if !found {
		log.Println(fmt.Sprintf("[service] handler not found, route:%s", route))
		r.Error(fmt.Errorf("[service] handler not found, route:%s", route))
		return
	}
	var argv reflect.Value = reflect.ValueOf(r.Arg)
	function := handler.method.Func
	var returnValues []reflect.Value = function.Call([]reflect.Value{reflect.ValueOf(receiver), reflect.ValueOf(r), argv})
	errInterface := returnValues[0].Interface()
	if errInterface != nil {
		r.Error(errors.New(errInterface.(error).Error()))
		return
	}
	r.Response()
}
