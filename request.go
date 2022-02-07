package roada

import (
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

var errRoadaCallTimeout = errors.New("roada call timeout")

const (
	rRequestTypeCall = 1
	rRequestTypePost = 2

	rRequestTypeLocal     = 10
	rRequestTypeLocalCall = 11
	rRequestTypeLocalPost = 12
)

type Request struct {
	Key     string
	Method  string
	Route   string
	Payload []byte
	Type    int8
	Arg     interface{}
	Reply   interface{}

	//road    *Road
	err    error
	done   int32
	agent  *rpcAgent
	seq    uint64
	chDone chan *Request
	//	rcvr   reflect.Value
	//method reflect.Method
	//	argv   reflect.Value
	//	replyv reflect.Value
}

func (r *Request) Response() {
	if !atomic.CompareAndSwapInt32(&r.done, 0, 1) {
		return
	}
	if r.chDone != nil {
		close(r.chDone)
		r.chDone = nil
	}
	if r.Type == rRequestTypeCall || r.Type == rRequestTypePost {
		r.agent.sendResponse(r.Route, r.seq, r.Reply, r.err)
	}
}

func (r *Request) Error(err error) {
	r.err = err
	r.Response()
}

func (r *Request) Wait(timeout int) {
	if r.chDone != nil {
		return
	}
	r.chDone = make(chan *Request, 1)
	select {
	case <-r.chDone:
		{

		}
	case <-time.After(time.Duration(timeout) * time.Second):
		{
			log.Printf("[roada] request timeout, route=%s", r.Route)
			r.Error(fmt.Errorf("[roada] request timeout, route=%s", r.Route))
		}
	}
}
