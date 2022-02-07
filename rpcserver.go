package roada

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
)

const (
	defaultRPCPath = "/_goRPC_"
)

var invalidRequest = struct{}{}

type rpcHandler interface {
	ServeRPC(req *Request)
}

type rpcRequest struct {
	Route   string
	Seq     uint64
	Payload []byte
	next    *rpcRequest
}

type rpcResponse struct {
	Route   string
	Seq     uint64
	Payload []byte
	Error   string
	reply   interface{}
	next    *rpcResponse
}

type rpcServer struct {
	road     *Road
	handler  rpcHandler
	reqLock  sync.Mutex
	respLock sync.Mutex
	freeReq  *rpcRequest
	freeResp *rpcResponse
}

// NewServer returns a new Server.
func rpcNewServer(road *Road) *rpcServer {
	return &rpcServer{
		road: road,
	}
}

type rpcAgent struct {
	server     *rpcServer
	rwc        io.ReadWriteCloser
	decoder    *gob.Decoder
	encoder    *gob.Encoder
	encoderBuf *bufio.Writer
	chSend     chan *rpcResponse
	closed     bool
}

func (self *rpcAgent) writeResponse(resp *rpcResponse) error {
	defer self.server.freeResponse(resp)
	buffer := bytes.NewBuffer(resp.Payload)
	encoder := gob.NewEncoder(buffer)
	if err := encoder.Encode(resp.reply); err != nil {
		return err
	}
	//log.Println("ffffffffffffffff", resp.Route)
	resp.Payload = buffer.Bytes()
	if err := self.encoder.Encode(resp); err != nil {
		if self.encoderBuf.Flush() == nil {
			log.Printf("[rpcserver] gob error encoding response failed, error=%s", err.Error())
			self.Close()
		}
		return err
	}
	return self.encoderBuf.Flush()
}

func (self *rpcAgent) sendResponse(route string, seq uint64, reply interface{}, err error) {
	resp := self.server.getResponse()
	// Encode the response header
	resp.Route = route
	resp.Seq = seq
	if err != nil {
		resp.Error = err.Error()
		reply = invalidRequest
	}
	resp.reply = reply
	self.chSend <- resp
}

func (self *rpcAgent) readRequest(server *rpcServer) (*rpcRequest, error) {
	req := server.getRequest()
	err := self.decoder.Decode(req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func (self *rpcAgent) write() {
	for {
		select {
		case resp := <-self.chSend:
			err := self.writeResponse(resp)
			if debugLog && err != nil {
				log.Printf("[rpcserver] writing response failed, error:%s\n", err.Error())
				return
			}
		}
	}
}

func (self *rpcAgent) input() {
	go self.write()
	//sending := new(sync.Mutex)
	for {
		req, err := self.readRequest(self.server)
		if err != nil {
			break
		}
		if self.server.handler != nil {
			r := &Request{
				Route:   req.Route,
				Payload: req.Payload,
				Type:    rRequestTypeCall,

				seq:   req.Seq,
				agent: self,
				//road:  self.server.road,
			}
			go self.server.handler.ServeRPC(r)
			/*if err := self.server.handler.ServeRPC(r); err != nil {
				self.sendResponse(req.Route, req.Seq, invalidRequest, err)
			}*/
		}
		//go service.call(self, sending, wg, mtype, req, argv, replyv, codec)
	}
	self.Close()
}

func (self *rpcAgent) Close() error {
	if self.closed {
		return nil
	}
	self.closed = true
	return self.rwc.Close()
}

func (self *rpcServer) Handle(handler rpcHandler) {
	self.handler = handler
}

func (self *rpcServer) serveConn(conn io.ReadWriteCloser) {
	buf := bufio.NewWriter(conn)
	agent := &rpcAgent{
		server:     self,
		rwc:        conn,
		decoder:    gob.NewDecoder(conn),
		encoder:    gob.NewEncoder(buf),
		encoderBuf: buf,
		chSend:     make(chan *rpcResponse),
	}
	agent.input()
}

func (self *rpcServer) getRequest() *rpcRequest {
	self.reqLock.Lock()
	req := self.freeReq
	if req == nil {
		req = new(rpcRequest)
	} else {
		self.freeReq = req.next
		*req = rpcRequest{}
	}
	self.reqLock.Unlock()
	return req
}

func (self *rpcServer) freeRequest(req *rpcRequest) {
	self.reqLock.Lock()
	req.next = self.freeReq
	self.freeReq = req
	self.reqLock.Unlock()
}

func (self *rpcServer) getResponse() *rpcResponse {
	self.respLock.Lock()
	resp := self.freeResp
	if resp == nil {
		resp = new(rpcResponse)
	} else {
		self.freeResp = resp.next
		*resp = rpcResponse{}
	}
	self.respLock.Unlock()
	return resp
}

func (self *rpcServer) freeResponse(resp *rpcResponse) {
	self.respLock.Lock()
	resp.next = self.freeResp
	self.freeResp = resp
	self.respLock.Unlock()
}

func (self *rpcServer) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Print("rpc.Serve: accept:", err.Error())
			return
		}
		go self.serveConn(conn)
	}
}

var connected = "200 Connected to Go RPC"

func (self *rpcServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	//log.Println("accept")
	self.serveConn(conn)
}

func (self *rpcServer) HandleHTTP() {
	http.Handle(defaultRPCPath, self)
}
