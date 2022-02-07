package roada

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
)

type ServerError string

func (e ServerError) Error() string {
	return string(e)
}

var rpcErrShutdown = errors.New("connection is shut down")

type rpcCall struct {
	Route string
	Args  interface{}
	Reply interface{}
	Error error
	Done  chan *rpcCall
}

type rpcClient struct {
	rwc        io.ReadWriteCloser
	decoder    *gob.Decoder
	encoder    *gob.Encoder
	encoderBuf *bufio.Writer

	reqMutex sync.Mutex
	request  rpcRequest
	mutex    sync.Mutex
	seq      uint64
	pending  map[uint64]*rpcCall
	closing  bool
	shutdown bool
}

func (self *rpcClient) writeRequest(req *rpcRequest, body interface{}) error {
	var network bytes.Buffer
	encoder := gob.NewEncoder(&network)
	if err := encoder.Encode(body); err != nil {
		return err
	}
	req.Payload = network.Bytes()
	if err := self.encoder.Encode(req); err != nil {
		return err
	}
	return self.encoderBuf.Flush()
}

func (self *rpcClient) readResponse(r *rpcResponse) error {
	return self.decoder.Decode(r)
}

func (self *rpcClient) readResponseBody(r *rpcResponse, v interface{}) error {
	decoder := gob.NewDecoder(bytes.NewReader(r.Payload))
	return decoder.Decode(v)
}

func (self *rpcClient) send(call *rpcCall) {
	self.reqMutex.Lock()
	defer self.reqMutex.Unlock()
	self.mutex.Lock()
	if self.shutdown || self.closing {
		self.mutex.Unlock()
		call.Error = io.EOF
		call.done()
		return
	}
	seq := self.seq
	self.seq++
	self.pending[seq] = call
	self.mutex.Unlock()
	self.request.Seq = seq
	self.request.Route = call.Route
	err := self.writeRequest(&self.request, call.Args)
	if err != nil {
		self.mutex.Lock()
		call = self.pending[seq]
		delete(self.pending, seq)
		self.mutex.Unlock()
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

func (self *rpcClient) input() {
	var err error
	for err == nil {
		resp := rpcResponse{}
		err = self.readResponse(&resp)
		if err != nil {
			break
		}
		seq := resp.Seq
		self.mutex.Lock()
		call := self.pending[seq]
		delete(self.pending, seq)
		self.mutex.Unlock()
		switch {
		case call == nil:
			err = self.readResponseBody(&resp, nil)
			if err != nil {
				err = errors.New("[rpcclient] reading error body: " + err.Error())
			}
		case resp.Error != "":
			call.Error = ServerError(resp.Error)
			err = self.readResponseBody(&resp, nil)
			if err != nil {
				err = errors.New("[rpcclient] reading error body: " + err.Error())
			}
			call.done()
		default:
			err = self.readResponseBody(&resp, call.Reply)
			if err != nil {
				call.Error = errors.New("[rpcclient] reading body " + err.Error())
			}
			call.done()
		}
	}
	self.reqMutex.Lock()
	self.mutex.Lock()
	self.shutdown = true
	closing := self.closing
	if err == io.EOF {
		if closing {
			err = rpcErrShutdown
		} else {
			err = io.ErrUnexpectedEOF
		}
	}
	for _, call := range self.pending {
		call.Error = err
		call.done()
	}
	self.mutex.Unlock()
	self.reqMutex.Unlock()
	log.Printf("[rpcclient] input failed, error=%s\n", err.Error())
}

func (call *rpcCall) done() {
	select {
	case call.Done <- call:
		// ok
	default:
		if debugLog {
			log.Println("[rpcclient] discarding Call reply due to insufficient Done chan capacity")
		}
	}
}

func (self *rpcClient) isClose() bool {
	return self.shutdown || self.closing
}

func (self *rpcClient) Close() error {
	self.mutex.Lock()
	if self.closing {
		self.mutex.Unlock()
		return rpcErrShutdown
	}
	self.closing = true
	self.mutex.Unlock()
	return self.rwc.Close()
}

func (self *rpcClient) Go(route string, args interface{}, reply interface{}, done chan *rpcCall) *rpcCall {
	call := new(rpcCall)
	call.Route = route
	call.Args = args
	call.Reply = reply
	if done == nil {
		done = make(chan *rpcCall, 10) // buffered.
	} else {
		if cap(done) == 0 {
			log.Panic("rpc: done channel is unbuffered")
		}
	}
	call.Done = done
	self.send(call)
	return call
}

func (self *rpcClient) Call(route string, args interface{}, reply interface{}) error {
	call := <-self.Go(route, args, reply, make(chan *rpcCall, 1)).Done
	return call.Error
}

func rpcNewClient(conn io.ReadWriteCloser) *rpcClient {
	encoderBuf := bufio.NewWriter(conn)
	self := &rpcClient{
		rwc:        conn,
		decoder:    gob.NewDecoder(conn),
		encoder:    gob.NewEncoder(encoderBuf),
		encoderBuf: encoderBuf,
		pending:    make(map[uint64]*rpcCall),
	}
	go self.input()
	return self
}

func rpcDialHTTP(network, address string) (*rpcClient, error) {
	return rpcDialHTTPPath(network, address, defaultRPCPath)
}

func rpcDialHTTPPath(network, address, path string) (*rpcClient, error) {
	var err error
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	io.WriteString(conn, "CONNECT "+path+" HTTP/1.0\n\n")

	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		return rpcNewClient(conn), nil
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	conn.Close()
	return nil, &net.OpError{
		Op:   "dial-http",
		Net:  network + " " + address,
		Addr: nil,
		Err:  err,
	}
}

func rpcDial(network, address string) (*rpcClient, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return rpcNewClient(conn), nil
}
