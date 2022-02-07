package roada

type HandlerInterface interface {
	ServeRPC(r *Request)
}
