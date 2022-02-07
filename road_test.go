package roada

import (
	"fmt"
	"testing"
)

type GameService struct {
}

func (self *GameService) RecvRpc(args interface{}, reply interface{}) error {
	return nil
}

func TestServices(t *testing.T) {
	t.Log("asfsf")
	Register("game", &GameService{})
	var replyType int
	t.Log("asfsf")
	Call("game", "Test", 1, &replyType)
	fmt.Printf("%+v", replyType)
}

func TestCoord(t *testing.T) {
	coord.Register("hello", ":9091")
}
