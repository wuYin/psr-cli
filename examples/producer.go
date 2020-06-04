package main

import (
	"github.com/k0kubun/pp"
	"psr-cli/core"
)

func main() {
	m := core.NewProducer("127.0.0.1:6650", "persistent://psr/default/topic-01")
	msgId, err := m.Send(&core.Message{
		Payload: []byte("A"),
	})
	if err != nil {
		panic(err)
	}
	pp.Println("msgId: ", msgId)
}
