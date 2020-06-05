package main

import (
	"github.com/k0kubun/pp"
	"psr-cli/psr"
)

func main() {
	p := psr.NewProducer("127.0.0.1:6650", "persistent://psr/default/topic-01")
	defer p.Close()
	msgId, err := p.Send(psr.NewMsg([]byte("A")))
	if err != nil {
		panic(err)
	}
	pp.Println("msgId: ", msgId)
}
