package main

import (
	"fmt"
	"psr-cli/psr"
)

func main() {
	p := psr.NewProducer("127.0.0.1:6650", "persistent://psr/default/topic-01")
	defer p.Close()
	for i := 0; i < 10; i++ {
		msgId, err := p.Send(psr.NewMsg([]byte("A")))
		if err != nil {
			panic(err)
		}
		fmt.Println("msgId: ", msgId)
	}
}
