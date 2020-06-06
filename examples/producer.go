package main

import (
	"fmt"
	"psr-cli/psr"
	"time"
)

func main() {
	asyncSend()
}

func asyncSend() {
	p := psr.NewProducer("127.0.0.1:6650", "persistent://psr/default/topic-01")
	defer p.Close()
	// 10 -> 2*2+1
	//    -> 2*2+1 -> broker -> consumer
	for i := 0; i < 10; i++ {
		p.AsyncSend(psr.NewMsg([]byte(fmt.Sprintf("MESSAGE_%d", i))))
	}
	time.Sleep(500 * time.Millisecond)
}

func syncSend() {
	p := psr.NewProducer("127.0.0.1:6650", "persistent://psr/default/topic-01")
	defer p.Close()
	for i := 0; i < 10; i++ {
		msgId, err := p.Send(psr.NewMsg([]byte(fmt.Sprintf("MESSAGE_%d", i))))
		if err != nil {
			panic(err)
		}
		fmt.Println("msgId: ", msgId)
	}
}
