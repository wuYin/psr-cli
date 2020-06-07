package main

import (
	"fmt"
	"os"
	"psr-cli/psr"
	"strconv"
	"time"
)

var (
	l    = 0
	r    = 10
	conf = psr.ProducerConf{
		Broker:   "127.0.0.1:6650",
		Topic:    "persistent://psr/default/topic-01",
		MaxBatch: 2,
	}
)

func main() {
	if len(os.Args) == 3 {
		l, _ = strconv.Atoi(os.Args[1])
		r, _ = strconv.Atoi(os.Args[2])
	}
	asyncSend()
}

func asyncSend() {
	p := psr.NewProducer(&conf)
	defer p.Close()
	for i := l; i < r; i++ {
		p.AsyncSend(psr.NewMsg([]byte(fmt.Sprintf("MESSAGE_%d", i))))
	}
	time.Sleep(500 * time.Millisecond)
}

func syncSend() {
	p := psr.NewProducer(&conf)
	defer p.Close()
	for i := l; i < r; i++ {
		msgId, err := p.Send(psr.NewMsg([]byte(fmt.Sprintf("MESSAGE_%d", i))))
		if err != nil {
			panic(err)
		}
		fmt.Println("produced: ", msgId)
	}
}
