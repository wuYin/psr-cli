package main

import (
	"fmt"
	"math"
	"os"
	"psr-cli/psr"
	"strconv"
)

var (
	n       = math.MaxInt64
	subName = "sub-"
)

func main() {
	if len(os.Args) == 3 {
		subName += os.Args[1]
		n, _ = strconv.Atoi(os.Args[2])
	}

	conf := psr.ConsumerConf{
		Broker:    "127.0.0.1:6650",
		Topic:     "persistent://psr/default/topic-01",
		SubName:   subName,
		MaxPermit: 4,
	}
	c := psr.NewConsumer(&conf)
	for i := 0; i < n; i++ {
		msg, err := c.Receive()
		if err != nil {
			panic(err)
		}
		fmt.Println("consumed", msg.String())
		c.Ack(msg.GetMessageId())
	}
}
