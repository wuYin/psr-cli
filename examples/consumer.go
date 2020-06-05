package main

import (
	"fmt"
	"psr-cli/psr"
)

func main() {
	c := psr.NewConsumer("127.0.0.1:6650", "persistent://psr/default/topic-01", "sub-new")
	for i := 0; i < 10; i++ {
		msg, err := c.Receive()
		if err != nil {
			panic(err)
		}
		fmt.Println(msg.String())
	}
}
