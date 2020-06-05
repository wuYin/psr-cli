package main

import (
	"psr-cli/psr"
	"time"
)

func main() {
	c := psr.NewConsumer("127.0.0.1:6650", "persistent://psr/default/topic-01", "sub-01")
	_ = c
	time.Sleep(5000 * time.Millisecond)
}
