package psr

import (
	"github.com/stretchr/testify/assert"
	"log"
	"net"
	"strings"
	"testing"
)

const (
	NUMS = "0123456789"
)

func TestRead(t *testing.T) {
	addr := "127.0.0.1:10086"
	go serve(addr)
	cli, err := newClient(addr) // comment out handshake
	if err != nil {
		panic(err)
	}

	type result struct {
		n int
		s string
	}
	for _, res := range []result{
		{n: 3, s: "012"},
		{n: 1, s: "3"},
		{n: 6, s: "456789"},
		{n: 0, s: ""},
		{n: 50, s: strings.Repeat(NUMS, 5)},
		{n: 10000, s: strings.Repeat(NUMS, 1000)},
	} {
		buf, err := cli.read(res.n)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, []byte(res.s), buf)
	}
}

func serve(addr string) {
	l, err := net.Listen("tcp4", addr)
	if err != nil {
		panic(err)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go func(c net.Conn) {
			sendAlphas(c)
		}(conn)
	}
}

func sendAlphas(conn net.Conn) {
	for _, v := range strings.Repeat(NUMS, 20000) {
		n, err := conn.Write([]byte{byte(v)})
		if err != nil || n != 1 {
			log.Println("conn write failed:", err)
		}
	}
}
