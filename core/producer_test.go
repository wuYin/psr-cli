package core

import (
	"github.com/k0kubun/pp"
	"testing"
)

func TestProducer(t *testing.T) {
	p := NewProducer("127.0.0.1:6650")
	ps, err := p.partitions("persistent://psr/default/topic-01")
	if err != nil {
		t.Fatal(err)
	}
	pp.Println("partitions ", ps)
}
