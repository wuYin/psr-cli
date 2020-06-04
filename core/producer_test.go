package core

import (
	"github.com/k0kubun/pp"
	"testing"
)

func TestProducerManager(t *testing.T) {
	m := NewProducer("127.0.0.1:6650", "persistent://psr/default/topic-01")
	msgId, err := m.Send(&Message{
		Payload: []byte("A"),
	})
	if err != nil {
		t.Fatal(err)
	}
	pp.Println("msgId: ", msgId)
}
