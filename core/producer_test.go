package core

import (
	"testing"
)

func TestProducerManager(t *testing.T) {
	m := NewProducer("127.0.0.1:6650", "persistent://psr/default/topic-01")
	err := m.initPartitionProducers()
	if err != nil {
		t.Fatal(err)
	}
	// pp.Println("partitions ", m.ps)
}
