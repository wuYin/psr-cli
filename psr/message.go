package psr

import (
	"fmt"
	"time"
)

func NewMsg(payload []byte) *message {
	return &message{
		payload: payload,
	}
}

type message struct {
	publishTim time.Time
	msgId      messageID
	payload    []byte
}

// message -> [partition, ledger, batch, entry]
type messageID struct {
	partitionIdx int
	ledgerId     int64
	batchIdx     int
	entryId      int64
}

func (m *messageID) String() string {
	return fmt.Sprintf("[%d, %d, %d %d]", m.partitionIdx, m.ledgerId, m.batchIdx, m.entryId)
}
