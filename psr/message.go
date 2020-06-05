package psr

import (
	"fmt"
	"time"
)

func NewMsg(content []byte) *message {
	return &message{
		content: content,
	}
}

type message struct {
	publishTim time.Time
	topic      string
	msgId      *messageID
	content    []byte
}

func (m *message) String() string {
	return fmt.Sprintf("[%s] [%s] %s %q", m.publishTim.Format("2006-01-02 15:04:05"), m.topic, m.msgId, m.content)
}

// message -> [partition, ledger, batch, entry]
type messageID struct {
	partitionIdx int
	ledgerId     int64
	batchIdx     int
	entryId      int64
}

func (m *messageID) String() string {
	return fmt.Sprintf("<%d, %d, %d %d>", m.partitionIdx, m.ledgerId, m.batchIdx, m.entryId)
}
