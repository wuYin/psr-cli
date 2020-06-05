package psr

import "time"

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
	ledgerId     uint64
	batchIdx     int
	entryId      uint64
}
