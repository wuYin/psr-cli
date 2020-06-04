package psr

type Message struct {
	Payload []byte
}

// message -> [partition, ledger, batch, entry]
type MessageID struct {
	partitionIdx int
	ledgerId     uint64
	batchIdx     int
	entryId      uint64
}
