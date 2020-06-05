package psr

import (
	"github.com/golang/protobuf/proto"
	"github.com/k0kubun/pp"
	"psr-cli/pb"
	"sync/atomic"
)

type partitionProducer struct {
	topic     string // partitioned topic
	name      string
	p         *Producer
	cli       *Client
	seqId     uint64
	prodId    uint64
	partition int

	receiptCh chan *messageID
}

func newPartitionProducer(p *Producer, topic string, partition int) *partitionProducer {
	return &partitionProducer{
		p:         p,
		topic:     topic,
		seqId:     0,
		prodId:    p.nextProdId(), // must be unique for every client host
		partition: partition,

		receiptCh: make(chan *messageID, 10),
	}
}

func (p *partitionProducer) register() error {
	// delegate parent producer to lookup
	broker, err := p.p.lp.lookup(p.topic)
	if err != nil {
		return err
	}

	cli, err := newClient(broker.Host, p.receiptCh, nil)
	if err != nil {
		return err
	}
	p.cli = cli

	reqId := p.cli.nextReqId()
	t := pb.BaseCommand_PRODUCER
	cmd := &pb.BaseCommand{
		Type: &t,
		Producer: &pb.CommandProducer{
			Topic:        proto.String(p.topic),
			ProducerId:   proto.Uint64(p.prodId),
			RequestId:    &reqId,
			ProducerName: nil, // use broker distributed
		},
	}
	resp, err := p.cli.conn.sendCmd(reqId, cmd)
	if err != nil {
		return err
	}
	p.name = *resp.ProducerSuccess.ProducerName

	go p.transferReceipts()
	return nil
}

func (p *partitionProducer) transferReceipts() {
	for {
		receipt := <-p.receiptCh
		receipt.partitionIdx = p.partition
		receipt.batchIdx = -1
		p.p.receiptCh <- receipt
	}
}

// send serialized pkg to broker directly
// notice: send operation is async, so message pkg does not contain a requestId, it's unnecessary
func (p *partitionProducer) send(msg *message) error {
	// cmd send
	seqId := p.nextSeqId()
	t := pb.BaseCommand_SEND
	sendCmd := &pb.BaseCommand{
		Type: &t,
		Send: &pb.CommandSend{
			ProducerId: proto.Uint64(p.prodId),
			SequenceId: proto.Uint64(seqId),
			NumMessage: proto.Int32(1),
		},
	}

	// batch message meta
	msgMeta := &pb.MessageMetadata{
		ProducerName: proto.String(p.name),
		SequenceId:   proto.Uint64(seqId),
		PublishTime:  proto.Uint64(uint64(nowMsTs())), // current ms ts
	}

	// produce content as a single msg
	payload, err := serializeSingleMsg(msg.content)
	if err != nil {
		return err
	}

	// serialized to batch to raw pkg
	batch, err := serializeBatch(sendCmd, msgMeta, payload)
	if err != nil {
		return err
	}

	n, err := p.cli.conn.sendPkg(batch)
	if err != nil {
		return err
	}
	_ = n
	return nil
}

func (p *partitionProducer) close() {
	t := pb.BaseCommand_CLOSE_PRODUCER
	reqId := p.cli.nextReqId()
	cmd := &pb.BaseCommand{
		Type: &t,
		CloseProducer: &pb.CommandCloseProducer{
			ProducerId: proto.Uint64(p.prodId),
			RequestId:  proto.Uint64(reqId),
		},
	}
	resp, err := p.cli.conn.sendCmd(reqId, cmd)
	if err != nil {
		pp.Printf("close producer %d failed", p.prodId)
		return
	}
	_ = resp
	pp.Printf("producer %s closed\n", int(p.prodId))
}

func (p *partitionProducer) nextSeqId() uint64 {
	return atomic.AddUint64(&p.seqId, 1)
}
