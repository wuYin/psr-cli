package psr

import (
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"github.com/k0kubun/pp"
	"psr-cli/pb"
	"sync/atomic"
	"time"
)

type partitionProducer struct {
	topic     string // partitioned topic
	name      string
	p         *Producer
	cli       *Client
	seqId     uint64
	prodId    uint64
	partition int
}

func newPartitionProducer(p *Producer, topic string, partition int) *partitionProducer {
	return &partitionProducer{
		p:         p,
		topic:     topic,
		seqId:     0,
		prodId:    p.nextProdId(), // must be unique for every client host
		partition: partition,
	}
}

func (p *partitionProducer) register() error {
	// delegate manager to lookup
	broker, err := p.p.lp.lookup(p.topic)
	if err != nil {
		return err
	}

	cli, err := newClient(broker.Host)
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
	resp, err := p.cli.sendCmd(cmd)
	if err != nil {
		return err
	}
	p.name = *resp.ProducerSuccess.ProducerName
	return nil
}

func (p *partitionProducer) send(msg *message) error {
	// single msg meta
	singleMeta := &pb.SingleMessageMetadata{
		PayloadSize: proto.Int32(int32(len(msg.payload))),
	}
	rawMeta, err := proto.Marshal(singleMeta)
	if err != nil {
		return err
	}

	// single msg as batch payload
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(len(rawMeta)))
	payload = append(payload, rawMeta...)
	payload = append(payload, msg.payload...)

	// send cmd
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
		PublishTime:  proto.Uint64(uint64(time.Now().UnixNano() / int64(time.Millisecond))), // current ms ts
	}

	// serialized to batch raw pkg
	batch, err := serializeBatch(sendCmd, msgMeta, payload)
	if err != nil {
		return err
	}

	n, err := p.cli.sendPkg(batch)
	if err != nil {
		return err
	}
	pp.Println(p.prodId, "produced ", n)
	return nil
}

func (p *partitionProducer) nextSeqId() uint64 {
	return atomic.AddUint64(&p.seqId, 1)
}
