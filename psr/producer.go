package psr

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"psr-cli/pb"
	"sync/atomic"
	"time"
)

type Producer struct {
	broker string
	topic  string
	ps     []*partitionProducer
	cli    *Client
	lp     *Lookuper
	prodId uint64
}

func NewProducer(broker, topic string) *Producer {
	cli, err := newClient(broker)
	if err != nil {
		panic(err)
	}
	m := &Producer{
		broker: broker,
		topic:  topic,
		cli:    cli,
		lp:     newLookuper(cli),
		prodId: 0,
	}
	if err := m.initPartitionProducers(); err != nil {
		panic(err)
	}
	return m
}

func (p *Producer) Send(msg *Message) (*MessageID, error) {
	if len(p.ps) == 0 {
		return nil, errors.New("no producer available")
	}
	idx := time.Now().Unix() % int64(len(p.ps)) // simple but useless
	pp := p.ps[idx]
	err := pp.send(msg)
	if err != nil {
		return nil, err
	}

	// wait receipt
	cmd, err := pp.cli.readCmd()
	if err != nil {
		return nil, err
	}
	if cmd.GetType() == pb.BaseCommand_SEND_RECEIPT {
		m := cmd.GetSendReceipt().GetMessageId()
		return &MessageID{
			partitionIdx: pp.partition,
			ledgerId:     *m.LedgerId,
			batchIdx:     -1, // not batch actually
			entryId:      *m.EntryId,
		}, nil
	}
	return nil, errors.New("invalid produce resp")
}

func (p *Producer) initPartitionProducers() error {
	topics, err := p.partitionTopics()
	if err != nil {
		return err
	}
	for i, t := range topics {
		pp := newPartitionProducer(p, t, i)
		if err = pp.register(); err != nil {
			return err
		}
		p.ps = append(p.ps, pp)
	}
	return nil
}

func (p *Producer) partitionTopics() ([]string, error) {
	n, err := p.partitions()
	if err != nil {
		return nil, err
	}
	if n == 1 {
		return []string{p.topic}, nil
	}

	topics := make([]string, n)
	for i := 0; i < n; i++ {
		topics[i] = fmt.Sprintf("%s-partition-%d", p.topic, i)
	}
	return topics, nil
}

func (p *Producer) partitions() (int, error) {
	reqId := p.cli.nextReqId()
	t := pb.BaseCommand_PARTITIONED_METADATA
	cmd := &pb.BaseCommand{
		Type: &t,
		PartitionMetadata: &pb.CommandPartitionedTopicMetadata{
			Topic:     proto.String(p.topic),
			RequestId: proto.Uint64(reqId),
		},
	}
	resp, err := p.cli.sendCmd(cmd)
	if err != nil {
		return 0, err
	}
	if resp.PartitionMetadataResponse == nil {
		return 0, errors.New("empty topic metadata resp")
	}
	return int(*resp.PartitionMetadataResponse.Partitions), nil
}

func (p *Producer) nextProdId() uint64 {
	return atomic.AddUint64(&p.prodId, 1)
}
