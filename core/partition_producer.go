package core

import (
	"github.com/golang/protobuf/proto"
	"psr-cli/pb"
)

type PartitionProducer struct {
	topic string // partitioned topic
	pidx  int    // partition index
	name  string
	p     *Producer
	cli   *Client
}

func newPartitionProducer(p *Producer, topic string, pidx int) *PartitionProducer {
	return &PartitionProducer{
		p:     p,
		topic: topic,
		pidx:  pidx,
	}
}

func (p *PartitionProducer) register() error {
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
	prodId := p.cli.nextProducerId() // should be global
	t := pb.BaseCommand_PRODUCER
	cmd := &pb.BaseCommand{
		Type: &t,
		Producer: &pb.CommandProducer{
			Topic:        proto.String(p.topic),
			ProducerId:   &prodId,
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
