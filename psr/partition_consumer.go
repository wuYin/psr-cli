package psr

import (
	"github.com/golang/protobuf/proto"
	"github.com/k0kubun/pp"
	"psr-cli/pb"
)

type partitionConsumer struct {
	c         *Consumer
	cli       *Client
	topic     string
	partition int
	cid       uint64
	cname     string

	readyCh chan struct{}
	msgsCh  chan []*message
}

func newPartitionConsumer(c *Consumer, topic string, partition int) *partitionConsumer {
	pc := &partitionConsumer{
		c:         c,
		cli:       nil,
		topic:     topic,
		partition: partition,
		cid:       c.nextCId(),
		cname:     "",

		readyCh: make(chan struct{}),
		msgsCh:  make(chan []*message, 10),
	}
	go pc.flowLoop()

	return pc
}

func (c *partitionConsumer) flowLoop() {
	for {
		select {
		case _, ok := <-c.readyCh:
			if !ok {
				return
			}
			c.flow(1) // expect just 1
		case msgs, ok := <-c.msgsCh:
			if !ok {
				return
			}
			for i := range msgs {
				msgs[i].topic = c.topic
				msgs[i].msgId.partitionIdx = c.partition
			}
			c.c.msgsCh <- msgs
		}
	}
}

func (c *partitionConsumer) flow(permit uint32) {
	t := pb.BaseCommand_FLOW
	cmd := &pb.BaseCommand{
		Type: &t,
		Flow: &pb.CommandFlow{
			ConsumerId:     proto.Uint64(c.cid),
			MessagePermits: proto.Uint32(permit),
		},
	}
	c.cli.conn.shotCmd(cmd)
}

func (c *partitionConsumer) register() error {
	broker, err := c.c.lp.lookup(c.topic)
	if err != nil {
		return err
	}

	cli, err := newClient(broker.Host, nil, c.msgsCh)
	if err != nil {
		return err
	}
	c.cli = cli

	t := pb.BaseCommand_SUBSCRIBE
	exclusive := pb.CommandSubscribe_Exclusive
	latest := pb.CommandSubscribe_Latest
	reqId := c.cli.nextReqId()
	cmd := &pb.BaseCommand{
		Type: &t,
		Subscribe: &pb.CommandSubscribe{
			Topic:           proto.String(c.topic),
			Subscription:    proto.String(c.c.subName),
			SubType:         &exclusive,
			ConsumerId:      proto.Uint64(c.cid),
			RequestId:       &reqId,
			ConsumerName:    proto.String(""),
			InitialPosition: &latest,
		},
	}
	resp, err := c.cli.conn.sendCmd(reqId, cmd)
	if err != nil {
		return err
	}
	if resp.ConsumerStatsResponse != nil {
		c.cname = *resp.ConsumerStatsResponse.ConsumerName
	}

	switch resp.GetType() {
	case pb.BaseCommand_SUCCESS:
		pp.Println("consumer register succeed!")
		c.readyCh <- struct{}{}
	case pb.BaseCommand_ERROR:
		pp.Println("consumer register failed...")
	default:
		pp.Println("unexpected resp: ", resp.GetType())
	}
	return nil
}
