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

	registerCh chan struct{}
	msgsCh     chan []*message
}

func newPartitionConsumer(c *Consumer, topic string, partition int) *partitionConsumer {
	p := &partitionConsumer{
		c:         c,
		cli:       nil,
		topic:     topic,
		partition: partition,
		cid:       c.nextCId(),
		cname:     "",

		registerCh: make(chan struct{}),
		msgsCh:     make(chan []*message, 100),
	}
	go p.recvLoop()
	go p.recvMsgs()
	return p
}

func (c *partitionConsumer) recvLoop() {
	for {
		select {
		case _, ok := <-c.registerCh:
			if !ok {
				return
			}
			pp.Println("=-===")
			c.flow(1) // expect just 1
		case msgs, ok := <-c.msgsCh:
			if !ok {
				return
			}
			_ = msgs
		}
	}
}

func (c *partitionConsumer) recvMsgs() {
	for {
		// FIXME: panic here for using same client but mixed up sync and async rw
		cmd, payload, err := c.cli.readCmd()
		if err != nil {
			pp.Println("consumer recv failed: ", err)
			break
		}
		if cmd.GetType() != pb.BaseCommand_MESSAGE {
			pp.Println("unexpected cmd type: ", cmd.GetType())
			continue
		}

		// unwrap single messages
		pp.Println(string(payload))
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
	_, err := c.cli.sendCmd(cmd)
	if err != nil {
		pp.Println("flow failed: ", err)
	}
}

func (c *partitionConsumer) register() error {
	broker, err := c.c.lp.lookup(c.topic)
	if err != nil {
		return err
	}
	cli, err := newClient(broker.Host)
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
	resp, err := c.cli.sendCmd(cmd)
	if err != nil {
		return err
	}
	if resp.ConsumerStatsResponse != nil {
		c.cname = *resp.ConsumerStatsResponse.ConsumerName
	}

	switch resp.GetType() {
	case pb.BaseCommand_SUCCESS:
		pp.Println("consumer register succeed!")
		c.registerCh <- struct{}{}
	case pb.BaseCommand_ERROR:
		pp.Println("consumer register failed...")
	default:
		pp.Println("unexpected resp: ", resp.GetType())
	}
	return nil
}
