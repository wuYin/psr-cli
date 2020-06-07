package psr

import (
	"sync/atomic"
)

type Consumer struct {
	broker  string
	topic   string
	cli     *Client
	cs      []*partitionConsumer
	lp      *Lookuper
	subName string // subscription name

	msgCh chan *message // consumer only take one msg at a time, partition consumer will do flow control
	cid   uint64

	maxPermit int // max cache message size
}

func NewConsumer(conf *ConsumerConf) *Consumer {
	if conf == nil || !conf.check() {
		panic("invalid consumer config")
	}
	cli, err := newClient(conf.Broker, nil, nil)
	if err != nil {
		panic(err)
	}
	c := &Consumer{
		broker:    conf.Broker,
		topic:     conf.Topic,
		cli:       cli,
		lp:        newLookuper(cli),
		subName:   conf.SubName,
		maxPermit: conf.MaxPermit,
		msgCh:     make(chan *message, conf.MaxPermit),
		cid:       0,
	}

	if err = c.initPartitionConsumers(); err != nil {
		panic(err)
	}
	return c
}

func (c *Consumer) Receive() (*message, error) {
	msg := <-c.msgCh
	return msg, nil
}

func (c *Consumer) Ack(msgId *messageID) {
	if msgId.partitionIdx >= len(c.cs) {
		return
	}
	c.cs[msgId.partitionIdx].ack(msgId)
}

func (c *Consumer) initPartitionConsumers() error {
	topics, err := c.cli.partitionTopics(c.topic)
	if err != nil {
		return err
	}

	avgPermit := c.maxPermit / len(topics)
	for p, t := range topics {
		pc := newPartitionConsumer(c, t, p, avgPermit)
		if err = pc.register(); err != nil {
			return err
		}
		c.cs = append(c.cs, pc)
	}
	return nil
}

func (c *Consumer) nextCId() uint64 {
	return atomic.AddUint64(&c.cid, 1)
}
