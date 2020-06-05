package psr

import (
	"github.com/k0kubun/pp"
	"sync/atomic"
)

type Consumer struct {
	broker  string
	topic   string
	cli     *Client
	cs      []*partitionConsumer
	lp      *Lookuper
	subName string // subscription name

	msgsCh chan []*message
	cid    uint64
}

func NewConsumer(broker, topic, subName string) *Consumer {
	cli, err := newClient(broker, nil, nil)
	if err != nil {
		panic(err)
	}
	c := &Consumer{
		broker:  broker,
		topic:   topic,
		cli:     cli,
		lp:      newLookuper(cli),
		subName: subName,
		msgsCh:  make(chan []*message, 100),
		cid:     0,
	}

	if err = c.initPartitionConsumers(); err != nil {
		panic(err)
	}
	return c
}

func (c *Consumer) Receive() (*message, error) {
	msgs := <-c.msgsCh
	if len(msgs) > 1 {
		pp.Println("overload: ", len(msgs))
	}
	return msgs[0], nil
}

func (c *Consumer) initPartitionConsumers() error {
	topics, err := c.cli.partitionTopics(c.topic)
	if err != nil {
		return err
	}
	for i, t := range topics {
		pc := newPartitionConsumer(c, t, i)
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
