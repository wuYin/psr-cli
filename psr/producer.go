package psr

import (
	"errors"
	"github.com/k0kubun/pp"
	"sync/atomic"
)

type Producer struct {
	broker string
	topic  string
	cli    *Client
	lp     *Lookuper
	prodId uint64

	ps        map[uint64]*partitionProducer // producer_id -> producer
	receiptCh chan *messageID

	maxBatch int
}

func NewProducer(conf *ProducerConf) *Producer {
	if conf == nil || !conf.check() {
		pp.Println("invalid producer config")
		return nil
	}

	cli, err := newClient(conf.Broker, nil, nil)
	if err != nil {
		panic(err)
	}
	p := &Producer{
		broker: conf.Broker,
		topic:  conf.Topic,
		cli:    cli,
		lp:     newLookuper(cli),
		prodId: 0,

		ps:        make(map[uint64]*partitionProducer),
		receiptCh: make(chan *messageID, 10),

		maxBatch: conf.MaxBatch,
	}
	if err := p.initPartitionProducers(); err != nil {
		panic(err)
	}
	return p
}

func (p *Producer) Send(msg *message) (*messageID, error) {
	// map range is random, sufficient router for now
	var pp *partitionProducer
	for _, pp = range p.ps {
		break
	}
	if pp == nil {
		return nil, errors.New("no producer available")
	}
	err := pp.send(msg)
	if err != nil {
		return nil, err
	}

	// wait receipt
	msgId := <-p.receiptCh
	return msgId, nil
}

func (p *Producer) AsyncSend(msg *message) {
	var pp *partitionProducer
	for _, pp = range p.ps { // FIXME: map range not random well in the beginning actually
		break
	}
	if pp == nil {
		print("no producer available")
		return
	}
	pp.send(msg)
}

func (p *Producer) Close() {
	for _, pp := range p.ps {
		pp.close()
	}
}

func (p *Producer) initPartitionProducers() error {
	topics, err := p.cli.partitionTopics(p.topic)
	if err != nil {
		return err
	}

	avgBatch := p.maxBatch / len(topics)
	for i, t := range topics {
		pp := newPartitionProducer(p, t, i, avgBatch)
		if err = pp.register(); err != nil {
			return err
		}
		p.ps[pp.prodId] = pp
	}
	return nil
}

func (p *Producer) nextProdId() uint64 {
	return atomic.AddUint64(&p.prodId, 1)
}
