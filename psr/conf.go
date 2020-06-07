package psr

import "strings"

var trim = strings.TrimSpace

type ProducerConf struct {
	Broker   string
	Topic    string
	MaxBatch int
}

func (c *ProducerConf) check() bool {
	c.Broker = trim(c.Broker)
	c.Topic = trim(c.Topic)
	if c.MaxBatch == 0 {
		c.MaxBatch = 10
	}
	return c.Broker != "" && c.Topic != ""
}

type ConsumerConf struct {
	Broker    string
	Topic     string
	SubName   string
	MaxPermit int
}

func (c *ConsumerConf) check() bool {
	c.Broker = trim(c.Broker)
	c.Topic = trim(c.Topic)
	c.SubName = trim(c.SubName)
	if c.MaxPermit == 0 {
		c.MaxPermit = 100
	}
	return c.Broker != "" && c.Topic != "" && c.SubName != ""
}
