package psr

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"net"
	"psr-cli/pb"
	"sync/atomic"
)

type Client struct {
	reqId  uint64
	prodId uint64
	addr   string
	conn   *Connection
}

func newClient(addr string, receiptCh chan *messageID) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	logicConn, err := NewConnection(conn, receiptCh)
	if err != nil {
		return nil, err
	}
	cli := &Client{
		addr: addr,
		conn: logicConn,
	}
	return cli, nil
}

func (c *Client) partitionTopics(topic string) ([]string, error) {
	reqId := c.nextReqId()
	t := pb.BaseCommand_PARTITIONED_METADATA
	cmd := &pb.BaseCommand{
		Type: &t,
		PartitionMetadata: &pb.CommandPartitionedTopicMetadata{
			Topic:     proto.String(topic),
			RequestId: proto.Uint64(reqId),
		},
	}
	resp, err := c.conn.sendCmd(reqId, cmd)
	if err != nil {
		return nil, err
	}
	if resp.PartitionMetadataResponse == nil {
		return nil, errors.New("empty topic metadata resp")
	}
	n := int(*resp.PartitionMetadataResponse.Partitions)
	if n == 1 {
		return []string{topic}, nil
	}

	topics := make([]string, n)
	for i := 0; i < n; i++ {
		topics[i] = fmt.Sprintf("%s-partition-%d", topic, i)
	}
	return topics, nil
}

// id generators
func (c *Client) nextReqId() uint64 {
	return atomic.AddUint64(&c.reqId, 1)
}
