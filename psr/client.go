package psr

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/k0kubun/pp"
	"io"
	"net"
	"psr-cli/pb"
	"sync"
	"sync/atomic"
)

type Client struct {
	lock   sync.Mutex
	reqId  uint64
	prodId uint64
	buf    []byte
	addr   string
	conn   net.Conn
}

func newClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	cli := &Client{
		lock: sync.Mutex{},
		addr: addr,
		conn: conn,
		buf:  nil,
	}
	if err = cli.handshake(); err != nil {
		return nil, err
	}
	return cli, nil
}

// build logic connection
func (c *Client) handshake() error {
	t := pb.BaseCommand_CONNECT
	cmd := &pb.BaseCommand{
		Type: &t,
		Connect: &pb.CommandConnect{
			ClientVersion: proto.String("Pulsar Go 0.1"),
		},
	}

	resp, err := c.sendCmd(cmd)
	if err != nil {
		return err
	}
	if resp.Connected == nil {
		return errors.New("empty handshake resp")
	}
	pp.Println("handshake ", *resp.Connected.ServerVersion)
	return nil
}

// 
// common helpers
// 
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
	resp, err := c.sendCmd(cmd)
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

// 
// rw
// 
func (c *Client) sendCmd(cmd *pb.BaseCommand) (*pb.BaseCommand, error) {
	c.writeCmd(cmd)
	resp, payload, err := c.readCmd()
	_ = payload // this should be empty
	return resp, err
}

func (c *Client) sendPkg(pkg []byte) (int, error) {
	return c.conn.Write(pkg)
}

// writeCmd should be used in client.go only
// because write and read is sync, must combine as a pair to use
func (c *Client) writeCmd(cmd *pb.BaseCommand) error {
	buf, err := wrapCmd(cmd)
	if err != nil {
		return err
	}
	if _, err = c.conn.Write(buf); err != nil {
		return err
	}
	return nil
}

// [frame_size] [cmd_size] [cmd] [response payload]
func (c *Client) readCmd() (*pb.BaseCommand, []byte, error) {
	buf, err := c.read(4)
	if err != nil {
		return nil, nil, err
	}

	var frameSize, cmdSize uint32
	frameSize = binary.BigEndian.Uint32(buf)
	buf, err = c.read(4)
	if err != nil {
		return nil, nil, err
	}
	_ = frameSize

	cmdSize = binary.BigEndian.Uint32(buf)
	buf, err = c.read(int(cmdSize))
	if err != nil {
		return nil, nil, err
	}

	// read resp cmd
	respCmd, err := unwrapCmd(buf)
	if err != nil {
		return nil, nil, err
	}

	var payload []byte = nil
	payloadSize := frameSize - (cmdSize + 4)
	if payloadSize > 0 {
		// read resp payload
		payload, err = c.read(int(payloadSize))
		if err != nil {
			return nil, nil, err
		}
	}
	return respCmd, payload, nil
}

func (c *Client) read(size int) ([]byte, error) {
	if len(c.buf) > size {
		res := cp(c.buf[:size])
		c.buf = c.buf[size:]
		return res, nil
	}
	remain := size - len(c.buf)
	backup := cp(c.buf)

	c.buf = make([]byte, 1024)
	n, err := io.ReadAtLeast(c.conn, c.buf, remain)
	if err != nil {
		return nil, err
	}
	c.buf = c.buf[:n]
	backup = append(backup, c.buf...)

	res := cp(backup[:size])
	c.buf = backup[size:]
	return res, nil
}

// 
// id generators
// 
func (c *Client) nextReqId() uint64 {
	return atomic.AddUint64(&c.reqId, 1)
}

//
// cmd codec
// 
func unwrapCmd(buf []byte) (*pb.BaseCommand, error) {
	var cmd pb.BaseCommand
	if err := proto.Unmarshal(buf, &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

// [frame_size] [cmd_size] [cmd]
func wrapCmd(cmd *pb.BaseCommand) ([]byte, error) {
	cmdSize := uint32(proto.Size(cmd))
	frameSize := cmdSize + 4
	buf := make([]byte, 4+4)
	binary.BigEndian.PutUint32(buf, frameSize)
	binary.BigEndian.PutUint32(buf[4:], cmdSize)

	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	buf = append(buf, data...)
	return buf, nil
}
