package core

import (
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/k0kubun/pp"
	"io"
	"net"
	"psr-cli/pb"
	"sync/atomic"
)

type Client struct {
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

func (c *Client) sendCmd(cmd *pb.BaseCommand) (*pb.BaseCommand, error) {
	c.writeCmd(cmd)
	return c.readCmd()
}

func (c *Client) sendPkg(pkg []byte) (int, error) {
	return c.conn.Write(pkg)
}

func (c *Client) writeCmd(cmd *pb.BaseCommand) error {
	buf, err := wrap(cmd)
	if err != nil {
		return err
	}
	if _, err = c.conn.Write(buf); err != nil {
		return err
	}
	return nil
}

func (c *Client) readCmd() (*pb.BaseCommand, error) {
	buf, err := c.read(4)
	if err != nil {
		return nil, err
	}

	var frameSize, cmdSize uint32
	frameSize = binary.BigEndian.Uint32(buf)
	buf, err = c.read(4)
	if err != nil {
		return nil, err
	}
	_ = frameSize

	cmdSize = binary.BigEndian.Uint32(buf)
	buf, err = c.read(int(cmdSize))
	if err != nil {
		return nil, err
	}

	return unwrap(buf)
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

func (c *Client) nextReqId() uint64 {
	return atomic.AddUint64(&c.reqId, 1)
}

func (c *Client) nextProducerId() uint64 {
	return atomic.AddUint64(&c.prodId, 1)
}

func unwrap(buf []byte) (*pb.BaseCommand, error) {
	var cmd pb.BaseCommand
	if err := proto.Unmarshal(buf, &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

func wrap(cmd *pb.BaseCommand) ([]byte, error) {
	// [frame_size] [cmd_size] [cmd]
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
