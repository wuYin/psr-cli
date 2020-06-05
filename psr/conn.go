package psr

import (
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/k0kubun/pp"
	"io"
	"net"
	"psr-cli/pb"
)

type respHandler func(resp *pb.BaseCommand, err error)

type sendReq struct {
	reqId *uint64
	req   *pb.BaseCommand
	h     respHandler
}

type recvResp struct {
	resp *pb.BaseCommand
}

type Connection struct {
	conn   net.Conn
	buf    []byte
	sendCh chan *sendReq
	recvCh chan *recvResp
	queue  map[uint64]*sendReq // wait for response
}

func NewConnection(conn net.Conn) (*Connection, error) {
	c := &Connection{
		conn:   conn,
		buf:    nil,
		sendCh: make(chan *sendReq, 100),
		recvCh: make(chan *recvResp, 100),
		queue:  make(map[uint64]*sendReq),
	}
	if err := c.handshake(); err != nil {
		return nil, err
	}
	return c, nil
}

// build logic connection
func (c *Connection) handshake() error {
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

// rw
func (c *Connection) sendCmd(cmd *pb.BaseCommand) (*pb.BaseCommand, error) {
	c.writeCmd(cmd)
	resp, payload, err := c.readCmd()
	_ = payload // this should be empty
	return resp, err
}

func (c *Connection) sendPkg(pkg []byte) (int, error) {
	return c.conn.Write(pkg)
}

// writeCmd should be used in client.go only
// because write and read is sync, must combine as a pair to use
func (c *Connection) writeCmd(cmd *pb.BaseCommand) error {
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
func (c *Connection) readCmd() (*pb.BaseCommand, []byte, error) {
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

func (c *Connection) read(size int) ([]byte, error) {
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
