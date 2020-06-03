package core

import (
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/k0kubun/pp"
	"io"
	"net"
	"psr-cli/pb"
)

type Producer struct {
	reqId  uint64
	broker string
	conn   net.Conn
	buf    []byte
}

func NewProducer(broker string) *Producer {
	p := &Producer{
		reqId:  0,
		broker: broker,
		buf:    nil,
	}
	conn, err := net.Dial("tcp", p.broker)
	if err != nil {
		panic(err)
	}
	p.conn = conn
	if err = p.handshake(); err != nil {
		panic(err)
	}
	return p
}

func (p *Producer) partitions(topic string) (int, error) {
	p.reqId++
	t := pb.BaseCommand_PARTITIONED_METADATA
	cmd := &pb.BaseCommand{
		Type: &t,
		PartitionMetadata: &pb.CommandPartitionedTopicMetadata{
			Topic:     proto.String(topic),
			RequestId: proto.Uint64(p.reqId),
		},
	}
	p.write(cmd)
	resp, err := p.read()
	if err != nil {
		return 0, err
	}
	if resp.PartitionMetadataResponse == nil {
		return 0, errors.New("empty topic metadata resp")
	}
	return int(*resp.PartitionMetadataResponse.Partitions), nil
}

func (p *Producer) handshake() error {
	t := pb.BaseCommand_CONNECT
	cmd := &pb.BaseCommand{
		Type: &t,
		Connect: &pb.CommandConnect{
			ClientVersion: proto.String("Pulsar Go 0.1"),
		},
	}

	p.write(cmd)
	resp, err := p.read()
	if err != nil {
		return err
	}
	if resp.Connected == nil {
		return errors.New("empty handshake resp")
	}
	pp.Println("handshake ", *resp.Connected.ServerVersion)
	return nil
}

func (p *Producer) write(cmd *pb.BaseCommand) error {
	buf, err := wrap(cmd)
	if err != nil {
		return err
	}
	if _, err = p.conn.Write(buf); err != nil {
		return err
	}
	return nil
}

func (p *Producer) read() (*pb.BaseCommand, error) {
	buf, err := p.recv(4)
	if err != nil {
		return nil, err
	}

	var frameSize, cmdSize uint32
	frameSize = binary.BigEndian.Uint32(buf)
	buf, err = p.recv(4)
	if err != nil {
		return nil, err
	}
	_ = frameSize

	cmdSize = binary.BigEndian.Uint32(buf)
	buf, err = p.recv(int(cmdSize))
	if err != nil {
		return nil, err
	}

	return unwrap(buf)
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

func (p *Producer) recv(size int) ([]byte, error) {
	if len(p.buf) > size {
		res := cp(p.buf[:size])
		p.buf = p.buf[size:]
		return res, nil
	}
	remain := size - len(p.buf)
	backup := cp(p.buf)

	p.buf = make([]byte, 1024)
	n, err := io.ReadAtLeast(p.conn, p.buf, remain)
	if err != nil {
		return nil, err
	}
	p.buf = p.buf[:n]
	backup = append(backup, p.buf...)

	res := cp(backup[:size])
	p.buf = backup[size:]
	return res, nil
}

func cp(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
