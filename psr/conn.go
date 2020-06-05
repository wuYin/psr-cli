package psr

import (
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"github.com/k0kubun/pp"
	"io"
	"net"
	"psr-cli/pb"
)

type notifyCh chan *recvResp

type sendReq struct {
	reqId    *uint64
	cmd      *pb.BaseCommand
	notifyCh notifyCh // notify upper layer producer or consumer
}

type recvResp struct {
	err     error
	cmd     *pb.BaseCommand
	payload []byte // multi single messages
}

type Connection struct {
	conn   net.Conn
	buf    []byte
	sendCh chan *sendReq
	recvCh chan *recvResp
	queue  map[uint64]*sendReq // reqId -> notifyCh

	receiptCh chan *messageID // dispatch message receipt
}

var (
	handshakeReqId uint64 = 0 // only for queue key usage
)

func NewConnection(conn net.Conn, receiptCh chan *messageID) (*Connection, error) {
	c := &Connection{
		conn:   conn,
		buf:    nil,
		sendCh: make(chan *sendReq, 100),
		recvCh: make(chan *recvResp, 100),
		queue:  make(map[uint64]*sendReq),

		receiptCh: receiptCh,
	}
	go c.eventloop()

	if err := c.handshake(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Connection) eventloop() {
	go c.readCmd()
	for {
		select {
		case r := <-c.recvCh:
			switch r.cmd.GetType() {
			case pb.BaseCommand_SUCCESS: // one-way operation, such as close producer
				c.queue[r.cmd.GetSuccess().GetRequestId()].notifyCh <- r
			case pb.BaseCommand_CONNECTED: // handshake
				c.queue[handshakeReqId].notifyCh <- r
			case pb.BaseCommand_PARTITIONED_METADATA_RESPONSE: // partitions
				c.queue[r.cmd.GetPartitionMetadataResponse().GetRequestId()].notifyCh <- r
			case pb.BaseCommand_LookupResponse:
				c.queue[r.cmd.GetLookupTopicResponse().GetRequestId()].notifyCh <- r
			case pb.BaseCommand_PRODUCER_SUCCESS:
				c.queue[r.cmd.GetProducerSuccess().GetRequestId()].notifyCh <- r
			case pb.BaseCommand_SEND_RECEIPT:
				mid := r.cmd.GetSendReceipt().GetMessageId()
				c.receiptCh <- &messageID{
					// partitionIdx: int(mid.GetPartition()), // it's empty, must be filled by partition producer itself
					ledgerId: int64(mid.GetLedgerId()),
					// batchIdx:     int(mid.GetBatchIndex()),
					entryId: int64(mid.GetEntryId()),
				}
			default:
				pp.Println("ignored pkg type:", r.cmd.GetType())
			}
		case req := <-c.sendCh:
			c.queue[*req.reqId] = req // pending
			err := c.writeCmd(req.cmd)
			if err != nil {
				pp.Println("conn write cmd failed:", err)
			}
		}
	}
}

// build logic connection
// handshake must be first pkg send to broker
func (c *Connection) handshake() error {
	t := pb.BaseCommand_CONNECT
	cmd := &pb.BaseCommand{
		Type: &t,
		Connect: &pb.CommandConnect{
			ClientVersion: proto.String("Pulsar Go 0.1"),
		},
	}

	ch := make(chan *recvResp)
	sendReq := &sendReq{
		reqId:    &handshakeReqId,
		cmd:      cmd,
		notifyCh: ch,
	}
	c.sendCh <- sendReq
	resp := <-ch

	if resp.err != nil {
		pp.Println("handshake failed:", resp.err)
		return resp.err
	}
	pp.Println("handshake ", *resp.cmd.Connected.ServerVersion)
	return nil
}

// block send and receive
func (c *Connection) sendCmd(reqId uint64, cmd *pb.BaseCommand) (*pb.BaseCommand, error) {
	notifyCh := make(chan *recvResp)
	req := &sendReq{
		reqId:    &reqId,
		cmd:      cmd,
		notifyCh: notifyCh,
	}
	c.sendCh <- req
	resp := <-notifyCh // block waiting...
	return resp.cmd, resp.err
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
func (c *Connection) readCmd() {
	for {
		buf, err := c.read(4)
		if err != nil {
			pp.Println("read failed:", err) // there should be reconnection
			continue
		}

		var frameSize, cmdSize uint32
		frameSize = binary.BigEndian.Uint32(buf)
		buf, err = c.read(4)
		if err != nil {
			pp.Println("read frame failed:", err)
			continue
		}
		_ = frameSize

		cmdSize = binary.BigEndian.Uint32(buf)
		buf, err = c.read(int(cmdSize))
		if err != nil {
			pp.Println("read cmd failed:", err)
			continue
		}

		// read resp cmd
		respCmd, err := unwrapCmd(buf)
		if err != nil {
			pp.Println("unwrap failed:", err)
			continue
		}

		var payload []byte = nil
		payloadSize := frameSize - (cmdSize + 4)
		if payloadSize > 0 {
			// read resp payload
			payload, err = c.read(int(payloadSize))
			if err != nil {
				pp.Println("read payload failed:", err)
				continue
			}
		}
		resp := &recvResp{
			err:     nil, // just ignore currently
			cmd:     respCmd,
			payload: payload,
		}
		c.recvCh <- resp
	}
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
