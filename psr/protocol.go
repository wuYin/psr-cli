package psr

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"psr-cli/pb"
)

const (
	MAGIC_NUMBER = uint16(0x0e01)
)

//
// section: <FRAME_SIZE> [CMD_SIZE] [cmd]
// type   :  uint32       uint32     bytes
// size   :  4            4          x
//
// wrap normal cmd as protocol bytes
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

func unwrapCmd(buf []byte) (*pb.BaseCommand, error) {
	var cmd pb.BaseCommand
	if err := proto.Unmarshal(buf, &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

//
// section: <META_SIZE> [meta] [MSG_CONTENT]
// type   :  uint32      bytes   bytes
// size   :  4           x       x
//
// wrap user content to single msg
func serializeSingleMsg(content []byte) ([]byte, error) {
	singleMeta := &pb.SingleMessageMetadata{
		PayloadSize: proto.Int32(int32(len(content))),
	}
	rawMeta, err := proto.Marshal(singleMeta)
	if err != nil {
		return nil, err
	}

	// single msg as batch payload
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(len(rawMeta)))
	payload = append(payload, rawMeta...)
	payload = append(payload, content...)
	return payload, nil
}

// TODO: reduce return arguments
func unserializeSingleMsg(buf []byte) ([]byte, int, error) {
	metaSize := binary.BigEndian.Uint32(buf)
	buf = buf[4:]

	rawMeta := buf[:metaSize]
	var singleMeta pb.SingleMessageMetadata
	if err := proto.Unmarshal(rawMeta, &singleMeta); err != nil {
		return nil, 0, err
	}

	l := metaSize
	r := metaSize + uint32(singleMeta.GetPayloadSize())

	return buf[l:r], int(4 + r), nil
}

//
// section: <FRAME_SIZE> [CMD_SIZE][cmd]  [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][metadata] [payload]
// type   :  uint32       uint32    bytes  uint16        uint32     uint32         bytes      bytes
// bytes  :  4            4         x      2             4          4              x          x
//
// wrap send cmd as protocol bytes
func serializeBatch(send proto.Message, meta proto.Message, payload []byte) ([]byte, error) {
	cmdSize := proto.Size(send)
	metaSize := proto.Size(meta)
	checksumSize := 2 + 4
	payloadSize := len(payload)
	headerSize := 4 + cmdSize + checksumSize + 4 + metaSize
	frameSize := headerSize + payloadSize

	pkg := make([]byte, 4+4)

	// total frame size
	binary.BigEndian.PutUint32(pkg, uint32(frameSize))

	// cmd
	binary.BigEndian.PutUint32(pkg[4:], uint32(cmdSize))
	cmd, err := proto.Marshal(send)
	if err != nil {
		return nil, err
	}
	pkg = append(pkg, cmd...)

	// checksum
	var n int
	pkg, n = expand(pkg, checksumSize)
	binary.BigEndian.PutUint16(pkg[n:], MAGIC_NUMBER)
	checkIdx := n + 2
	binary.BigEndian.PutUint32(pkg[n+2:], 0) // checksum placeholder

	// metadata
	metaIdx := len(pkg)
	pkg, n = expand(pkg, 4)
	binary.BigEndian.PutUint32(pkg[n:], uint32(metaSize))
	metadata, err := proto.Marshal(meta)
	if err != nil {
		return nil, err
	}
	pkg = append(pkg, metadata...)

	// payload
	pkg = append(pkg, payload...)

	checksum := crc(pkg[metaIdx:])
	binary.BigEndian.PutUint32(pkg[checkIdx:checkIdx+4], checksum)
	return pkg, nil
}

// unwrap package bytes to messages
// header : pkg meta cmd
// payload: batched single messages
func unserializeBatch(h *pb.CommandMessage, p []byte) ([]*message, error) {
	p = cp(p)

	// checksum and check
	magic := binary.BigEndian.Uint16(p)
	if magic != MAGIC_NUMBER {
		return nil, fmt.Errorf("invalid magic number: %d != %d", magic, MAGIC_NUMBER)
	}
	p = p[2:]

	checksum := binary.BigEndian.Uint32(p)
	if checksum != crc(p[4:]) {
		return nil, errors.New("invalid checksum")
	}
	p = p[4:]

	// batch metadata
	metaSize := binary.BigEndian.Uint32(p)
	p = p[4:]

	metadata := p[:metaSize]
	var batchMeta pb.MessageMetadata
	if err := proto.Unmarshal(metadata, &batchMeta); err != nil {
		return nil, err
	}
	p = p[metaSize:]

	// read batched single messages
	n := batchMeta.GetNumMessagesInBatch()
	if n == 0 {
		n = 1
	}
	msgs := make([]*message, n)
	for i := 0; i < int(n); i++ {
		content, read, err := unserializeSingleMsg(p)
		if err != nil {
			return nil, err
		}
		p = p[read:] // move to next single msg

		mid := h.GetMessageId()
		msgs[i] = &message{
			publishTim: convMsTs(batchMeta.GetPublishTime()),
			msgId: &messageID{
				partitionIdx: -1, // same as producer, must be filled by partitioned consumer
				ledgerId:     int64(mid.GetLedgerId()),
				batchIdx:     -1,
				entryId:      int64(mid.GetEntryId()),
			},
			content: content,
			topic:   "",
		}
	}

	return msgs, nil
}
