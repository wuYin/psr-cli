package core

import (
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"hash/crc32"
)

// wrap as protocol package bytes
func serializeBatch(send proto.Message, meta proto.Message, payload []byte) ([]byte, error) {
	// section: <TOTAL_SIZE> [CMD_SIZE][COMMAND] [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]
	// type   :  uint32       uint32    bytes    uint16        uint32     uint32         bytes      bytes
	// bytes  :  4            4         x        2             4          4              x          x
	cmdSize := proto.Size(send)
	metaSize := proto.Size(meta)
	checksumSize := 2 + 4
	payloadSize := len(payload)
	headerSize := 4 + cmdSize + checksumSize + 4 + metaSize
	totalSize := headerSize + payloadSize

	pkg := make([]byte, 4+4)

	// total size
	binary.BigEndian.PutUint32(pkg, uint32(totalSize))

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
	binary.BigEndian.PutUint16(pkg[n:], uint16(0x0e01))
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

func crc(data []byte) uint32 {
	return crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
}
