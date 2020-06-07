package psr

import (
	"hash/crc32"
	"time"
)

func cp(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// this must be optimize, may overload memory and gc
func expand(src []byte, incr int) (dst []byte, oldLen int) {
	oldLen = len(src)
	dst = make([]byte, len(src)+incr)
	copy(dst, src)
	return
}

func crc(data []byte) uint32 {
	return crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
}

func nowMsTs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func convMsTs(ts uint64) time.Time {
	t := int64(ts) * int64(time.Millisecond)
	sec := t / int64(time.Second)
	nsec := t - (sec * int64(time.Second))
	return time.Unix(sec, nsec)
}

func fmtTime(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}
