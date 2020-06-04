package psr

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
