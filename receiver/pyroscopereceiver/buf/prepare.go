package buf

import "bytes"

// Pre-allocates a buffer based on heuristics to minimize resize
func PrepareBuffer(uncompressedSizeBytes int64) *bytes.Buffer {
	var buf bytes.Buffer
	// extra space to try avoid realloc where expected size fits enough
	// TODO: try internal simple statistical model to pre-allocate a buffer
	buf.Grow(int(uncompressedSizeBytes) + bytes.MinRead)
	return &buf
}
