package compress

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

type codec uint8

const (
	Gzip codec = iota
)

// Decodes compressed streams
type Decompressor struct {
	decompressedSizeBytesExpectedValue int64
	maxDecompressedSizeBytes           int64
	decoders                           map[codec]func(body io.Reader) (io.Reader, error)
}

// Creates a new decompressor
func NewDecompressor(decompressedSizeBytesExpectedValue int64, maxDecompressedSizeBytes int64) *Decompressor {
	return &Decompressor{
		decompressedSizeBytesExpectedValue: decompressedSizeBytesExpectedValue,
		maxDecompressedSizeBytes:           maxDecompressedSizeBytes,
		decoders: map[codec]func(r io.Reader) (io.Reader, error){
			Gzip: func(r io.Reader) (io.Reader, error) {
				gr, err := gzip.NewReader(r)
				if err != nil {
					return nil, err
				}
				return gr, nil
			},
		},
	}
}

func (d *Decompressor) readBytes(r io.Reader) (*bytes.Buffer, error) {
	buf := d.prepareBuffer()

	// read max+1 to validate size via a single Read()
	lr := io.LimitReader(r, d.maxDecompressedSizeBytes+1)

	n, err := buf.ReadFrom(lr)
	if err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, fmt.Errorf("empty profile")
	}
	if n > d.maxDecompressedSizeBytes {
		return nil, fmt.Errorf("body size exceeds the limit %d bytes", d.maxDecompressedSizeBytes)
	}
	return buf, nil
}

// Decodes the accepted reader, applying the configured size limit to avoid oom by compression bomb
func (d *Decompressor) Decompress(r io.Reader, c codec) (*bytes.Buffer, error) {
	decoder, ok := d.decoders[c]
	if !ok {
		return nil, fmt.Errorf("unsupported encoding")
	}

	dr, err := decoder(r)
	if err != nil {
		return nil, err
	}

	return d.readBytes(dr)
}

// Pre-allocates a buffer based on heuristics to minimize resize
func (d *Decompressor) prepareBuffer() *bytes.Buffer {
	var buf bytes.Buffer
	// extra space to try avoid realloc where expected size fits enough
	buf.Grow(int(d.decompressedSizeBytesExpectedValue) + bytes.MinRead)
	return &buf
}
