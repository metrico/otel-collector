package compress

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

type Decompressor struct {
	maxDecompressedSizeBytes int64
	decoders                 map[string]func(body io.ReadCloser) (io.ReadCloser, error)
}

func NewDecompressor(maxDecompressedSizeBytes int64) *Decompressor {
	return &Decompressor{
		maxDecompressedSizeBytes: maxDecompressedSizeBytes,
		decoders: map[string]func(r io.ReadCloser) (io.ReadCloser, error){
			"gzip": func(r io.ReadCloser) (io.ReadCloser, error) {
				gr, err := gzip.NewReader(r)
				if err != nil {
					return nil, err
				}
				return gr, nil
			},
		},
	}
}

func (d *Decompressor) readBytes(r io.ReadCloser) (*bytes.Buffer, error) {
	buf := PrepareBuffer(d.maxDecompressedSizeBytes)

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

// Reads and decompresses the accepted reader, applying the configured decompressed size limit
func (d *Decompressor) Decompress(r io.ReadCloser, encoding string) (*bytes.Buffer, error) {
	decoder, ok := d.decoders[encoding]
	if !ok {
		return nil, fmt.Errorf("unsupported encoding")
	}

	dr, err := decoder(r)
	if err != nil {
		return nil, err
	}

	return d.readBytes(dr)
}

func PrepareBuffer(maxDecompressedSizeBytes int64) *bytes.Buffer {
	var (
		buf                      bytes.Buffer
		expectedMinDataSizeBytes int64 = 10e3
	)

	if maxDecompressedSizeBytes < expectedMinDataSizeBytes {
		expectedMinDataSizeBytes = maxDecompressedSizeBytes
	}
	// extra space to try avoid realloc where expected size fits enough
	buf.Grow(int(expectedMinDataSizeBytes) + bytes.MinRead)
	return &buf
}
