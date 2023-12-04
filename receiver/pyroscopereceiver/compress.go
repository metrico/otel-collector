package pyroscopereceiver

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

type decompressor struct {
	maxDecompressedSizeBytes int64
	decoders                 map[string]func(body io.ReadCloser) (io.ReadCloser, error)
}

func newDecompressor(maxDecompressedSizeBytes int64) *decompressor {
	return &decompressor{
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

func (d *decompressor) readBytes(r io.ReadCloser) (*bytes.Buffer, error) {
	var (
		buf                   bytes.Buffer
		expectedDataSizeBytes int64 = 10e3
	)

	if d.maxDecompressedSizeBytes < expectedDataSizeBytes {
		expectedDataSizeBytes = d.maxDecompressedSizeBytes
	}
	// small extra space to try avoid realloc where expected size fits enough and +1 like limit
	buf.Grow(int(expectedDataSizeBytes) + bytes.MinRead + 1)

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
	return &buf, nil
}

// Reads and decompresses the accepted reader, applying the configured decompressed size limit
func (d *decompressor) decompress(r io.ReadCloser, encoding string) (*bytes.Buffer, error) {
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
