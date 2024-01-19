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
	maxUncompressedSizeBytes int64
	decoders                 map[codec]func(body io.Reader) (io.Reader, error)
}

// Creates a new decompressor
func NewDecompressor(maxUncompressedSizeBytes int64) *Decompressor {
	return &Decompressor{
		maxUncompressedSizeBytes: maxUncompressedSizeBytes,
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

func (d *Decompressor) readBytes(r io.Reader, out *bytes.Buffer) error {
	// read max+1 to validate size via a single Read()
	lr := io.LimitReader(r, d.maxUncompressedSizeBytes+1)

	n, err := out.ReadFrom(lr)
	if err != nil {
		return err
	}
	if n < 1 {
		return fmt.Errorf("empty profile")
	}
	if n > d.maxUncompressedSizeBytes {
		return fmt.Errorf("body size exceeds the limit %d bytes", d.maxUncompressedSizeBytes)
	}
	return nil
}

// Decodes the accepted reader, applying the configured size limit to avoid oom by compression bomb
func (d *Decompressor) Decompress(r io.Reader, c codec, out *bytes.Buffer) error {
	decoder, ok := d.decoders[c]
	if !ok {
		return fmt.Errorf("unsupported encoding")
	}

	dr, err := decoder(r)
	if err != nil {
		return err
	}

	return d.readBytes(dr, out)
}
