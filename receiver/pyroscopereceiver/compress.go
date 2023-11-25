package pyroscopereceiver

import (
	"compress/gzip"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
)

type decompressor struct {
	conf     *Config
	decoders map[string]func(body io.ReadCloser) (io.ReadCloser, error)
}

func newDecompressor(conf *Config) *decompressor {
	return &decompressor{
		conf: conf,
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

// Prepares the accepted request body for decompressed read
func (d *decompressor) decompress(req *http.Request) error {
	// support gzip only
	decoder := d.decoders["gzip"]

	// support only multipart/form-data
	file, err := d.openMultipartJfr(req)
	if err != nil {
		return err
	}
	defer file.Close()

	// close old body
	if err = req.Body.Close(); err != nil {
		return err
	}

	resetHeaders(req)
	reader, err := decoder(file)
	if err != nil {
		return err
	}
	// new body should be closed by the server
	req.Body = reader
	return nil
}

func (d *decompressor) openMultipartJfr(unparsed *http.Request) (multipart.File, error) {
	if err := unparsed.ParseMultipartForm(d.conf.Protocols.Http.MaxRequestBodySize); err != nil {
		return nil, fmt.Errorf("failed to parse multipart request: %w", err)
	}

	part, ok := unparsed.MultipartForm.File["jfr"]
	if !ok {
		return nil, fmt.Errorf("required jfr part is missing")
	}
	if len(part) != 1 {
		return nil, fmt.Errorf("invalid jfr part")
	}
	jfr := part[0]
	if jfr.Filename != "jfr" {
		return nil, fmt.Errorf("invalid jfr part file")
	}
	file, err := jfr.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open jfr file")
	}
	return file, nil
}

func resetHeaders(req *http.Request) {
	// reset content-type for the new binary jfr body
	req.Header.Set("Content-Type", "application/octet-stream")
	// multipart content-types cannot have encodings so no need to Del() Content-Encoding
	// reset "Content-Length" to -1 as the size of the decompressed body is unknown
	req.Header.Del("Content-Length")
	req.ContentLength = -1
}
