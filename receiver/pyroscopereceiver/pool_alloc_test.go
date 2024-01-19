package pyroscopereceiver

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/compress"
	"github.com/stretchr/testify/assert"
)

func TestAllocDecompress(t *testing.T) {
	dist := []string{
		filepath.Join("testdata", "cortex-dev-01__kafka-0__cpu__0.jfr"),
		filepath.Join("testdata", "memory_alloc_live_example.jfr"),
	}
	compressed := []*bytes.Buffer{
		loadCompressed(t, dist[0]),
		loadCompressed(t, dist[1]),
	}
	d := compress.NewDecompressor(1024 * 1024 * 1024)
	j := 0
	p := &sync.Pool{}

	n := testing.AllocsPerRun(100, func() {
		buf := acquireBuf(p)
		d.Decompress(compressed[j], compress.Gzip, buf)
		releaseBuf(p, buf)
		j = (j + 1) % len(dist)
	})
	t.Logf("\naverage alloc count: %f", n)
}

func loadCompressed(t *testing.T, jfr string) *bytes.Buffer {
	uncompressed, err := os.ReadFile(jfr)
	if err != nil {
		assert.NoError(t, err, "failed to load jfr")
	}
	compressed := new(bytes.Buffer)
	gw := gzip.NewWriter(compressed)
	if _, err := gw.Write(uncompressed); err != nil {
		assert.NoError(t, err, "failed to compress jfr")
	}
	gw.Close()
	return compressed
}
