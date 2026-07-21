package gigapipeexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestFactoryTypes verifies the exporter registers under its current
// "gigapipe" type and keeps the deprecated "qryn" alias working.
func TestFactoryTypes(t *testing.T) {
	assert.Equal(t, "gigapipe", NewFactory().Type().String())
	assert.Equal(t, "qryn", NewFactoryQryn().Type().String())
}

// TestFactoryDefaultConfigParity confirms both factories produce the same
// default configuration, so the deprecated alias behaves identically.
func TestFactoryDefaultConfigParity(t *testing.T) {
	assert.Equal(t, NewFactory().CreateDefaultConfig(), NewFactoryQryn().CreateDefaultConfig())
}
