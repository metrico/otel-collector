package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestComponentsInit is the distribution's startup smoke test.
//
// Its real value is that it exists at all: running any test in package main
// forces the package init() of every imported component into the test binary.
// A dependency that panics during init -- as SermoDigital/jose did under Go
// 1.27 with "crypto: RegisterHash of unknown hash function" (#167) -- crashes
// this test instead of shipping in the `latest` image.
func TestComponentsInit(t *testing.T) {
	factories, err := components()
	require.NoError(t, err)

	assert.NotEmpty(t, factories.Receivers, "no receiver factories registered")
	assert.NotEmpty(t, factories.Processors, "no processor factories registered")
	assert.NotEmpty(t, factories.Exporters, "no exporter factories registered")
	assert.NotEmpty(t, factories.Extensions, "no extension factories registered")
	assert.NotEmpty(t, factories.Connectors, "no connector factories registered")
}

// TestComponentsNoDuplicateFactories guards against a component being listed
// twice in components(), which otelcol reports as an error rather than a panic.
func TestComponentsNoDuplicateFactories(t *testing.T) {
	_, err := components()
	require.NoError(t, err)
}
