package gigapipeexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
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

// createLogsWithObserver creates a logs exporter from the given factory and
// returns the log lines it emitted during creation. The nop settings are typed
// from the factory itself, so the qryn factory is exercised as "qryn".
func createLogsWithObserver(t *testing.T, factory exporter.Factory) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zapcore.WarnLevel)
	set := exportertest.NewNopSettings(factory.Type())
	set.Logger = zap.New(core)

	_, err := factory.CreateLogs(context.Background(), set, createDefaultConfig())
	require.NoError(t, err)
	return logs
}

// TestFactoryCreateAllSignals exercises the three create funcs (and thus the
// exporter constructors + initMetrics) end to end with the default config.
// clickhouse.Open is lazy, so no live server is required.
func TestFactoryCreateAllSignals(t *testing.T) {
	factory := NewFactory()
	set := exportertest.NewNopSettings(factory.Type())
	cfg := factory.CreateDefaultConfig()

	tr, err := factory.CreateTraces(context.Background(), set, cfg)
	require.NoError(t, err)
	require.NoError(t, tr.Shutdown(context.Background()))

	lg, err := factory.CreateLogs(context.Background(), set, cfg)
	require.NoError(t, err)
	require.NoError(t, lg.Shutdown(context.Background()))

	me, err := factory.CreateMetrics(context.Background(), set, cfg)
	require.NoError(t, err)
	require.NoError(t, me.Shutdown(context.Background()))
}

// TestQrynAliasLogsDeprecationWarning verifies the deprecated "qryn" factory
// logs a deprecation warning on use, and the "gigapipe" factory does not.
func TestQrynAliasLogsDeprecationWarning(t *testing.T) {
	qryn := NewFactoryQryn()
	require.Equal(t, "qryn", qryn.Type().String()) // guard: we are exercising the qryn alias
	qrynLogs := createLogsWithObserver(t, qryn)
	assert.Equal(t, 1, qrynLogs.FilterMessageSnippet(`"qryn" exporter type is deprecated`).Len(),
		"deprecated qryn factory should warn exactly once")

	gigapipe := NewFactory()
	require.Equal(t, "gigapipe", gigapipe.Type().String())
	gigapipeLogs := createLogsWithObserver(t, gigapipe)
	assert.Equal(t, 0, gigapipeLogs.FilterMessageSnippet("deprecated").Len(),
		"gigapipe factory should not warn")
}
