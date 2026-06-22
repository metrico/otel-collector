package qrynexporter

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestConvertAttributesAndMerge_DefaultIncludesAllAttrs(t *testing.T) {
	exp := &logsExporter{}

	logAttrs := pcommon.NewMap()
	logAttrs.PutStr("k8s.namespace.name", "devnet")
	logAttrs.PutStr("k8s.event.reason", "Unhealthy")

	resAttrs := pcommon.NewMap()
	resAttrs.PutStr("signoz.component", "otel-deployment")
	resAttrs.PutStr("k8s.object.kind", "Pod")

	addLogLevelAttributeAndHint(newLogRecord(plog.SeverityNumberWarn))

	// Re-copy attrs after addLogLevelAttributeAndHint for a realistic flow.
	log := newLogRecord(plog.SeverityNumberWarn)
	logAttrs.CopyTo(log.Attributes())
	addLogLevelAttributeAndHint(log)

	labels := exp.convertAttributesAndMerge(log.Attributes(), resAttrs)

	assert.Equal(t, model.LabelValue("WARN"), labels["level"])
	assert.Equal(t, model.LabelValue("devnet"), labels["k8s.namespace.name"])
	assert.Equal(t, model.LabelValue("Unhealthy"), labels["k8s.event.reason"])
	assert.Equal(t, model.LabelValue("otel-deployment"), labels["signoz.component"])
	assert.Equal(t, model.LabelValue("Pod"), labels["k8s.object.kind"])
}

func TestConvertAttributesAndMerge_RespectsResourceLabelsHint(t *testing.T) {
	exp := &logsExporter{}

	logAttrs := pcommon.NewMap()
	logAttrs.PutStr("k8s.namespace.name", "devnet")

	resAttrs := pcommon.NewMap()
	resAttrs.PutStr("signoz.component", "otel-deployment")
	resAttrs.PutStr("k8s.object.kind", "Pod")
	resAttrs.PutStr(hintResources, "signoz.component")

	log := newLogRecord(plog.SeverityNumberWarn)
	logAttrs.CopyTo(log.Attributes())
	addLogLevelAttributeAndHint(log)

	labels := exp.convertAttributesAndMerge(log.Attributes(), resAttrs)

	assert.Equal(t, model.LabelValue("otel-deployment"), labels["signoz.component"])
	assert.NotContains(t, labels, "k8s.object.kind")
	assert.Equal(t, model.LabelValue("devnet"), labels["k8s.namespace.name"])
}

func TestConvertAttributesAndMerge_RespectsAttributeLabelsHint(t *testing.T) {
	exp := &logsExporter{}

	logAttrs := pcommon.NewMap()
	logAttrs.PutStr("k8s.namespace.name", "devnet")
	logAttrs.PutStr("k8s.event.reason", "Unhealthy")
	logAttrs.PutStr(hintAttributes, "k8s.event.reason")

	resAttrs := pcommon.NewMap()
	resAttrs.PutStr("signoz.component", "otel-deployment")

	log := newLogRecord(plog.SeverityNumberWarn)
	logAttrs.CopyTo(log.Attributes())
	addLogLevelAttributeAndHint(log)

	labels := exp.convertAttributesAndMerge(log.Attributes(), resAttrs)

	assert.Equal(t, model.LabelValue("Unhealthy"), labels["k8s.event.reason"])
	assert.NotContains(t, labels, "k8s.namespace.name")
	assert.Equal(t, model.LabelValue("otel-deployment"), labels["signoz.component"])
}

func TestAddLogLevelAttributeAndHint_AppendsToExistingHint(t *testing.T) {
	log := newLogRecord(plog.SeverityNumberWarn)
	log.Attributes().PutStr(hintAttributes, "k8s.event.reason")

	addLogLevelAttributeAndHint(log)

	hint, found := log.Attributes().Get(hintAttributes)
	require.True(t, found)
	assert.Equal(t, "k8s.event.reason,level", hint.AsString())
	assert.Equal(t, "WARN", log.Attributes().AsRaw()["level"])
}

func TestAddLogLevelAttributeAndHint_DoesNotInjectHint(t *testing.T) {
	log := newLogRecord(plog.SeverityNumberWarn)

	addLogLevelAttributeAndHint(log)

	_, found := log.Attributes().Get(hintAttributes)
	assert.False(t, found)
	assert.Equal(t, "WARN", log.Attributes().AsRaw()["level"])
}

func newLogRecord(severity plog.SeverityNumber) plog.LogRecord {
	log := plog.NewLogRecord()
	log.SetSeverityNumber(severity)
	return log
}
