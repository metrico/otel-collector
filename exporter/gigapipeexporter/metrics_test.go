package gigapipeexporter

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func TestRemovePromForbiddenRunes(t *testing.T) {
	assert.Equal(t, "a_b_c", removePromForbiddenRunes("a.b-c"))
	assert.Equal(t, "keep_it", removePromForbiddenRunes("keep_it"))
}

func TestNormalizeLabel(t *testing.T) {
	assert.Equal(t, "", normalizeLabel(""))
	assert.Equal(t, "a_b", normalizeLabel("a.b"))
	assert.Equal(t, "key_1abc", normalizeLabel("1abc")) // leading digit
	assert.Equal(t, "key_x", normalizeLabel("_x"))      // single leading underscore
	assert.Equal(t, "__x", normalizeLabel("__x"))       // double underscore preserved
}

func TestBuildPromCompliantName(t *testing.T) {
	m := pmetric.NewMetric()
	m.SetName("my.metric")
	assert.Equal(t, "my_metric", buildPromCompliantName(m, ""))
	assert.Equal(t, "ns_my_metric", buildPromCompliantName(m, "ns"))

	d := pmetric.NewMetric()
	d.SetName("5xx")
	assert.Equal(t, "_5xx", buildPromCompliantName(d, "")) // leading digit prefixed
}

func TestBuildLabelSet_JobInstanceAndExtras(t *testing.T) {
	res := pcommon.NewResource()
	res.Attributes().PutStr(attrServiceName, "svc")
	res.Attributes().PutStr(attrServiceNamespace, "ns")
	res.Attributes().PutStr(attrServiceInstanceID, "inst-1")

	attrs := pcommon.NewMap()
	attrs.PutStr("http.method", "GET")

	ls := buildLabelSet(res, attrs, model.MetricNameLabel, "m", "__internal__", "keep")

	assert.Equal(t, model.LabelValue("ns/svc"), ls[model.JobLabel])
	assert.Equal(t, model.LabelValue("inst-1"), ls[model.InstanceLabel])
	assert.Equal(t, model.LabelValue("GET"), ls["http_method"])
	assert.Equal(t, model.LabelValue("m"), ls[model.MetricNameLabel])
	assert.Equal(t, model.LabelValue("keep"), ls["__internal__"]) // internal label kept verbatim
}

func TestIsValidAggregationTemporality(t *testing.T) {
	gauge := pmetric.NewMetric()
	gauge.SetEmptyGauge()
	assert.True(t, isValidAggregationTemporality(gauge))

	summary := pmetric.NewMetric()
	summary.SetEmptySummary()
	assert.True(t, isValidAggregationTemporality(summary))

	sumCumulative := pmetric.NewMetric()
	sumCumulative.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	assert.True(t, isValidAggregationTemporality(sumCumulative))

	sumDelta := pmetric.NewMetric()
	sumDelta.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	assert.False(t, isValidAggregationTemporality(sumDelta))

	histDelta := pmetric.NewMetric()
	histDelta.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	assert.False(t, isValidAggregationTemporality(histDelta))
}

func TestExportNumberDataPoint_GaugeIntAndDouble(t *testing.T) {
	e := &metricsExporter{}
	res := pcommon.NewResource()
	m := pmetric.NewMetric()
	m.SetName("g")

	dpDouble := pmetric.NewNumberDataPoint()
	dpDouble.SetDoubleValue(1.5)
	var samples []Sample
	var ts []TimeSerie
	require.NoError(t, e.exportNumberDataPoint(dpDouble, res, m, &samples, &ts))
	require.Len(t, samples, 1)
	assert.Equal(t, 1.5, samples[0].Value)
	assert.Equal(t, int8(SAMPLE_TYPE_METRIC), samples[0].Type)
	require.Len(t, ts, 1)

	dpInt := pmetric.NewNumberDataPoint()
	dpInt.SetIntValue(4)
	samples = nil
	ts = nil
	require.NoError(t, e.exportNumberDataPoint(dpInt, res, m, &samples, &ts))
	assert.Equal(t, float64(4), samples[0].Value)
}

func TestExportHistogramDataPoint(t *testing.T) {
	e := &metricsExporter{}
	res := pcommon.NewResource()
	m := pmetric.NewMetric()
	m.SetName("h")
	dp := pmetric.NewHistogramDataPoint()
	dp.SetCount(10)
	dp.SetSum(42)
	dp.ExplicitBounds().FromRaw([]float64{1, 5})
	dp.BucketCounts().FromRaw([]uint64{3, 4, 3})

	var samples []Sample
	var ts []TimeSerie
	require.NoError(t, e.exportHistogramDataPoint(dp, res, m, &samples, &ts))

	// sum + count + 2 explicit buckets + +Inf bucket = 5 samples
	assert.Len(t, samples, 5)
	assert.NotEmpty(t, ts)
}

func TestExportSummaryDataPoint(t *testing.T) {
	e := &metricsExporter{}
	res := pcommon.NewResource()
	m := pmetric.NewMetric()
	m.SetName("s")
	dp := pmetric.NewSummaryDataPoint()
	dp.SetCount(2)
	dp.SetSum(10)
	q := dp.QuantileValues().AppendEmpty()
	q.SetQuantile(0.99)
	q.SetValue(7)

	var samples []Sample
	var ts []TimeSerie
	require.NoError(t, e.exportSummaryDataPoint(dp, res, m, &samples, &ts))

	// sum + count + 1 quantile = 3 samples
	assert.Len(t, samples, 3)
	assert.NotEmpty(t, ts)
}

func TestCollectFromMetric_SkipsInvalidTemporality(t *testing.T) {
	e := &metricsExporter{}
	res := pcommon.NewResource()
	m := pmetric.NewMetric()
	m.SetName("d")
	m.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	m.Sum().DataPoints().AppendEmpty().SetIntValue(1)

	var samples []Sample
	var ts []TimeSerie
	require.NoError(t, e.collectFromMetric(m, res, &samples, &ts))
	assert.Empty(t, samples)
	assert.Empty(t, ts)
}

func TestCollectFromMetrics_AllTypes(t *testing.T) {
	e := &metricsExporter{}
	res := pcommon.NewResource()
	ms := pmetric.NewMetricSlice()

	gauge := ms.AppendEmpty()
	gauge.SetName("g")
	gauge.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1)

	sum := ms.AppendEmpty()
	sum.SetName("c")
	s := sum.SetEmptySum()
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	s.DataPoints().AppendEmpty().SetIntValue(2)

	hist := ms.AppendEmpty()
	hist.SetName("h")
	h := hist.SetEmptyHistogram()
	h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	hdp := h.DataPoints().AppendEmpty()
	hdp.SetCount(3)
	hdp.SetSum(9)
	hdp.ExplicitBounds().FromRaw([]float64{1})
	hdp.BucketCounts().FromRaw([]uint64{1, 2})

	summary := ms.AppendEmpty()
	summary.SetName("s")
	sdp := summary.SetEmptySummary().DataPoints().AppendEmpty()
	sdp.SetCount(1)
	sdp.SetSum(4)
	sdp.QuantileValues().AppendEmpty().SetValue(4)

	var samples []Sample
	var ts []TimeSerie
	require.NoError(t, e.collectFromMetrics(ms, res, &samples, &ts))
	assert.NotEmpty(t, samples)
	assert.NotEmpty(t, ts)
}

func TestPushMetricsData_SingleNode(t *testing.T) {
	fc := &fakeConn{}
	e := &metricsExporter{logger: zap.NewNop(), db: fc}

	require.NoError(t, e.pushMetricsData(context.Background(), newGaugeMetrics("g", 3)))

	require.Len(t, fc.queries, 2)
	joined := strings.Join(fc.queries, "\n")
	assert.Contains(t, joined, "samples_v3")
	assert.Contains(t, joined, "time_series")
	require.Len(t, fc.batches, 2)
	assert.True(t, fc.batches[0].sent)
	assert.True(t, fc.batches[1].sent)
}

func TestPushMetricsData_PrepareBatchError(t *testing.T) {
	fc := &fakeConn{prepareErr: errFakePrepare}
	e := &metricsExporter{logger: zap.NewNop(), db: fc}
	err := e.pushMetricsData(context.Background(), newGaugeMetrics("g", 1))
	require.ErrorIs(t, err, errFakePrepare)
}

func TestPushMetricsData_TimeSeriesBatchError(t *testing.T) {
	// fail only the second (time_series) PrepareBatch to cover that branch
	fc := &fakeConn{prepareFailOn: "time_series"}
	e := &metricsExporter{logger: zap.NewNop(), db: fc}
	err := e.pushMetricsData(context.Background(), newGaugeMetrics("g", 1))
	require.ErrorIs(t, err, errFakePrepare)
}

func TestMetricsShutdownClosesConn(t *testing.T) {
	fc := &fakeConn{}
	e := &metricsExporter{db: fc}
	require.NoError(t, e.Shutdown(context.Background()))
	assert.True(t, fc.closed)
}
