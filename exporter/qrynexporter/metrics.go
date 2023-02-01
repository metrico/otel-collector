package qrynexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/value"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

const (
	sumSuffix    = "_sum"
	countSuffix  = "_count"
	bucketSuffix = "_bucket"
	quantileStr  = "quantile"
)

type metricsExporter struct {
	logger *zap.Logger

	db clickhouse.Conn

	namespace string
}

func newMetricsExporter(logger *zap.Logger, cfg *Config) (*metricsExporter, error) {
	opts, err := clickhouse.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, err
	}
	db, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	return &metricsExporter{
		logger:    logger,
		db:        db,
		namespace: cfg.Namespace,
	}, nil
}

// Shutdown will shutdown the exporter.
func (e *metricsExporter) Shutdown(_ context.Context) error {
	if e.db != nil {
		e.db.Close()
	}
	return nil
}

func removePromForbiddenRunes(s string) string {
	return strings.Join(strings.FieldsFunc(s, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != ':' }), "_")
}

// Return '_' for anything non-alphanumeric
func sanitizeRune(r rune) rune {
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		return r
	}
	return '_'
}

// Normalizes the specified label to follow Prometheus label names standard
// See rules at https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
// Labels that start with non-letter rune will be prefixed with "key_"
// Exception is made for double-underscores which are allowed
func normalizeLabel(label string) string {
	if len(label) == 0 {
		return label
	}
	label = strings.Map(sanitizeRune, label)
	if unicode.IsDigit(rune(label[0])) {
		label = "key_" + label
	} else if strings.HasPrefix(label, "_") && !strings.HasPrefix(label, "__") {
		label = "key" + label
	}
	return label
}

func buildLabelSet(resource pcommon.Resource, attributes pcommon.Map, extras ...string) model.LabelSet {
	out := defaultExporterLabels.Clone()
	attributes.Range(func(key string, value pcommon.Value) bool {
		out[model.LabelName(normalizeLabel(key))] = model.LabelValue(value.AsString())
		return true
	})
	// service.name + service.namespace to job
	if serviceName, ok := resource.Attributes().Get(conventions.AttributeServiceName); ok {
		val := serviceName.AsString()
		if serviceNamespace, ok := resource.Attributes().Get(conventions.AttributeServiceNamespace); ok {
			val = fmt.Sprintf("%s/%s", serviceNamespace.AsString(), val)
		}
		out[model.LabelName(model.JobLabel)] = model.LabelValue(val)
	}
	// service.instance.id to instance
	if instance, ok := resource.Attributes().Get(conventions.AttributeServiceInstanceID); ok {
		out[model.LabelName(model.InstanceLabel)] = model.LabelValue(instance.AsString())
	}

	for i := 0; i < len(extras); i += 2 {
		if i+1 >= len(extras) {
			break
		}
		// internal labels should be maintained
		name := extras[i]
		if len(name) <= 4 || name[:2] != "__" || name[len(name)-2:] != "__" {
			name = normalizeLabel(name)
		}
		out[model.LabelName(name)] = model.LabelValue(extras[i+1])
	}
	return out
}

// Build a Prometheus-compliant metric name for the specified metric
//
// Metric name is prefixed with specified namespace and underscore (if any).
// Namespace is not cleaned up. Make sure specified namespace follows Prometheus
// naming convention.
//
// See rules at https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
// and https://prometheus.io/docs/practices/naming/#metric-and-label-naming
func buildPromCompliantName(metric pmetric.Metric, namespace string) string {
	var metricName string

	// Simple case (no full normalization, no units, etc.), we simply trim out forbidden chars
	metricName = removePromForbiddenRunes(metric.Name())

	// Namespace?
	if namespace != "" {
		return namespace + "_" + metricName
	}

	// Metric name starts with a digit? Prefix it with an underscore
	if metricName != "" && unicode.IsDigit(rune(metricName[0])) {
		metricName = "_" + metricName
	}

	return metricName
}

func (e *metricsExporter) exportNumberDataPoint(pt pmetric.NumberDataPoint,
	resource pcommon.Resource, metric pmetric.Metric,
	samples *[]Sample, timeSeries *[]TimeSerie,
) error {
	metricName := buildPromCompliantName(metric, e.namespace)
	labelSet := buildLabelSet(resource, pt.Attributes(), model.MetricNameLabel, metricName)
	fingerprint := labelSet.Fingerprint()
	sample := Sample{
		TimestampNs: pt.Timestamp().AsTime().UnixNano(),
		Fingerprint: uint64(fingerprint),
		String:      string(labelSet[model.LabelName(model.MetricNameLabel)]),
	}
	switch pt.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		sample.Value = float64(pt.IntValue())
	case pmetric.NumberDataPointValueTypeDouble:
		sample.Value = pt.DoubleValue()
	}
	if pt.Flags().NoRecordedValue() {
		sample.Value = math.Float64frombits(value.StaleNaN)
	}

	*samples = append(*samples, sample)

	labelsJSON, err := json.Marshal(labelSet)
	if err != nil {
		return fmt.Errorf("marshal label set error: %w", err)
	}
	timeSerie := &TimeSerie{
		Fingerprint: uint64(fingerprint),
		Date:        pt.Timestamp().AsTime(),
		Labels:      string(labelsJSON),
		Name:        string(labelSet[model.LabelName(model.MetricNameLabel)]),
	}
	*timeSeries = append(*timeSeries, *timeSerie)
	return nil
}

func (e *metricsExporter) exportNumberDataPoints(dataPoints pmetric.NumberDataPointSlice,
	resource pcommon.Resource, metric pmetric.Metric,
	samples *[]Sample, timeSeries *[]TimeSerie,
) error {
	for x := 0; x < dataPoints.Len(); x++ {
		pt := dataPoints.At(x)
		err := e.exportNumberDataPoint(pt, resource, metric, samples, timeSeries)
		if err != nil {
			return err
		}
	}
	return nil
}
func (e *metricsExporter) exportSummaryDataPoints(dataPoints pmetric.SummaryDataPointSlice,
	resource pcommon.Resource, metric pmetric.Metric,
	samples *[]Sample, timeSeries *[]TimeSerie,
) error {
	for x := 0; x < dataPoints.Len(); x++ {
		if err := e.exportSummaryDataPoint(dataPoints.At(x), resource, metric, samples, timeSeries); err != nil {
			return fmt.Errorf("export SummaryDataPoint error: %w", err)
		}
	}
	return nil
}

func (e *metricsExporter) exportSummaryDataPoint(pt pmetric.SummaryDataPoint,
	resource pcommon.Resource, metric pmetric.Metric,
	samples *[]Sample, timeSeries *[]TimeSerie,
) error {
	baseName := buildPromCompliantName(metric, e.namespace)
	timestampNs := pt.Timestamp().AsTime().UnixNano()

	// treat sum as a sample in an individual TimeSeries
	sumLabels := buildLabelSet(resource, pt.Attributes(), model.MetricNameLabel, baseName+sumSuffix)
	labelsJSON, err := json.Marshal(sumLabels)
	if err != nil {
		return fmt.Errorf("marshal label set err: %w", err)
	}
	fingerprint := sumLabels.Fingerprint()
	timeSerie := TimeSerie{
		Fingerprint: uint64(fingerprint),
		Date:        pt.Timestamp().AsTime(),
		Labels:      string(labelsJSON),
		Name:        string(sumLabels[model.LabelName(model.MetricNameLabel)]),
	}
	*timeSeries = append(*timeSeries, timeSerie)
	sum := Sample{
		Fingerprint: uint64(fingerprint),
		Value:       pt.Sum(),
		TimestampNs: timestampNs,
		String:      string(sumLabels[model.LabelName(model.MetricNameLabel)]),
	}
	if pt.Flags().NoRecordedValue() {
		sum.Value = math.Float64frombits(value.StaleNaN)
	}
	*samples = append(*samples, sum)

	// treat count as a sample in an individual TimeSeries
	countLabels := buildLabelSet(resource, pt.Attributes(), model.MetricNameLabel, baseName+countSuffix)
	labelsJSON, err = json.Marshal(countLabels)
	if err != nil {
		return fmt.Errorf("marshal label set error: %w", err)
	}
	fingerprint = countLabels.Fingerprint()
	timeSerie = TimeSerie{
		Fingerprint: uint64(fingerprint),
		Date:        pt.Timestamp().AsTime(),
		Labels:      string(labelsJSON),
		Name:        string(countLabels[model.LabelName(model.MetricNameLabel)]),
	}
	*timeSeries = append(*timeSeries, timeSerie)
	count := Sample{
		Fingerprint: uint64(fingerprint),
		Value:       pt.Sum(),
		TimestampNs: timestampNs,
		String:      string(countLabels[model.LabelName(model.MetricNameLabel)]),
	}
	if pt.Flags().NoRecordedValue() {
		count.Value = math.Float64frombits(value.StaleNaN)
	}
	*samples = append(*samples, count)

	// process each percentile/quantile
	for i := 0; i < pt.QuantileValues().Len(); i++ {
		qt := pt.QuantileValues().At(i)
		percentileStr := strconv.FormatFloat(qt.Quantile(), 'f', -1, 64)
		qtlabels := buildLabelSet(resource, pt.Attributes(), model.MetricNameLabel, baseName, quantileStr, percentileStr)
		fingerprint = qtlabels.Fingerprint()
		labelsJSON, err = json.Marshal(countLabels)
		if err != nil {
			return fmt.Errorf("marshal label set error: %w", err)
		}
		timeSerie = TimeSerie{
			Fingerprint: uint64(fingerprint),
			Date:        pt.Timestamp().AsTime(),
			Labels:      string(labelsJSON),
			Name:        string(qtlabels[model.LabelName(model.MetricNameLabel)]),
		}
		*timeSeries = append(*timeSeries, timeSerie)
		quantile := Sample{
			Fingerprint: uint64(fingerprint),
			Value:       qt.Value(),
			TimestampNs: timestampNs,
			String:      string(countLabels[model.LabelName(model.MetricNameLabel)]),
		}
		if pt.Flags().NoRecordedValue() {
			quantile.Value = math.Float64frombits(value.StaleNaN)
		}
		*samples = append(*samples, quantile)
	}
	return nil
}

func (e *metricsExporter) exportHistogramDataPoints(dataPoints pmetric.HistogramDataPointSlice,
	resource pcommon.Resource, metric pmetric.Metric,
	samples *[]Sample, timeSeries *[]TimeSerie,
) error {
	for x := 0; x < dataPoints.Len(); x++ {
		if err := e.exportHistogramDataPoint(dataPoints.At(x), resource, metric, samples, timeSeries); err != nil {
			return fmt.Errorf("export HistogramDataPoint error: %w", err)
		}
	}
	return nil
}

func (e *metricsExporter) exportHistogramDataPoint(pt pmetric.HistogramDataPoint,
	resource pcommon.Resource, metric pmetric.Metric,
	samples *[]Sample, timeSeries *[]TimeSerie,
) error {
	baseName := buildPromCompliantName(metric, e.namespace)
	timestampNs := pt.Timestamp().AsTime().UnixNano()
	// add sum count labels
	if pt.HasSum() {
		sumLabels := buildLabelSet(resource, pt.Attributes(), model.MetricNameLabel, baseName+sumSuffix)
		labelsJSON, err := json.Marshal(sumLabels)
		if err != nil {
			return fmt.Errorf("marshal label set error: %w", err)
		}
		fingerprint := sumLabels.Fingerprint()
		timeSerie := TimeSerie{
			Fingerprint: uint64(fingerprint),
			Date:        pt.Timestamp().AsTime(),
			Labels:      string(labelsJSON),
			Name:        string(sumLabels[model.LabelName(model.MetricNameLabel)]),
		}
		*timeSeries = append(*timeSeries, timeSerie)
		sample := Sample{
			Fingerprint: uint64(fingerprint),
			Value:       pt.Sum(),
			TimestampNs: timestampNs,
			String:      string(sumLabels[model.LabelName(model.MetricNameLabel)]),
		}
		*samples = append(*samples, sample)
	}

	// add count labels sample
	countLabels := buildLabelSet(resource, pt.Attributes(), model.MetricNameLabel, baseName+countSuffix)
	fingerprint := countLabels.Fingerprint()
	count := Sample{
		Fingerprint: uint64(countLabels.Fingerprint()),
		Value:       float64(pt.Count()),
		TimestampNs: pt.Timestamp().AsTime().UnixNano(),
		String:      string(countLabels[model.LabelName(model.MetricNameLabel)]),
	}
	*samples = append(*samples, count)
	labelsJSON, err := json.Marshal(countLabels)
	if err != nil {
		return fmt.Errorf("marshal label set error: %w", err)
	}
	timeSerie := TimeSerie{
		Fingerprint: uint64(fingerprint),
		Date:        pt.Timestamp().AsTime(),
		Labels:      string(labelsJSON),
		Name:        string(countLabels[model.LabelName(model.MetricNameLabel)]),
	}
	*timeSeries = append(*timeSeries, timeSerie)
	// cumulative count for conversion to cumulative histogram
	var cumulativeCount uint64
	// process each bound, based on histograms proto definition, # of buckets = # of explicit bounds + 1
	for i := 0; i < pt.ExplicitBounds().Len() && i < pt.BucketCounts().Len(); i++ {
		bound := pt.ExplicitBounds().At(i)
		boundStr := strconv.FormatFloat(bound, 'f', -1, 64)
		cumulativeCount += pt.BucketCounts().At(i)
		bucketLabels := buildLabelSet(resource, pt.Attributes(), model.MetricNameLabel, baseName+bucketSuffix, "le", boundStr)
		fingerprint := bucketLabels.Fingerprint()
		bucket := Sample{
			Fingerprint: uint64(fingerprint),
			Value:       float64(cumulativeCount),
			TimestampNs: timestampNs,
			String:      string(bucketLabels[model.LabelName(model.MetricNameLabel)]),
		}
		*samples = append(*samples, bucket)
		if pt.Flags().NoRecordedValue() {
			bucket.Value = math.Float64frombits(value.StaleNaN)
		}
		timeSerie := TimeSerie{
			Fingerprint: uint64(fingerprint),
			Date:        pt.Timestamp().AsTime(),
			Labels:      string(labelsJSON),
			Name:        string(countLabels[model.LabelName(model.MetricNameLabel)]),
		}
		*timeSeries = append(*timeSeries, timeSerie)
	}
	// add le=+Inf bucket
	bucketLabels := buildLabelSet(resource, pt.Attributes(), model.MetricNameLabel, baseName+bucketSuffix, "le", "+Inf")
	fingerprint = bucketLabels.Fingerprint()
	infBucket := &Sample{
		Fingerprint: uint64(fingerprint),
		TimestampNs: timestampNs,
		String:      string(bucketLabels[model.LabelName(model.MetricNameLabel)]),
	}
	if pt.Flags().NoRecordedValue() {
		infBucket.Value = math.Float64frombits(value.StaleNaN)
	} else {
		infBucket.Value = float64(pt.Count())
	}
	*samples = append(*samples, *infBucket)
	labelsJSON, err = json.Marshal(bucketLabels)
	if err != nil {
		return fmt.Errorf("marshal label set error: %w", err)
	}
	timeSerie = TimeSerie{
		Fingerprint: uint64(fingerprint),
		Date:        pt.Timestamp().AsTime(),
		Labels:      string(labelsJSON),
		Name:        string(bucketLabels[model.LabelName(model.MetricNameLabel)]),
	}
	*timeSeries = append(*timeSeries, timeSerie)
	return nil
}

func (e *metricsExporter) collectFromMetrics(metrics pmetric.MetricSlice, resource pcommon.Resource, samples *[]Sample, timeSeries *[]TimeSerie) error {
	for k := 0; k < metrics.Len(); k++ {
		if err := e.collectFromMetric(metrics.At(k), resource, samples, timeSeries); err != nil {
			return err
		}
	}
	return nil
}

func (e *metricsExporter) collectFromMetric(metric pmetric.Metric, resource pcommon.Resource, samples *[]Sample, timeSeries *[]TimeSerie) error {
	if !isValidAggregationTemporality(metric) {
		return nil
	}
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dataPoints := metric.Gauge().DataPoints()
		if err := e.exportNumberDataPoints(dataPoints, resource, metric, samples, timeSeries); err != nil {
			return fmt.Errorf("export NumberDataPoints error: %w", err)
		}
	case pmetric.MetricTypeSum:
		dataPoints := metric.Sum().DataPoints()
		if err := e.exportNumberDataPoints(dataPoints, resource, metric, samples, timeSeries); err != nil {
			return fmt.Errorf("export NumberDataPoints error: %w", err)
		}
	case pmetric.MetricTypeHistogram:
		dataPoints := metric.Histogram().DataPoints()
		if err := e.exportHistogramDataPoints(dataPoints, resource, metric, samples, timeSeries); err != nil {
			return fmt.Errorf("export HistogramDataPoint error: %w", err)
		}
	case pmetric.MetricTypeSummary:
		dataPoints := metric.Summary().DataPoints()
		if err := e.exportSummaryDataPoints(dataPoints, resource, metric, samples, timeSeries); err != nil {
			return fmt.Errorf("export SummaryDataPoints error: %w", err)
		}
	}
	return nil
}

func (e *metricsExporter) pushMetricsData(ctx context.Context, md pmetric.Metrics) error {

	// for collect samples and timeSeries
	var (
		samples    []Sample
		timeSeries []TimeSerie
	)

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetrics := md.ResourceMetrics().At(i)
		resource := resourceMetrics.Resource()
		for j := 0; j < resourceMetrics.ScopeMetrics().Len(); j++ {
			metrics := resourceMetrics.ScopeMetrics().At(j).Metrics()
			err := e.collectFromMetrics(metrics, resource, &samples, &timeSeries)
			if err != nil {
				return err
			}
		}
	}

	return batchSamplesAndTimeSeries(ctx, e.db, samples, timeSeries)
}

// isValidAggregationTemporality checks whether an OTel metric has a valid
// aggregation temporality for conversion to a Prometheus metric.
func isValidAggregationTemporality(metric pmetric.Metric) bool {
	switch metric.Type() {
	case pmetric.MetricTypeGauge, pmetric.MetricTypeSummary:
		return true
	case pmetric.MetricTypeSum:
		return metric.Sum().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	case pmetric.MetricTypeHistogram:
		return metric.Histogram().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	case pmetric.MetricTypeExponentialHistogram:
		return metric.ExponentialHistogram().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	}
	return false
}
