package qrynexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"

	"github.com/ClickHouse/ch-go"
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
	client *ch.Client

	logger *zap.Logger
}

func newMetricsExporter(ctx context.Context, logger *zap.Logger, cfg *Config) (*metricsExporter, error) {
	opts, err := parseDSN(cfg.DSN)
	if err != nil {
		return nil, err
	}
	client, err := ch.Dial(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &metricsExporter{
		client: client,
		logger: logger,
	}, nil
}

// Shutdown will shutdown the exporter.
func (e *metricsExporter) Shutdown(_ context.Context) error {
	if e.client != nil {
		return e.client.Close()
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
	out := defaultExporterLabels
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

func exportNumberDataPoint(pt pmetric.NumberDataPoint,
	resource pcommon.Resource, metric pmetric.Metric,
	samples *[]Sample, timeSeries *[]TimeSerie,
) error {
	metricName := removePromForbiddenRunes(metric.Name())
	labelSet := buildLabelSet(resource, resource.Attributes(), model.MetricNameLabel, metricName)
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

func exportNumberDataPoints(dataPoints pmetric.NumberDataPointSlice,
	resource pcommon.Resource, metric pmetric.Metric,
	samples *[]Sample, timeSeries *[]TimeSerie,
) error {
	for x := 0; x < dataPoints.Len(); x++ {
		pt := dataPoints.At(x)
		err := exportNumberDataPoint(pt, resource, metric, samples, timeSeries)
		if err != nil {
			return err
		}
	}
	return nil
}

func exportSummaryDataPoint(pt pmetric.SummaryDataPoint,
	resource pcommon.Resource, metric pmetric.Metric,
	samples *[]Sample, timeSeries *[]TimeSerie,
) error {
	baseName := normalizeLabel(metric.Name())
	timestampNs := pt.Timestamp().AsTime().UnixNano()

	// treat sum as a sample in an individual TimeSeries
	sumLabels := buildLabelSet(resource, resource.Attributes(), model.MetricNameLabel, baseName+sumSuffix)
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
	countLabels := buildLabelSet(resource, resource.Attributes(), model.MetricNameLabel, baseName+countSuffix)
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

func exportHistogramDataPoint(pt pmetric.HistogramDataPoint,
	resource pcommon.Resource, metric pmetric.Metric,
	samples *[]Sample, timeSeries *[]TimeSerie,
) error {
	baseName := normalizeLabel(metric.Name())
	timestampNs := pt.Timestamp().AsTime().UnixNano()
	// add sum count labels
	if pt.HasSum() {
		sumLabels := buildLabelSet(resource, resource.Attributes(), model.MetricNameLabel, baseName+sumSuffix)
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
	countLabels := buildLabelSet(resource, resource.Attributes(), model.MetricNameLabel, baseName+countSuffix)
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
		bucketLabels := buildLabelSet(resource, resource.Attributes(), model.MetricNameLabel, baseName+bucketSuffix, "le", boundStr)
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
	bucketLabels := buildLabelSet(resource, resource.Attributes(), model.MetricNameLabel, baseName+bucketSuffix, "le", "+Inf")
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

func (e *metricsExporter) pushMetricsData(ctx context.Context, md pmetric.Metrics) error {
	sampleSchema := new(SampleSchema)
	samplesInput := newSamplesInput(sampleSchema)

	timeSerieSchema := new(TimeSerieSchema)
	timeSeriesInput := newTimeSeriesInput(timeSerieSchema)
	var (
		samples    []Sample
		timeSeries []TimeSerie
		err        error
	)

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetrics := md.ResourceMetrics().At(i)
		resource := resourceMetrics.Resource()
		for j := 0; j < resourceMetrics.ScopeMetrics().Len(); j++ {
			metrics := resourceMetrics.ScopeMetrics().At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				if !isValidAggregationTemporality(metric) {
					continue
				}
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					dataPoints := metric.Gauge().DataPoints()
					err = exportNumberDataPoints(dataPoints, resource, metric, &samples, &timeSeries)
					if err != nil {
						return fmt.Errorf("export NumberDataPointSlice error: %w", err)
					}
				case pmetric.MetricTypeSum:
					dataPoints := metric.Sum().DataPoints()
					err = exportNumberDataPoints(dataPoints, resource, metric, &samples, &timeSeries)
					if err != nil {
						return fmt.Errorf("export NumberDataPointSlice error: %w", err)
					}
				case pmetric.MetricTypeHistogram:
					dataPoints := metric.Histogram().DataPoints()
					for x := 0; x < dataPoints.Len(); x++ {
						err = exportHistogramDataPoint(dataPoints.At(x), resource, metric, &samples, &timeSeries)
						if err != nil {
							return fmt.Errorf("export NumberDataPointSlice error: %w", err)
						}
					}
				case pmetric.MetricTypeSummary:
					dataPoints := metric.Summary().DataPoints()
					if dataPoints.Len() == 0 {
						return fmt.Errorf("empty data points. %s is dropped", metric.Name())
					}
					for x := 0; x < dataPoints.Len(); x++ {
						err = exportSummaryDataPoint(dataPoints.At(x), resource, metric, &samples, &timeSeries)
						if err != nil {
							return fmt.Errorf("export SummaryDataPoint error: %w", err)
						}
					}
				}
			}
		}
	}
	if err := e.client.Do(ctx, ch.Query{
		Body:  samplesInput.Into("samples_v3"),
		Input: samplesInput,
		OnInput: func(_ context.Context) error {
			samplesInput.Reset()
			for _, sample := range samples {
				appendSample(sampleSchema, sample)
			}
			return nil
		},
	}); err != nil {
		return err
	}

	if err := e.client.Do(ctx, ch.Query{
		Body:  timeSeriesInput.Into("time_series"),
		Input: timeSeriesInput,
		OnInput: func(_ context.Context) error {
			timeSeriesInput.Reset()
			for _, timeSerie := range timeSeries {
				appendTimeSerie(timeSerieSchema, timeSerie)
			}
			return nil
		},
	}); err != nil {
		return err
	}

	return nil
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
