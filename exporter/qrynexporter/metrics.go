package qrynexporter

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"

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
	db *sql.DB

	logger *zap.Logger
	cfg    *Config
}

func newMetricsExporter(logger *zap.Logger, cfg *Config) (*metricsExporter, error) {
	db, err := sql.Open("clickhouse", cfg.DSN)
	if err != nil {
		return nil, err
	}
	return &metricsExporter{
		db:     db,
		logger: logger,
		cfg:    cfg,
	}, nil
}

// Shutdown will shutdown the exporter.
func (e *metricsExporter) Shutdown(_ context.Context) error {
	if e.db != nil {
		return e.db.Close()
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

func buildLabels(resource pcommon.Resource, attributes pcommon.Map, extras ...string) model.LabelSet {
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
func convertNumberDataPoint(ctx context.Context, pt pmetric.NumberDataPoint,
	resource pcommon.Resource, metric pmetric.Metric,
	sampleStatement, timeSerieStatement *sql.Stmt,
) error {
	metricName := removePromForbiddenRunes(metric.Name())
	labelSet := buildLabels(resource, resource.Attributes(), model.MetricNameLabel, metricName)
	fingerprint := labelSet.Fingerprint()
	sample := Sample{
		TimestampNs: pt.Timestamp().AsTime().UnixNano(),
		Fingerprint: uint64(fingerprint),
		String:      string(labelSet[model.LabelName(model.MetricNameLabel)]),
	}
	if _, err := sampleStatement.ExecContext(ctx, sample.Fingerprint,
		sample.TimestampNs,
		sample.Value,
		sample.String,
	); err != nil {
		return err
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

	labelsJSON, err := json.Marshal(labelSet)
	if err != nil {
		return fmt.Errorf("marshal label set error: %w", err)
	}
	timeSerie := &TimeSerie{
		Fingerprint: uint64(fingerprint),
		Date:        pt.Timestamp().AsTime().Format("2006-01-02"),
		Labels:      string(labelsJSON),
		Name:        string(labelSet[model.LabelName(model.MetricNameLabel)]),
	}
	if _, err := timeSerieStatement.ExecContext(ctx,
		timeSerie.Date,
		timeSerie.Fingerprint,
		timeSerie.Labels,
		timeSerie.Name,
	); err != nil {
		return err
	}
	return nil
}

func exportNumberDataPoints(ctx context.Context, dataPoints pmetric.NumberDataPointSlice,
	resource pcommon.Resource, metric pmetric.Metric,
	sampleStatement, timeSerieStatement *sql.Stmt,
) error {
	if dataPoints.Len() == 0 {
		return fmt.Errorf("empty data points. %s is dropped", metric.Name())
	}
	for x := 0; x < dataPoints.Len(); x++ {
		pt := dataPoints.At(x)
		err := convertNumberDataPoint(ctx, pt, resource, metric, sampleStatement, timeSerieStatement)
		if err != nil {
			return err
		}
	}
	return nil
}

func exportSummaryDataPoint(ctx context.Context, pt pmetric.SummaryDataPoint,
	resource pcommon.Resource, metric pmetric.Metric,
	sampleStatement, timeSerieStatement *sql.Stmt,
) error {
	baseName := normalizeLabel(metric.Name())
	timestampNs := pt.Timestamp().AsTime().UnixNano()

	// treat sum as a sample in an individual TimeSeries
	sumLabels := buildLabels(resource, resource.Attributes(), model.MetricNameLabel, baseName+sumSuffix)
	labelsJSON, err := json.Marshal(sumLabels)
	if err != nil {
		return fmt.Errorf("marshal label set err: %w", err)
	}
	fingerprint := sumLabels.Fingerprint()
	timeSerie := TimeSerie{
		Fingerprint: uint64(fingerprint),
		Date:        pt.Timestamp().AsTime().Format("2006-01-02"),
		Labels:      string(labelsJSON),
		Name:        string(sumLabels[model.LabelName(model.MetricNameLabel)]),
	}
	if _, err := timeSerieStatement.ExecContext(ctx, timeSerie.Date, timeSerie.Fingerprint, timeSerie.Labels, timeSerie.Name); err != nil {
		return err
	}
	sum := Sample{
		Fingerprint: uint64(fingerprint),
		Value:       pt.Sum(),
		TimestampNs: timestampNs,
		String:      string(sumLabels[model.LabelName(model.MetricNameLabel)]),
	}
	if pt.Flags().NoRecordedValue() {
		sum.Value = math.Float64frombits(value.StaleNaN)
	}
	if _, err := sampleStatement.ExecContext(ctx, sum.Fingerprint, sum.TimestampNs, sum.Value, sum.String); err != nil {
		return err
	}

	// treat count as a sample in an individual TimeSeries
	countLabels := buildLabels(resource, resource.Attributes(), model.MetricNameLabel, baseName+countSuffix)
	labelsJSON, err = json.Marshal(countLabels)
	if err != nil {
		return fmt.Errorf("marshal label set error: %w", err)
	}
	fingerprint = countLabels.Fingerprint()
	timeSerie = TimeSerie{
		Fingerprint: uint64(fingerprint),
		Date:        pt.Timestamp().AsTime().Format("2006-01-02"),
		Labels:      string(labelsJSON),
		Name:        string(countLabels[model.LabelName(model.MetricNameLabel)]),
	}
	if _, err := timeSerieStatement.ExecContext(ctx, timeSerie.Date, timeSerie.Fingerprint, timeSerie.Labels, timeSerie.Name); err != nil {
		return err
	}
	count := Sample{
		Fingerprint: uint64(fingerprint),
		Value:       pt.Sum(),
		TimestampNs: timestampNs,
		String:      string(countLabels[model.LabelName(model.MetricNameLabel)]),
	}
	if pt.Flags().NoRecordedValue() {
		count.Value = math.Float64frombits(value.StaleNaN)
	}
	if _, err := sampleStatement.ExecContext(ctx, count.Fingerprint, count.TimestampNs, count.Value, count.String); err != nil {
		return err
	}

	// process each percentile/quantile
	for i := 0; i < pt.QuantileValues().Len(); i++ {
		qt := pt.QuantileValues().At(i)
		percentileStr := strconv.FormatFloat(qt.Quantile(), 'f', -1, 64)
		qtlabels := buildLabels(resource, pt.Attributes(), model.MetricNameLabel, baseName, quantileStr, percentileStr)
		fingerprint = qtlabels.Fingerprint()
		labelsJSON, err = json.Marshal(countLabels)
		if err != nil {
			return fmt.Errorf("marshal label set error: %w", err)
		}
		timeSerie = TimeSerie{
			Fingerprint: uint64(fingerprint),
			Date:        pt.Timestamp().AsTime().Format("2006-01-02"),
			Labels:      string(labelsJSON),
			Name:        string(qtlabels[model.LabelName(model.MetricNameLabel)]),
		}
		if _, err := timeSerieStatement.ExecContext(ctx, timeSerie.Date, timeSerie.Fingerprint, timeSerie.Labels, timeSerie.Name); err != nil {
			return err
		}
		quantile := Sample{
			Fingerprint: uint64(fingerprint),
			Value:       qt.Value(),
			TimestampNs: timestampNs,
			String:      string(countLabels[model.LabelName(model.MetricNameLabel)]),
		}
		if pt.Flags().NoRecordedValue() {
			quantile.Value = math.Float64frombits(value.StaleNaN)
		}
		if _, err := sampleStatement.ExecContext(ctx, quantile.Fingerprint, quantile.TimestampNs, quantile.Value, quantile.String); err != nil {
			return err
		}
	}
	return nil
}

func exportHistogramDataPoint(ctx context.Context, pt pmetric.HistogramDataPoint,
	resource pcommon.Resource, metric pmetric.Metric,
	sampleStatement, timeSerieStatement *sql.Stmt,
) error {
	baseName := normalizeLabel(metric.Name())
	timestampNs := pt.Timestamp().AsTime().UnixNano()
	// add sum count labels
	if pt.HasSum() {
		sumLabels := buildLabels(resource, resource.Attributes(), model.MetricNameLabel, baseName+sumSuffix)
		labelsJSON, err := json.Marshal(sumLabels)
		if err != nil {
			return fmt.Errorf("marshal label set error: %w", err)
		}
		fingerprint := sumLabels.Fingerprint()
		timeSerie := TimeSerie{
			Fingerprint: uint64(fingerprint),
			Date:        pt.Timestamp().AsTime().Format("2006-01-02"),
			Labels:      string(labelsJSON),
			Name:        string(sumLabels[model.LabelName(model.MetricNameLabel)]),
		}
		if _, err := timeSerieStatement.ExecContext(ctx, timeSerie.Date, timeSerie.Fingerprint, timeSerie.Labels, timeSerie.Name); err != nil {
			return err
		}
		sample := Sample{
			Fingerprint: uint64(fingerprint),
			Value:       pt.Sum(),
			TimestampNs: timestampNs,
			String:      string(sumLabels[model.LabelName(model.MetricNameLabel)]),
		}
		if _, err := sampleStatement.ExecContext(ctx, sample.Fingerprint, sample.TimestampNs, sample.Value, sample.String); err != nil {
			return err
		}
	}

	// add count labels sample
	countLabels := buildLabels(resource, resource.Attributes(), model.MetricNameLabel, baseName+countSuffix)
	fingerprint := countLabels.Fingerprint()
	count := Sample{
		Fingerprint: uint64(countLabels.Fingerprint()),
		Value:       float64(pt.Count()),
		TimestampNs: pt.Timestamp().AsTime().UnixNano(),
		String:      string(countLabels[model.LabelName(model.MetricNameLabel)]),
	}
	if _, err := sampleStatement.ExecContext(ctx, count.Fingerprint, count.TimestampNs, count.Value, count.String); err != nil {
		return err
	}
	labelsJSON, err := json.Marshal(countLabels)
	if err != nil {
		return fmt.Errorf("marshal label set error: %w", err)
	}
	timeSerie := TimeSerie{
		Fingerprint: uint64(fingerprint),
		Date:        pt.Timestamp().AsTime().Format("2006-01-02"),
		Labels:      string(labelsJSON),
		Name:        string(countLabels[model.LabelName(model.MetricNameLabel)]),
	}
	if _, err := timeSerieStatement.ExecContext(ctx, timeSerie.Date, timeSerie.Fingerprint, timeSerie.Labels, timeSerie.Name); err != nil {
		return err
	}
	// cumulative count for conversion to cumulative histogram
	var cumulativeCount uint64
	// process each bound, based on histograms proto definition, # of buckets = # of explicit bounds + 1
	for i := 0; i < pt.ExplicitBounds().Len() && i < pt.BucketCounts().Len(); i++ {
		bound := pt.ExplicitBounds().At(i)
		boundStr := strconv.FormatFloat(bound, 'f', -1, 64)
		cumulativeCount += pt.BucketCounts().At(i)
		bucketLabels := buildLabels(resource, resource.Attributes(), model.MetricNameLabel, baseName+bucketSuffix, "le", boundStr)
		fingerprint := bucketLabels.Fingerprint()
		bucket := &Sample{
			Fingerprint: uint64(fingerprint),
			Value:       float64(cumulativeCount),
			TimestampNs: timestampNs,
			String:      string(bucketLabels[model.LabelName(model.MetricNameLabel)]),
		}
		if _, err := sampleStatement.ExecContext(ctx, bucket.Fingerprint, bucket.TimestampNs, bucket.Value, bucket.String); err != nil {
			return err
		}
		if pt.Flags().NoRecordedValue() {
			bucket.Value = math.Float64frombits(value.StaleNaN)
		}
		timeSerie := TimeSerie{
			Fingerprint: uint64(fingerprint),
			Date:        pt.Timestamp().AsTime().Format("2006-01-02"),
			Labels:      string(labelsJSON),
			Name:        string(countLabels[model.LabelName(model.MetricNameLabel)]),
		}
		if _, err := timeSerieStatement.ExecContext(ctx, timeSerie.Date, timeSerie.Fingerprint, timeSerie.Labels, timeSerie.Name); err != nil {
			return err
		}
	}
	// add le=+Inf bucket
	bucketLabels := buildLabels(resource, resource.Attributes(), model.MetricNameLabel, baseName+bucketSuffix, "le", "+Inf")
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
	if _, err := sampleStatement.ExecContext(ctx, infBucket.Fingerprint, infBucket.TimestampNs, infBucket.Value, infBucket.String); err != nil {
		return err
	}
	labelsJSON, err = json.Marshal(bucketLabels)
	if err != nil {
		return fmt.Errorf("marshal label set error: %w", err)
	}
	timeSerie = TimeSerie{
		Fingerprint: uint64(fingerprint),
		Date:        pt.Timestamp().AsTime().Format("2006-01-02"),
		Labels:      string(labelsJSON),
		Name:        string(bucketLabels[model.LabelName(model.MetricNameLabel)]),
	}
	if _, err := timeSerieStatement.ExecContext(ctx, timeSerie.Date, timeSerie.Fingerprint, timeSerie.Labels, timeSerie.Name); err != nil {
		return err
	}
	return nil
}

func (e *metricsExporter) pushMetricsData(ctx context.Context, md pmetric.Metrics) error {
	return Transaction(ctx, e.db, func(tx *sql.Tx) error {
		sampleStatement, err := tx.PrepareContext(ctx, samplesInsertSQL)
		if err != nil {
			return fmt.Errorf("PrepareContext sampleStatement error: %w", err)
		}
		defer sampleStatement.Close()
		timeSerieStatement, err := tx.PrepareContext(ctx, timeSeriesInsertSQL)
		if err != nil {
			return fmt.Errorf("PrepareContext timeSeriesInsertSQL error: %w", err)
		}
		defer sampleStatement.Close()
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
						err = exportNumberDataPoints(ctx, dataPoints, resource, metric, sampleStatement, timeSerieStatement)
						if err != nil {
							return fmt.Errorf("export NumberDataPointSlice error: %w", err)
						}
					case pmetric.MetricTypeSum:
						dataPoints := metric.Sum().DataPoints()
						err = exportNumberDataPoints(ctx, dataPoints, resource, metric, sampleStatement, timeSerieStatement)
						if err != nil {
							return fmt.Errorf("export NumberDataPointSlice error: %w", err)
						}
					case pmetric.MetricTypeHistogram:
						dataPoints := metric.Histogram().DataPoints()
						if dataPoints.Len() == 0 {
							return fmt.Errorf("empty data points. %s is dropped", metric.Name())
						}
						for x := 0; x < dataPoints.Len(); x++ {
							err = exportHistogramDataPoint(ctx, dataPoints.At(x), resource, metric, sampleStatement, timeSerieStatement)
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
							err = exportSummaryDataPoint(ctx, dataPoints.At(x), resource, metric, sampleStatement, timeSerieStatement)
							if err != nil {
								return fmt.Errorf("export SummaryDataPoint error: %w", err)
							}
						}
					}
				}
			}
		}
		return nil
	})
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
