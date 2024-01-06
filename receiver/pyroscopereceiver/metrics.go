package pyroscopereceiver

import (
	"fmt"

	"go.opentelemetry.io/otel/metric"
)

const prefix = "receiver_pyroscope_"

var (
	otelcolReceiverPyroscopeHttpRequestTotal                     metric.Int64Counter
	otelcolReceiverPyroscopeReceivedPayloadSizeUncompressedBytes metric.Int64Histogram
	otelcolReceiverPyroscopeParsedPayloadSizeUncompressedBytes   metric.Int64Histogram
	otelcolReceiverPyroscopeHttpResponseTimeMillis               metric.Int64Histogram
)

func initMetrics(meter metric.Meter) error {
	var err error
	if otelcolReceiverPyroscopeHttpRequestTotal, err = meter.Int64Counter(
		fmt.Sprint(prefix, "http_request_total"),
		metric.WithDescription("Pyroscope receiver http request count"),
	); err != nil {
		return err
	}
	if otelcolReceiverPyroscopeReceivedPayloadSizeUncompressedBytes, err = meter.Int64Histogram(
		fmt.Sprint(prefix, "received_payload_size_uncompressed_bytes"),
		metric.WithDescription("Pyroscope receiver received uncompressed payload size in bytes"),
	); err != nil {
		return err
	}
	if otelcolReceiverPyroscopeParsedPayloadSizeUncompressedBytes, err = meter.Int64Histogram(
		fmt.Sprint(prefix, "parsed_payload_size_uncompressed_bytes"),
		metric.WithDescription("Pyroscope receiver parsed uncompressed payload size in bytes"),
	); err != nil {
		return err
	}
	if otelcolReceiverPyroscopeHttpResponseTimeMillis, err = meter.Int64Histogram(
		fmt.Sprint(prefix, "http_response_time_millis"),
		metric.WithDescription("Pyroscope receiver http response time in millis"),
	); err != nil {
		return err
	}
	return nil
}
