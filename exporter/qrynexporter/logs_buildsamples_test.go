package qrynexporter

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
)

// makeBenchLogs builds an OTLP batch with a single resource (resAttrs attributes)
// and `records` log records, mimicking a promtail/k8s workload.
func makeBenchLogs(records, resAttrs int, body string) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	res := rl.Resource()
	for i := 0; i < resAttrs; i++ {
		res.Attributes().PutStr(fmt.Sprintf("k8s.resource.%d", i), fmt.Sprintf("value-%d", i))
	}
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < records; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.Body().SetStr(body)
		lr.Attributes().PutStr("k8s.event.reason", "Started")
	}
	return ld
}

// Raw format (the default) must not read the resource; the line is just the body.
func TestBuildSamplesAndTimeSeries_RawUsesBody(t *testing.T) {
	e := &logsExporter{} // format "" -> raw
	ld := makeBenchLogs(3, 5, "hello world")

	samples, timeSeries, err := e.buildSamplesAndTimeSeries(ld)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 3 || len(timeSeries) != 3 {
		t.Fatalf("expected 3 samples/timeseries, got %d/%d", len(samples), len(timeSeries))
	}
	for _, s := range samples {
		if s.String != "hello world" {
			t.Fatalf("raw sample should equal body, got %q", s.String)
		}
	}
}

// Logfmt renders resource attributes (the path that keeps the per-record resource
// copy). Under the default policy resource attrs are promoted to labels and pruned
// from the line, so we opt out of resource promotion with the loki.resource.labels
// hint: the named attr becomes a label (pruned), the unnamed one stays in the line.
func TestBuildSamplesAndTimeSeries_LogfmtIncludesUnpromotedResource(t *testing.T) {
	e := &logsExporter{format: formatLogfmt}

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(hintResources, "cluster") // promote only "cluster"
	rl.Resource().Attributes().PutStr("cluster", "prod")
	rl.Resource().Attributes().PutStr("region", "eu")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetSeverityNumber(plog.SeverityNumberInfo)
	lr.Body().SetStr("msg=hi")

	samples, _, err := e.buildSamplesAndTimeSeries(ld)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(samples))
	}
	// region is not promoted -> rendered in the line.
	if !strings.Contains(samples[0].String, "resource_region=eu") {
		t.Fatalf("logfmt line should contain the unpromoted resource attribute, got %q", samples[0].String)
	}
	// cluster is promoted to a label -> pruned from the line.
	if strings.Contains(samples[0].String, "resource_cluster") {
		t.Fatalf("promoted resource attribute should be pruned from the line, got %q", samples[0].String)
	}
}

// Json renders resource attributes in the "resources" field, with label-promoted
// ones pruned first (same contract as logfmt). This guards both the Resources bug
// fix and the resource copy/prune path being applied to the json format.
func TestBuildSamplesAndTimeSeries_JSONResourcesFieldPruned(t *testing.T) {
	e := &logsExporter{format: formatJSON}

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(hintResources, "cluster") // promote only "cluster"
	rl.Resource().Attributes().PutStr("cluster", "prod")
	rl.Resource().Attributes().PutStr("region", "eu")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetSeverityNumber(plog.SeverityNumberInfo)
	lr.Body().SetStr("hi")

	samples, _, err := e.buildSamplesAndTimeSeries(ld)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(samples))
	}

	var rec logRecord
	if err := json.Unmarshal([]byte(samples[0].String), &rec); err != nil {
		t.Fatalf("json sample did not parse: %v (line %q)", err, samples[0].String)
	}
	// resources field reflects the resource bag, not the log attributes.
	if _, ok := rec.Resources["region"]; !ok {
		t.Fatalf("resources field should contain the unpromoted resource attr, got %v", rec.Resources)
	}
	// cluster is promoted to a label -> pruned from the resources field.
	if _, ok := rec.Resources["cluster"]; ok {
		t.Fatalf("promoted resource attr should be pruned from resources field, got %v", rec.Resources)
	}
}

// The input batch must not be mutated (other exporters in the pipeline see it).
func TestBuildSamplesAndTimeSeries_DoesNotMutateInput(t *testing.T) {
	e := &logsExporter{}
	ld := makeBenchLogs(1, 1, "body")

	_, _, err := e.buildSamplesAndTimeSeries(ld)
	if err != nil {
		t.Fatal(err)
	}
	lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if _, ok := lr.Attributes().Get(levelAttributeName); ok {
		t.Fatal("input record was mutated (level attribute leaked back to the source)")
	}
}

func BenchmarkBuildSamplesAndTimeSeries_Raw(b *testing.B) {
	e := &logsExporter{} // raw path: no per-record resource copy
	ld := makeBenchLogs(500, 20, "a fairly typical log line of moderate length")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := e.buildSamplesAndTimeSeries(ld); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuildSamplesAndTimeSeries_Logfmt(b *testing.B) {
	e := &logsExporter{format: formatLogfmt} // copies + prunes the resource per record
	ld := makeBenchLogs(500, 20, "a fairly typical log line of moderate length")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := e.buildSamplesAndTimeSeries(ld); err != nil {
			b.Fatal(err)
		}
	}
}
