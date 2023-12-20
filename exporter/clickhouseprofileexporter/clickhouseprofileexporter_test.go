package clickhouseprofileexporter

// TODO: fix and complete this tc based integration test, something seems wrong with the docker wiring

// import (
// 	"context"
// 	"testing"

// 	"github.com/stretchr/testify/require"
// 	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
// 	"go.opentelemetry.io/collector/pdata/plog"
// )

// type chtest struct {
// 	name  string
// 	batch plog.Logs
// 	// expected
// }

// func startContainer(t *testing.T) string {
// 	ctx := context.Background()

// 	tc, err := clickhouse.RunContainer(ctx) // clickhouse.WithInitScripts(filepath.Join("testdata", "init_db.sh")),

// 	if err != nil {
// 		panic(err)
// 	}

// 	t.Cleanup(func() { require.NoError(t, tc.Terminate(ctx)) })

// 	dsn, err := tc.ConnectionString(ctx)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return dsn
// }

// func TestExportProfileBatch(t *testing.T) {
// startContainer(t)
// cfg := &Config{
// 	TimeoutSettings: exporterhelper.NewDefaultTimeoutSettings(),
// 	QueueSettings:   QueueSettings{QueueSize: exporterhelper.NewDefaultQueueSettings().QueueSize},
// 	RetrySettings:   exporterhelper.NewDefaultRetrySettings(),
// 	Dsn:             dsn,
// }
// set := exportertest.NewNopCreateSettings()
// set.Logger = zap.Must(zap.NewDevelopment())
// exp, err := newClickhouseProfileExporter(context.Background(), &set, cfg)
// if err != nil {
// 	panic(err)
// }

// t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) })

// tests := []chtest{{
// 	name: "send profile batch to clickhouse server",
// 	batch: ,
// 	expected: ,
// }}

// for _, tt := range tests {
// 	t.Run(tt.name, func(t *testing.T) {
// 		assert.NoError(t, send(context.Background()), "send shouldn't have been failed")
// 		actual :=
// 		assert.NoError(t, plogtest.CompareLogs(tt.expected, actual[0]))
// 		truncate table
// 	})
// }
// }
