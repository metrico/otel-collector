package chstatsreceiver

import (
	"bytes"
	"context"
	"fmt"
	"text/template"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"golang.org/x/sync/errgroup"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type chReceiver struct {
	cfg             *Config
	db              clickhouse.Conn
	metricsConsumer consumer.Metrics
	logsConsumer    consumer.Logs
	templates       []*template.Template
	logger          *zap.Logger
	cancel          context.CancelFunc
}

func (r *chReceiver) Start(ctx context.Context, _ component.Host) error {
	opts, err := clickhouse.ParseDSN(r.cfg.DSN)
	if err != nil {
		return err
	}
	db, err := clickhouse.Open(opts)
	if err != nil {
		return err
	}
	r.db = db
	r.templates = make([]*template.Template, len(r.cfg.Queries))
	for i, query := range r.cfg.Queries {
		r.templates[i], err = template.New(fmt.Sprintf("tpl-%d", i)).Parse(query)
		if err != nil {
			return err
		}
	}

	_ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	go r.mainLoop(_ctx)
	return nil
}

// nextRun returns the first run after now. Runs are in the middle of each
// interval of the wall clock. Close to the edge of an interval, the small delays
// of a run would put two points in one interval and none in the next.
func nextRun(now time.Time, interval time.Duration) time.Time {
	next := now.Truncate(interval).Add(interval / 2)
	if !next.After(now) {
		next = next.Add(interval)
	}
	return next
}

func (r *chReceiver) mainLoop(ctx context.Context) {
	next := nextRun(time.Now(), r.cfg.Timeout)
	for {
		r.logger.Info("tick start")
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			fmt.Println("tick stop")
			return
		case <-timer.C:
			err := r.GetMetrics(ctx, next)
			if err != nil {
				r.logger.Error("failed to get metrics", zap.Error(err))
			}
		}
		r.logger.Info("tick end")
		// The timer can fire slightly before next on the wall clock. Do not run
		// the same interval twice.
		now := time.Now()
		if now.Before(next) {
			now = next
		}
		next = nextRun(now, r.cfg.Timeout)
	}
}

// GetMetrics runs the queries of the interval that tick belongs to.
func (r *chReceiver) GetMetrics(ctx context.Context, tick time.Time) error {
	g := errgroup.Group{}
	for _, tpl := range r.templates {
		_tpl := tpl
		g.Go(func() error {
			switch r.cfg.Type {
			case RCV_TYPE_METRICS:
			case "":
				return r.getMetricsTemplate(ctx, _tpl, tick)
			case RCV_TYPE_LOGS:
				return r.getLogsTemplate(ctx, _tpl, tick)
			}
			return nil
		})
	}
	return g.Wait()
}

func queryParams(tick time.Time) map[string]any {
	return map[string]any{
		"timestamp_ns": tick.UnixNano(),
		"timestamp_ms": tick.UnixMilli(),
		"timestamp_s":  tick.Unix(),
	}
}

func (r *chReceiver) getMetricsTemplate(ctx context.Context, tpl *template.Template, tick time.Time) error {
	queryBuf := bytes.Buffer{}
	params := queryParams(tick)
	err := tpl.Execute(&queryBuf, params)
	wrapErr := func(err error) error {
		return fmt.Errorf("failed to execute. Query: %s; error: %w", queryBuf.String(), err)
	}
	if err != nil {
		return wrapErr(err)
	}
	rows, err := r.db.Query(ctx, queryBuf.String())
	if err != nil {
		return wrapErr(err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			labels [][]string
			value  float64
		)
		err = rows.Scan(&labels, &value)
		if err != nil {
			return wrapErr(err)
		}
		metrics := pmetric.NewMetrics()
		res := metrics.ResourceMetrics().AppendEmpty()
		res.Resource().Attributes()
		metric := res.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		data := metric.SetEmptyGauge().DataPoints().AppendEmpty()
		for _, label := range labels {
			if label[0] == "__name__" {
				metric.SetName(label[1])
				continue
			}
			data.Attributes().PutStr(label[0], label[1])
		}
		// The scheduled time, not the time the query returned, so a slow query
		// cannot move a point to another interval.
		data.SetTimestamp(pcommon.NewTimestampFromTime(tick))
		data.SetDoubleValue(value)
		select {
		case <-ctx.Done():
			return nil
		default:
			err = r.metricsConsumer.ConsumeMetrics(ctx, metrics)
			if err != nil {
				return wrapErr(err)
			}
		}
	}
	return nil
}

func (r *chReceiver) getLogsTemplate(ctx context.Context, tpl *template.Template, tick time.Time) error {
	queryBuf := bytes.Buffer{}
	params := queryParams(tick)
	err := tpl.Execute(&queryBuf, params)
	wrapErr := func(err error) error {
		return fmt.Errorf("failed to execute. Query: %s; error: %w", queryBuf.String(), err)
	}
	if err != nil {
		return wrapErr(err)
	}
	rows, err := r.db.Query(ctx, queryBuf.String())
	if err != nil {
		return wrapErr(err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			labels [][]string
			value  string
		)
		err = rows.Scan(&labels, &value)
		if err != nil {
			return wrapErr(err)
		}
		logs := plog.NewLogs()
		res := logs.ResourceLogs().AppendEmpty()
		res.Resource().Attributes()
		log := res.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		for _, label := range labels {
			log.Attributes().PutStr(label[0], label[1])
		}
		log.Body().SetStr(value)
		log.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		select {
		case <-ctx.Done():
			return nil
		default:
			err = r.logsConsumer.ConsumeLogs(ctx, logs)
			if err != nil {
				return wrapErr(err)
			}
		}
	}
	return nil
}

func (r *chReceiver) Shutdown(_ context.Context) error {
	fmt.Println("shutting down")
	r.cancel()
	_ = r.db.Close()
	return nil
}
