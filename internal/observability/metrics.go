// Package observability implements the metrics and health surface for a
// single node (TR-014/TR-015), scoped honestly to what a single-node,
// unreplicated deployment actually has: request rate/errors/duration and
// commit position. Replication lag, under-replication, rebalance, and
// compaction metrics are omitted rather than stubbed to zero — TR-015
// requires health/metrics to never convert missing evidence into a
// healthy/complete status, and a fabricated "replication_lag=0" would do
// exactly that for a topology that doesn't exist yet.
package observability

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Metrics holds the Prometheus collectors for one node process.
type Metrics struct {
	Registry *prometheus.Registry

	requestsTotal   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	commitPosition  *prometheus.GaugeVec
}

// New creates a fresh registry and registers all collectors.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		Registry: reg,
		requestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "uddp_requests_total",
			Help: "Total native API requests, by method and result code.",
		}, []string{"method", "code"}),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "uddp_request_duration_seconds",
			Help:    "Native API request duration in seconds, by method.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method"}),
		commitPosition: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "uddp_partition_commit_position",
			Help: "Last assigned commit position, by namespace. Doubles as the change stream's head offset (TRD 4.5).",
		}, []string{"namespace"}),
	}

	reg.MustRegister(m.requestsTotal, m.requestDuration, m.commitPosition)
	return m
}

// SetCommitPosition records the namespace's latest commit position/stream
// head offset. Callers pass the value after every mutation rather than
// polling, so the gauge never lags behind what clients could already see.
func (m *Metrics) SetCommitPosition(namespace string, position uint64) {
	m.commitPosition.WithLabelValues(namespace).Set(float64(position))
}

// UnaryServerInterceptor records request count, result code, and duration
// for every native API RPC (TR-014 "request rate/errors/duration").
func (m *Metrics) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)

		code := status.Code(err)
		if err == nil {
			code = codes.OK
		}
		m.requestsTotal.WithLabelValues(info.FullMethod, code.String()).Inc()
		m.requestDuration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())

		return resp, err
	}
}
