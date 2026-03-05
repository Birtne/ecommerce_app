package metrics

import (
	"bytes"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

var (
	once sync.Once

	OrderPlacedTotal  *prometheus.CounterVec
	OrderRequestTotal *prometheus.CounterVec
	OrderPlaceLatency *prometheus.HistogramVec
	OrderListRequests *prometheus.CounterVec

	OutboxPublishTotal       *prometheus.CounterVec
	OutboxPublishLatency     *prometheus.HistogramVec
	OutboxEventsGauge        *prometheus.GaugeVec
	OutboxRuntimeGauge       *prometheus.GaugeVec
	AdminCommandWaitTotal    *prometheus.CounterVec
	AdminCommandWaitDuration *prometheus.HistogramVec
)

func Init() {
	once.Do(func() {
		OrderPlacedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ecommerce",
			Subsystem: "order",
			Name:      "placed_total",
			Help:      "Total order placement attempts by replay status.",
		}, []string{"replay"})

		OrderPlaceLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "ecommerce",
			Subsystem: "order",
			Name:      "place_duration_seconds",
			Help:      "Order placement latency in seconds.",
			Buckets:   []float64{0.01, 0.03, 0.1, 0.3, 1, 3},
		}, []string{"result"})

		OrderRequestTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ecommerce",
			Subsystem: "order",
			Name:      "place_requests_total",
			Help:      "Total order placement requests by result and replay.",
		}, []string{"result", "replay"})

		OrderListRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ecommerce",
			Subsystem: "order",
			Name:      "history_requests_total",
			Help:      "Order history query requests by filter usage.",
		}, []string{"status_filter", "time_filter"})

		OutboxPublishTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ecommerce",
			Subsystem: "outbox",
			Name:      "publish_total",
			Help:      "Outbox publish attempts by topic and result.",
		}, []string{"topic", "result"})

		OutboxPublishLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "ecommerce",
			Subsystem: "outbox",
			Name:      "publish_duration_seconds",
			Help:      "Outbox publish latency in seconds by topic and result.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.03, 0.1, 0.3, 1},
		}, []string{"topic", "result"})

		OutboxEventsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ecommerce",
			Subsystem: "outbox",
			Name:      "events",
			Help:      "Current outbox event counts by status.",
		}, []string{"status"})

		OutboxRuntimeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ecommerce",
			Subsystem: "outbox",
			Name:      "runtime",
			Help:      "Outbox runtime counters and timestamps.",
		}, []string{"metric"})

		AdminCommandWaitTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ecommerce",
			Subsystem: "admin_command",
			Name:      "wait_total",
			Help:      "Admin command idempotency wait attempts by action and result.",
		}, []string{"action", "result"})

		AdminCommandWaitDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "ecommerce",
			Subsystem: "admin_command",
			Name:      "wait_duration_seconds",
			Help:      "Admin command idempotency wait duration in seconds.",
			Buckets:   []float64{0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5},
		}, []string{"action", "result"})

		prometheus.MustRegister(
			OrderPlacedTotal,
			OrderRequestTotal,
			OrderPlaceLatency,
			OrderListRequests,
			OutboxPublishTotal,
			OutboxPublishLatency,
			OutboxEventsGauge,
			OutboxRuntimeGauge,
			AdminCommandWaitTotal,
			AdminCommandWaitDuration,
		)
	})
}

func EncodeText() (string, error) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return "", err
	}
	var b bytes.Buffer
	for _, mf := range mfs {
		if _, err := expfmt.MetricFamilyToText(&b, mf); err != nil {
			return "", err
		}
	}
	return b.String(), nil
}
