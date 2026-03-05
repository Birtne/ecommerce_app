package metrics

import (
	"strings"
	"testing"
)

func TestInitRegistersAndAllowsObservation(t *testing.T) {
	Init()
	OrderPlacedTotal.WithLabelValues("false").Inc()
	OrderRequestTotal.WithLabelValues("ok", "false").Inc()
	OrderPlaceLatency.WithLabelValues("ok").Observe(0.02)
	OrderListRequests.WithLabelValues("all", "none").Inc()
	OutboxPublishTotal.WithLabelValues("ecom.order.created", "sent").Inc()
	OutboxPublishLatency.WithLabelValues("ecom.order.created", "sent").Observe(0.001)
	OutboxEventsGauge.WithLabelValues("pending").Set(3)
	OutboxRuntimeGauge.WithLabelValues("runs").Set(9)
	AdminCommandWaitTotal.WithLabelValues("replay_job:create", "ok").Inc()
	AdminCommandWaitDuration.WithLabelValues("replay_job:create", "ok").Observe(0.03)
}

func TestEncodeTextContainsEcommerceMetrics(t *testing.T) {
	Init()
	body, err := EncodeText()
	if err != nil {
		t.Fatalf("encode text: %v", err)
	}
	want := []string{
		"ecommerce_order_placed_total",
		"ecommerce_order_place_requests_total",
		"ecommerce_outbox_publish_total",
		"ecommerce_outbox_events",
		"ecommerce_outbox_runtime",
		"ecommerce_admin_command_wait_total",
		"ecommerce_admin_command_wait_duration_seconds",
	}
	for _, token := range want {
		if !strings.Contains(body, token) {
			t.Fatalf("metrics text missing %q", token)
		}
	}
}
