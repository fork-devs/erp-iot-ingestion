package telemetry

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsCollector_Handler(t *testing.T) {
	collector := NewMetricsCollector()
	collector.SetQueueDepthProvider(func() int { return 42 })

	collector.RecordIngest(true, 0.045)  // 45ms, fits <=0.05 and <=0.20
	collector.RecordIngest(false, 0.250) // 250ms
	collector.RecordKafkaError()
	collector.RecordDLQDispatch()

	req := httptest.NewRequest("GET", "/metrics", nil)
	rr := httptest.NewRecorder()

	collector.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}

	body := rr.Body.String()
	if !strings.Contains(body, `telemetry_ingested_total{status="accepted"} 1`) {
		t.Errorf("missing accepted count metric: %s", body)
	}
	if !strings.Contains(body, `telemetry_ingested_total{status="rejected"} 1`) {
		t.Errorf("missing rejected count metric: %s", body)
	}
	if !strings.Contains(body, `kafka_publish_errors_total 1`) {
		t.Errorf("missing kafka errors metric: %s", body)
	}
	if !strings.Contains(body, `dlq_messages_total 1`) {
		t.Errorf("missing dlq metric: %s", body)
	}
	if !strings.Contains(body, `worker_pool_queue_depth 42`) {
		t.Errorf("missing queue depth metric: %s", body)
	}
	if !strings.Contains(body, `telemetry_ingestion_duration_seconds_bucket{le="0.2"} 1`) {
		t.Errorf("missing p95 bucket metric: %s", body)
	}
}
