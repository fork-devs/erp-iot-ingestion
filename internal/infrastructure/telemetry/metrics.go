package telemetry

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
)

// MetricsCollector tracks operational metrics for erp-iot-ingestion in Prometheus format.
type MetricsCollector struct {
	mu                   sync.RWMutex
	ingestedAccepted     atomic.Int64
	ingestedRejected     atomic.Int64
	kafkaPublishErrors   atomic.Int64
	dlqDispatches        atomic.Int64
	workerQueueDepthFunc func() int

	// Latency histogram buckets (0.01s, 0.05s, 0.1s, 0.2s, 0.5s, 1s)
	bucket001 atomic.Int64 // <= 10ms
	bucket005 atomic.Int64 // <= 50ms
	bucket010 atomic.Int64 // <= 100ms
	bucket020 atomic.Int64 // <= 200ms (QA-1 SLA target)
	bucket050 atomic.Int64 // <= 500ms
	bucketInf atomic.Int64 // +Inf
	latencySum atomic.Uint64 // in microseconds
	latencyCount atomic.Int64
}

// DefaultMetrics is the singleton collector instance.
var DefaultMetrics = NewMetricsCollector()

// NewMetricsCollector instantiates a MetricsCollector.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{}
}

// SetQueueDepthProvider registers a callback returning current worker queue depth.
func (m *MetricsCollector) SetQueueDepthProvider(fn func() int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerQueueDepthFunc = fn
}

// RecordIngest increments ingestion counter and observes latency duration.
func (m *MetricsCollector) RecordIngest(success bool, durationSeconds float64) {
	if success {
		m.ingestedAccepted.Add(1)
	} else {
		m.ingestedRejected.Add(1)
	}

	m.latencyCount.Add(1)
	micros := uint64(durationSeconds * 1e6)
	m.latencySum.Add(micros)

	if durationSeconds <= 0.01 {
		m.bucket001.Add(1)
	}
	if durationSeconds <= 0.05 {
		m.bucket005.Add(1)
	}
	if durationSeconds <= 0.10 {
		m.bucket010.Add(1)
	}
	if durationSeconds <= 0.20 {
		m.bucket020.Add(1)
	}
	if durationSeconds <= 0.50 {
		m.bucket050.Add(1)
	}
	m.bucketInf.Add(1)
}

// RecordKafkaError increments error counter publishing to Kafka.
func (m *MetricsCollector) RecordKafkaError() {
	m.kafkaPublishErrors.Add(1)
}

// RecordDLQDispatch increments DLQ message counter.
func (m *MetricsCollector) RecordDLQDispatch() {
	m.dlqDispatches.Add(1)
}

// Handler returns an HTTP handler serving Prometheus text exposition format.
func (m *MetricsCollector) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		queueDepth := 0
		m.mu.RLock()
		if m.workerQueueDepthFunc != nil {
			queueDepth = m.workerQueueDepthFunc()
		}
		m.mu.RUnlock()

		sumSeconds := float64(m.latencySum.Load()) / 1e6
		count := m.latencyCount.Load()

		fmt.Fprintf(w, "# HELP telemetry_ingested_total Total count of vehicle telemetry packets ingested\n")
		fmt.Fprintf(w, "# TYPE telemetry_ingested_total counter\n")
		fmt.Fprintf(w, "telemetry_ingested_total{status=\"accepted\"} %d\n", m.ingestedAccepted.Load())
		fmt.Fprintf(w, "telemetry_ingested_total{status=\"rejected\"} %d\n\n", m.ingestedRejected.Load())

		fmt.Fprintf(w, "# HELP telemetry_ingestion_duration_seconds Ingestion duration latency in seconds (QA-1 target <= 0.20s)\n")
		fmt.Fprintf(w, "# TYPE telemetry_ingestion_duration_seconds histogram\n")
		fmt.Fprintf(w, "telemetry_ingestion_duration_seconds_bucket{le=\"0.01\"} %d\n", m.bucket001.Load())
		fmt.Fprintf(w, "telemetry_ingestion_duration_seconds_bucket{le=\"0.05\"} %d\n", m.bucket005.Load())
		fmt.Fprintf(w, "telemetry_ingestion_duration_seconds_bucket{le=\"0.1\"} %d\n", m.bucket010.Load())
		fmt.Fprintf(w, "telemetry_ingestion_duration_seconds_bucket{le=\"0.2\"} %d\n", m.bucket020.Load())
		fmt.Fprintf(w, "telemetry_ingestion_duration_seconds_bucket{le=\"0.5\"} %d\n", m.bucket050.Load())
		fmt.Fprintf(w, "telemetry_ingestion_duration_seconds_bucket{le=\"+Inf\"} %d\n", m.bucketInf.Load())
		fmt.Fprintf(w, "telemetry_ingestion_duration_seconds_sum %.6f\n", sumSeconds)
		fmt.Fprintf(w, "telemetry_ingestion_duration_seconds_count %d\n\n", count)

		fmt.Fprintf(w, "# HELP kafka_publish_errors_total Total errors publishing to Kafka topics\n")
		fmt.Fprintf(w, "# TYPE kafka_publish_errors_total counter\n")
		fmt.Fprintf(w, "kafka_publish_errors_total %d\n\n", m.kafkaPublishErrors.Load())

		fmt.Fprintf(w, "# HELP dlq_messages_total Total messages routed to dead-letter queue\n")
		fmt.Fprintf(w, "# TYPE dlq_messages_total counter\n")
		fmt.Fprintf(w, "dlq_messages_total %d\n\n", m.dlqDispatches.Load())

		fmt.Fprintf(w, "# HELP worker_pool_queue_depth Pending jobs in the bounded worker pool queue\n")
		fmt.Fprintf(w, "# TYPE worker_pool_queue_depth gauge\n")
		fmt.Fprintf(w, "worker_pool_queue_depth %d\n", queueDepth)
	}
}
