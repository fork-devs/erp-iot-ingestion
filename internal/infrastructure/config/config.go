package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime configuration loaded from environment variables.
type Config struct {
	ServerPort          int
	Environment         string
	LogLevel            string
	KafkaBrokers        []string
	KafkaTelemetryTopic string
	KafkaDLQTopic       string
	KafkaBatchSize      int
	KafkaBatchTimeout   time.Duration
	KafkaAsync          bool
	MaxRequestBodySize  int64
	ShutdownTimeout     time.Duration
	AllowedDeviceKeys   map[string]string // deviceKey -> tenantId
}

// Load loads configuration from environment variables with sensible defaults for local development.
func Load() (*Config, error) {
	portStr := getEnv("SERVER_PORT", "8087")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid SERVER_PORT '%s': %w", portStr, err)
	}

	brokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := splitAndTrim(brokersStr, ",")
	if len(brokers) == 0 {
		return nil, fmt.Errorf("KAFKA_BROKERS must contain at least one broker address")
	}

	batchSizeStr := getEnv("KAFKA_BATCH_SIZE", "100")
	batchSize, err := strconv.Atoi(batchSizeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid KAFKA_BATCH_SIZE '%s': %w", batchSizeStr, err)
	}

	batchTimeoutMsStr := getEnv("KAFKA_BATCH_TIMEOUT_MS", "10")
	batchTimeoutMs, err := strconv.Atoi(batchTimeoutMsStr)
	if err != nil {
		return nil, fmt.Errorf("invalid KAFKA_BATCH_TIMEOUT_MS '%s': %w", batchTimeoutMsStr, err)
	}

	shutdownTimeoutSecStr := getEnv("SHUTDOWN_TIMEOUT_SECONDS", "10")
	shutdownTimeoutSec, err := strconv.Atoi(shutdownTimeoutSecStr)
	if err != nil {
		return nil, fmt.Errorf("invalid SHUTDOWN_TIMEOUT_SECONDS '%s': %w", shutdownTimeoutSecStr, err)
	}

	maxBodySizeStr := getEnv("MAX_REQUEST_BODY_SIZE", "1048576") // 1MB
	maxBodySize, err := strconv.ParseInt(maxBodySizeStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid MAX_REQUEST_BODY_SIZE '%s': %w", maxBodySizeStr, err)
	}

	asyncStr := getEnv("KAFKA_ASYNC", "true")
	kafkaAsync := strings.ToLower(asyncStr) == "true" || asyncStr == "1"

	deviceKeysStr := getEnv("ALLOWED_DEVICE_KEYS", "test-device-key:00000000-0000-0000-0000-000000000001")
	deviceKeys := parseDeviceKeys(deviceKeysStr)

	return &Config{
		ServerPort:          port,
		Environment:         getEnv("ENVIRONMENT", "dev"),
		LogLevel:            getEnv("LOG_LEVEL", "INFO"),
		KafkaBrokers:        brokers,
		KafkaTelemetryTopic: getEnv("KAFKA_TELEMETRY_TOPIC", "telemetry.raw"),
		KafkaDLQTopic:       getEnv("KAFKA_DLQ_TOPIC", "telemetry.dlq"),
		KafkaBatchSize:      batchSize,
		KafkaBatchTimeout:   time.Duration(batchTimeoutMs) * time.Millisecond,
		KafkaAsync:          kafkaAsync,
		MaxRequestBodySize:  maxBodySize,
		ShutdownTimeout:     time.Duration(shutdownTimeoutSec) * time.Second,
		AllowedDeviceKeys:   deviceKeys,
	}, nil
}

func getEnv(key, defaultVal string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return val
}

func splitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func parseDeviceKeys(raw string) map[string]string {
	result := make(map[string]string)
	entries := splitAndTrim(raw, ",")
	for _, entry := range entries {
		kv := strings.Split(entry, ":")
		if len(kv) == 2 {
			key := strings.TrimSpace(kv[0])
			tenant := strings.TrimSpace(kv[1])
			if key != "" && tenant != "" {
				result[key] = tenant
			}
		}
	}
	return result
}
