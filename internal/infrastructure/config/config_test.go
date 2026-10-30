package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/config"
)

func TestLoad_DefaultValues(t *testing.T) {
	os.Clearenv()

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("expected no error loading default config, got: %v", err)
	}

	if cfg.ServerPort != 8087 {
		t.Errorf("expected ServerPort 8087, got %d", cfg.ServerPort)
	}
	if cfg.KafkaTelemetryTopic != "telemetry.raw" {
		t.Errorf("expected KafkaTelemetryTopic 'telemetry.raw', got '%s'", cfg.KafkaTelemetryTopic)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("expected ShutdownTimeout 10s, got %v", cfg.ShutdownTimeout)
	}
	if len(cfg.AllowedDeviceKeys) == 0 {
		t.Error("expected default AllowedDeviceKeys to contain fallback key")
	}
}

func TestLoad_CustomValues(t *testing.T) {
	os.Setenv("SERVER_PORT", "9090")
	os.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	os.Setenv("KAFKA_BATCH_SIZE", "500")
	os.Setenv("ALLOWED_DEVICE_KEYS", "key-a:tenant-1,key-b:tenant-2")
	defer os.Clearenv()

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("expected no error loading custom config, got: %v", err)
	}

	if cfg.ServerPort != 9090 {
		t.Errorf("expected ServerPort 9090, got %d", cfg.ServerPort)
	}
	if len(cfg.KafkaBrokers) != 2 {
		t.Errorf("expected 2 brokers, got %d", len(cfg.KafkaBrokers))
	}
	if cfg.KafkaBatchSize != 500 {
		t.Errorf("expected batch size 500, got %d", cfg.KafkaBatchSize)
	}
	if cfg.AllowedDeviceKeys["key-a"] != "tenant-1" || cfg.AllowedDeviceKeys["key-b"] != "tenant-2" {
		t.Errorf("expected parsed device keys map, got: %v", cfg.AllowedDeviceKeys)
	}
}
