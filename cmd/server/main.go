package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fork-devs/erp-iot-ingestion/internal/application/service"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/config"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/http/handler"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/http/middleware"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/kafka"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/persistence/memory"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/telemetry"
	"github.com/fork-devs/erp-iot-ingestion/internal/infrastructure/worker"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	initLogger(cfg.LogLevel)

	kafkaPublisher := kafka.NewKafkaEventPublisher(cfg)
	deviceRepo := memory.NewMemoryDeviceRepository()

	deviceService := service.NewDeviceApplicationService(deviceRepo)
	batchService := service.NewBatchApplicationService(deviceRepo, kafkaPublisher)
	ingestService := service.NewIngestionService(kafkaPublisher)

	workerPool := worker.NewWorkerPool(16, 5000)
	workerPool.Start()
	telemetry.DefaultMetrics.SetQueueDepthProvider(workerPool.QueueDepth)

	deviceHandler := handler.NewDeviceHandler(deviceService)
	registrationHandler := handler.NewRegistrationHandler(deviceService)
	batchHandler := handler.NewBatchHandler(batchService, kafkaPublisher, cfg.MaxRequestBodySize)
	telemetryHandler := handler.NewTelemetryHandler(ingestService, kafkaPublisher, cfg.MaxRequestBodySize)
	healthHandler := handler.NewHealthHandler(kafkaPublisher)
	authMiddleware := middleware.NewDeviceAuthenticator(cfg.AllowedDeviceKeys)

	r := chi.NewRouter()
	r.Use(chimiddleware.RequestID)
	r.Use(chimiddleware.RealIP)
	r.Use(chimiddleware.Recoverer)
	r.Use(chimiddleware.Timeout(30 * time.Second))

	// Health, readiness, and metrics endpoints
	r.Get("/healthz", healthHandler.Liveness)
	r.Get("/readyz", healthHandler.Readiness)
	r.Get("/metrics", telemetry.DefaultMetrics.Handler())

	// Secured OBD2 Devices API
	r.Route("/api/v1/obd2-devices", func(sub chi.Router) {
		sub.Use(authMiddleware.Handler)
		sub.Post("/", deviceHandler.RegisterDevice)
		sub.Get("/", deviceHandler.ListDevices)
		sub.Get("/{id}", deviceHandler.GetDevice)
	})

	// Secured Device Registrations (Vehicle Linking) API
	r.Route("/api/v1/obd2-device-registrations", func(sub chi.Router) {
		sub.Use(authMiddleware.Handler)
		sub.Post("/", registrationHandler.LinkDevice)
		sub.Patch("/{id}", registrationHandler.UpdateStatus)
		sub.Get("/", registrationHandler.ListRegistrations)
	})

	// Secured Batch Telemetry Ingestion API
	r.Route("/api/v1/telemetry-batches", func(sub chi.Router) {
		sub.Use(authMiddleware.Handler)
		sub.Post("/", batchHandler.HandleIngestBatch)
	})

	// Secured Telemetry Ingestion Routes
	r.Route("/api/v1/iot", func(sub chi.Router) {
		sub.Use(authMiddleware.Handler)
		sub.Post("/telemetry", telemetryHandler.HandleIngest)
	})

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.ServerPort),
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		slog.Info("starting erp-iot-ingestion server",
			"port", cfg.ServerPort,
			"environment", cfg.Environment,
			"kafka_brokers", cfg.KafkaBrokers,
			"kafka_topic", cfg.KafkaTelemetryTopic,
		)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server listen error", "error", err)
			os.Exit(1)
		}
	}()

	<-stop
	slog.Info("shutdown signal received, draining active connections, worker pool and kafka buffers...")

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Error("server graceful shutdown error", "error", err)
	}

	if err := workerPool.Stop(5 * time.Second); err != nil {
		slog.Error("worker pool shutdown error", "error", err)
	}

	if err := kafkaPublisher.Close(); err != nil {
		slog.Error("kafka publisher flush error", "error", err)
	}

	slog.Info("erp-iot-ingestion stopped gracefully")
}

func initLogger(levelStr string) {
	var level slog.Level
	switch levelStr {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})
	slog.SetDefault(slog.New(handler))
}
