package domain

import "errors"

var (
	// ErrInvalidTelemetry indicates validation failed on telemetry data.
	ErrInvalidTelemetry = errors.New("invalid telemetry data")

	// ErrUnauthorizedDevice indicates invalid or missing device credentials.
	ErrUnauthorizedDevice = errors.New("unauthorized device")

	// ErrDeviceNotFound indicates the device key is not registered to any tenant.
	ErrDeviceNotFound = errors.New("device not registered")

	// ErrPublishFailed indicates failure publishing to message bus.
	ErrPublishFailed = errors.New("failed to publish telemetry event")
)
