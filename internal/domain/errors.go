package domain

import "errors"

var (
	// ErrInvalidTelemetry indicates validation failed on telemetry data.
	ErrInvalidTelemetry = errors.New("invalid telemetry data")

	// ErrUnauthorizedDevice indicates invalid or missing device credentials.
	ErrUnauthorizedDevice = errors.New("unauthorized device")

	// ErrDeviceNotFound indicates the device is not registered to any tenant.
	ErrDeviceNotFound = errors.New("device not registered")

	// ErrDeviceConflict indicates a device with the same MAC address already exists in the tenant.
	ErrDeviceConflict = errors.New("device with MAC address already registered")

	// ErrDeviceNotAvailable indicates the device is not in AVAILABLE status for linking.
	ErrDeviceNotAvailable = errors.New("device is not available for linking")

	// ErrDeviceAlreadyLinked indicates the device already has an active registration.
	ErrDeviceAlreadyLinked = errors.New("device already has an active registration")

	// ErrVehicleAlreadyLinked indicates the vehicle already has an active registration.
	ErrVehicleAlreadyLinked = errors.New("vehicle already has an active registration")

	// ErrRegistrationNotFound indicates the device-vehicle registration was not found.
	ErrRegistrationNotFound = errors.New("device registration not found")

	// ErrInvalidMACAddress indicates MAC address format is invalid.
	ErrInvalidMACAddress = errors.New("invalid MAC address format")

	// ErrInvalidOdometer indicates odometer value decreased compared to previous reading.
	ErrInvalidOdometer = errors.New("odometer cannot decrease")

	// ErrPublishFailed indicates failure publishing to message bus.
	ErrPublishFailed = errors.New("failed to publish telemetry event")
)
