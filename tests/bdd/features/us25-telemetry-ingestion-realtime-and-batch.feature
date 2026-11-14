@US25 @EP10 @erp-iot-ingestion
# User Story: US25 - Ingest real-time and batch vehicle telemetry
Feature: Real-Time and Batch Vehicle Telemetry Ingestion
  As an OBD-II telemetry gateway
  I want to submit single real-time sensor readings and compressed telemetry batches
  So that engine parameters are published asynchronously to Kafka without blocking active vehicles

  Background:
    Given the "erp-iot-ingestion" REST API is available at "/api/v1"

  @US25 @happy-path
  # User Story: US25 - Ingest single real-time OBD-II telemetry reading
  Scenario: Authorized OBD-II device streams a valid real-time telemetry frame
    Given an authenticated OBD-II device "device-uuid-01" linked to vehicle "vehicle-uuid-01" with a valid "X-Device-Key"
    When the client sends a "POST" request to "/api/v1/iot/telemetry" with JSON payload:
      """
      {
        "deviceId": "device-uuid-01",
        "vehicleId": "vehicle-uuid-01",
        "engineRpm": 2450,
        "speedKmh": 65.5,
        "coolantTempC": 89.0,
        "batteryVoltage": 13.8
      }
      """
    Then the response status code should be 202
    And the JSON response field "status" should equal "ACCEPTED"

  @US25 @negative
  # User Story: US25 - Reject malformed telemetry payload
  Scenario: Submitting telemetry with negative RPM or missing required fields returns HTTP 400
    Given an authenticated OBD-II device "device-uuid-01" with a valid "X-Device-Key"
    When the client sends a "POST" request to "/api/v1/iot/telemetry" with JSON payload:
      """
      {
        "deviceId": "device-uuid-01",
        "engineRpm": -500
      }
      """
    Then the response status code should be 400
    And the JSON response should contain a validation error
