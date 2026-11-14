@US24 @EP10 @erp-iot-ingestion
# User Story: US24 - Manage OBD-II device inventory and telematic vehicle linking
Feature: OBD-II Device Inventory and Vehicle Linking
  As a workshop administrator or service advisor
  I want to register, update, deactivate OBD-II dongles and link them to vehicles
  So that only authorized hardware units stream vehicle telemetry

  Background:
    Given the "erp-iot-ingestion" REST API is available at "/api/v1"

  @US24 @happy-path
  # User Story: US24 - Register an OBD-II dongle and link it to a vehicle
  Scenario: Workshop administrator registers an OBD-II device and links it to a vehicle
    Given an authenticated user with role "WORKSHOP_ADMIN" in tenant "tenant-lima-01"
    When the client sends a "POST" request to "/api/v1/obd2-devices" with JSON payload:
      """
      {
        "imei": "359881090123456",
        "macAddress": "AA:BB:CC:DD:EE:01",
        "protocol": "ISO-15765-4",
        "firmwareVersion": "v2.4.1"
      }
      """
    Then the response status code should be 201
    And the JSON response should contain a generated "id"
    When the client sends a "POST" request to "/api/v1/obd2-device-registrations" with JSON payload:
      """
      {
        "deviceId": "{deviceId}",
        "vehicleId": "vehicle-uuid-01"
      }
      """
    Then the response status code should be 201
    And the JSON response field "status" should equal "ACTIVE"

  @US24 @negative
  # User Story: US24 - Reject duplicate OBD-II IMEI registration
  Scenario: Registering an OBD-II device with an already registered IMEI returns HTTP 409
    Given an authenticated user with role "WORKSHOP_ADMIN"
    And an OBD-II device with IMEI "359881090123456" already exists
    When the client sends a "POST" request to "/api/v1/obd2-devices" with JSON payload:
      """
      {
        "imei": "359881090123456",
        "macAddress": "AA:BB:CC:DD:EE:02",
        "protocol": "ISO-15765-4",
        "firmwareVersion": "v2.4.1"
      }
      """
    Then the response status code should be 409
    And the JSON response should contain an error message indicating duplicate IMEI
