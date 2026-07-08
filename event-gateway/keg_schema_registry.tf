# ============================================================================
# Schema Registry & Validation Policy
# ============================================================================

resource "konnect_event_gateway_schema_registry" "apicurio_schema_registry" {
  provider   = konnect
  gateway_id = konnect_event_gateway.event_driven_ai_patterns_gateway.id

  confluent = {
    name        = "Apicurio Schema Registry Compatibility Mode"
    description = "Confluent-compatible schema registry interface powered by Apicurio Registry. Provides centralized schema validation for the event-driven AI patterns demo."

    labels = {
      env  = "demo"
      role = "schema-registry"
      tier = "core"
    }

    config = {
      endpoint        = "http://apicurio-registry:8080/apis/ccompat/v7"
      schema_type     = "json"
      timeout_seconds = 8
    }
  }
}

resource "konnect_event_gateway_produce_policy_schema_validation" "sentinel_readings_schema_validation" {
  provider           = konnect
  name               = "sentinel-readings-produce-schema-validation"
  description        = "Enforces JSON schema validation on sentinel.readings produce requests, validating against config/schemas/sentinel_reading.json. Rejects non-conformant payloads to preserve data integrity in the sensor telemetry stream."
  gateway_id         = konnect_event_gateway.event_driven_ai_patterns_gateway.id
  virtual_cluster_id = konnect_event_gateway_virtual_cluster.zion_mainframe.id

  labels = {
    env  = "demo"
    role = "schema-validation"
    tier = "policy"
  }

  enabled   = true
  condition = "context.topic.name == 'sentinel.readings'"

  config = {
    confluent_schema_registry = {
      value_validation_action = "reject"

      schema_registry = {
        id = konnect_event_gateway_schema_registry.apicurio_schema_registry.id
      }
    }
  }
}
