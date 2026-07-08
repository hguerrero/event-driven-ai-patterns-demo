# ============================================================================
# Event Gateway & Backend Cluster
# ============================================================================

resource "konnect_event_gateway" "event_driven_ai_patterns_gateway" {
  provider = konnect
  name     = var.event_gateway_name
}

resource "konnect_event_gateway_backend_cluster" "backend_cluster" {
  provider    = konnect
  name        = "Zion-Backend-Mainframe"
  description = "The single backend Kafka cluster underlying the Zion Mainframe virtual cluster. Carries sentinel telemetry, Agent Smith Protocol orchestration events, Oracle CDC changes, and the Zion Archive commit log."
  gateway_id  = konnect_event_gateway.event_driven_ai_patterns_gateway.id

  labels = {
    env       = "demo"
    role      = "source"
    tier      = "core"
    pattern   = "event-driven-ai"
  }

  authentication = {
    anonymous = {}
  }

  bootstrap_servers = var.backend_cluster_bootstrap_servers

  tls = {
    enabled = false
  }

  insecure_allow_anonymous_virtual_cluster_auth = true

  depends_on = [konnect_event_gateway.event_driven_ai_patterns_gateway]
}
