# ============================================================================
# Virtual Cluster — single Zion_Mainframe cluster, passthrough mode
# ============================================================================
#
# Unlike the baseline SKO demo (which split traffic across three virtual
# clusters for three simulated "worlds"), this demo uses exactly ONE virtual
# cluster in passthrough mode (no topic prefix). All four flat-named topics
# (sentinel.readings, agent.orchestration.events, oracle.profile.changes,
# zion.archive.log) pass straight through to the backend cluster unmodified.

resource "konnect_event_gateway_virtual_cluster" "zion_mainframe" {
  provider    = konnect
  name        = "Zion_Mainframe"
  description = "The single virtual cluster for the event-driven AI patterns demo. Carries sentinel telemetry, Agent Smith Protocol fan-out events, Oracle CDC changes, and the Zion Archive audit log — all in passthrough mode with anonymous auth."
  gateway_id  = konnect_event_gateway.event_driven_ai_patterns_gateway.id

  labels = {
    env     = "demo"
    tier    = "core"
    pattern = "event-driven-ai"
  }

  destination = {
    id = konnect_event_gateway_backend_cluster.backend_cluster.id
  }

  acl_mode  = "passthrough"
  dns_label = "zion-mainframe"

  authentication = [{ anonymous = {} }]

  depends_on = [konnect_event_gateway.event_driven_ai_patterns_gateway, konnect_event_gateway_backend_cluster.backend_cluster]
}
