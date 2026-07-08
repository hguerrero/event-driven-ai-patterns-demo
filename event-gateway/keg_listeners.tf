# ============================================================================
# Listener — single Zion_Mainframe listener
# ============================================================================

resource "konnect_event_gateway_listener" "zion_mainframe" {
  provider   = konnect
  name       = "zion-mainframe-listener"
  gateway_id = konnect_event_gateway.event_driven_ai_patterns_gateway.id
  addresses  = ["0.0.0.0"]
  ports      = ["19092-19190"]

  depends_on = [konnect_event_gateway.event_driven_ai_patterns_gateway]
}

# ============================================================================
# Forwarding Policy (Port Mapping)
# ============================================================================

resource "konnect_event_gateway_listener_policy_forward_to_virtual_cluster" "zion_mainframe" {
  provider    = konnect
  name        = "forward-to-zion-mainframe"
  gateway_id  = konnect_event_gateway.event_driven_ai_patterns_gateway.id
  listener_id = konnect_event_gateway_listener.zion_mainframe.id

  config = {
    port_mapping = {
      advertised_host = "localhost"
      bootstrap_port  = "none"
      min_broker_id   = 0
      destination = {
        id = konnect_event_gateway_virtual_cluster.zion_mainframe.id
      }
    }
  }
}
