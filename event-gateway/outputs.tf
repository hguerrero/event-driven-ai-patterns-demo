# ============================================================================
# Event Gateway Outputs
# ============================================================================

output "event_gateway_id" {
  description = "The ID of the Event Gateway"
  value       = konnect_event_gateway.event_driven_ai_patterns_gateway.id
}

output "event_gateway_name" {
  description = "The name of the Event Gateway"
  value       = konnect_event_gateway.event_driven_ai_patterns_gateway.name
}

output "zion_mainframe_virtual_cluster_id" {
  description = "The ID of the Zion_Mainframe virtual cluster"
  value       = konnect_event_gateway_virtual_cluster.zion_mainframe.id
}
