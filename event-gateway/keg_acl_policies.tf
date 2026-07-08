# ============================================================================
# ACL Policy — basic allow-all for the single Zion_Mainframe virtual cluster
# ============================================================================
# Since this demo runs anonymous/passthrough for simplicity, the ACL policy
# here is intentionally permissive. In a real deployment you would scope
# `resource_names` down to the four demo topics and specific consumer groups.

resource "konnect_event_gateway_cluster_policy_acls" "acl_zion_mainframe" {
  provider           = konnect
  name               = "acl_zion_mainframe"
  description        = "Baseline ACL policy allowing describe/read/write on all topics and consumer groups within the Zion Mainframe virtual cluster."
  gateway_id         = konnect_event_gateway.event_driven_ai_patterns_gateway.id
  virtual_cluster_id = konnect_event_gateway_virtual_cluster.zion_mainframe.id

  config = {
    rules = [
      {
        action = "allow"
        operations = [
          { name = "describe" },
          { name = "read" },
          { name = "write" }
        ]
        resource_type = "topic"
        resource_names = [{
          match = "*"
        }]
      },
      {
        action = "allow"
        operations = [
          { name = "describe" },
          { name = "read" },
          { name = "write" },
          { name = "create" }
        ]
        resource_type = "group"
        resource_names = [{
          match = "*"
        }]
      }
    ]
  }
}
