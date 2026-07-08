# ============================================================================
# Konnect Configuration
# ============================================================================

variable "konnect_server_url" {
  type        = string
  description = "Which Konnect instance to point at"
  default     = "https://us.api.konghq.com"
}

variable "konnect_token" {
  type        = string
  description = "API token to reach Konnect"
  sensitive   = true
}


# ============================================================================
# Event Gateway Configuration
# ============================================================================

variable "event_gateway_name" {
  type        = string
  description = "Name of the Event Gateway instance"
  default     = "event_driven_ai_patterns_gateway"
}

variable "backend_cluster_bootstrap_servers" {
  description = "List of bootstrap servers for the 3-node Kafka KRaft backend cluster"
  type        = list(string)
  default = [
    "kafka1:9092",
    "kafka2:9092",
    "kafka3:9092"
  ]
}

# ============================================================================
# Docker / Data Plane Configuration
# ============================================================================

variable "konnect_region" {
  type        = string
  description = "Konnect region for the data plane (e.g. us, eu)"
  default     = "us"
}

variable "konnect_domain" {
  type        = string
  description = "Konnect domain for the data plane"
  default     = "konghq.com"
}
