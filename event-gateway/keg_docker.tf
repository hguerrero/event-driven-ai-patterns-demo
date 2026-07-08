# KEG Data Plane Docker Container
# Runs the Kong Event Gateway data plane container using the generated certificates

# Reference the existing Docker network created by docker-compose
data "docker_network" "keg_network" {
  name = "keg-network"
}

# Pull the Kong Event Gateway image
resource "docker_image" "keg" {
  name         = "kong/kong-event-gateway-dev:latest"
  keep_locally = true
}

# Run the KEG data plane container
resource "docker_container" "keg_data_plane" {
  name  = "keg-data-plane"
  image = docker_image.keg.image_id

  env = [
    "KONG_KONNECT_REGION=${var.konnect_region}",
    "KONG_KONNECT_DOMAIN=${var.konnect_domain}",
    "KONG_KONNECT_GATEWAY_CLUSTER_ID=${konnect_event_gateway.event_driven_ai_patterns_gateway.id}",
    "KONG_KONNECT_CLIENT_CERT=${tls_self_signed_cert.keg_data_plane.cert_pem}",
    "KONG_KONNECT_CLIENT_KEY=${tls_private_key.keg_data_plane.private_key_pem}",
    "KEG__OBSERVABILITY__LOG_FLAGS=info,keg=debug",
  ]

  # Single virtual cluster listener range: 19092-19190
  dynamic "ports" {
    for_each = range(19092, 19190)
    content {
      internal = ports.value
      external = ports.value
    }
  }

  networks_advanced {
    name = data.docker_network.keg_network.id
  }

  restart = "unless-stopped"

  depends_on = [
    konnect_event_gateway_data_plane_certificate.keg_data_plane_cert
  ]
}

# ============================================================================
# Apicurio Registry API (Schema Registry)
# ============================================================================

# Pull the Apicurio Registry image
resource "docker_image" "apicurio_registry" {
  name         = "apicurio/apicurio-registry:3.1.7"
  keep_locally = true
}

# Run the Apicurio Registry API container (in-memory storage)
resource "docker_container" "apicurio_registry" {
  name  = "apicurio-registry"
  image = docker_image.apicurio_registry.image_id

  ports {
    internal = 8080
    external = 8080
  }

  networks_advanced {
    name = data.docker_network.keg_network.id
  }

  restart = "unless-stopped"
}

# ============================================================================
# Apicurio Registry UI
# ============================================================================

# Pull the Apicurio Registry UI image
resource "docker_image" "apicurio_registry_ui" {
  name         = "apicurio/apicurio-registry-ui:3.1.7"
  keep_locally = true
}

# Run the Apicurio Registry UI container
resource "docker_container" "apicurio_registry_ui" {
  name  = "apicurio-registry-ui"
  image = docker_image.apicurio_registry_ui.image_id

  ports {
    internal = 8080
    external = 8888
  }

  networks_advanced {
    name = data.docker_network.keg_network.id
  }

  restart = "unless-stopped"

  depends_on = [
    docker_container.apicurio_registry
  ]
}

# ============================================================================
# Register JSON Schema in Apicurio Registry
# ============================================================================

resource "terraform_data" "register_schema" {
  # Force re-run on every apply to ensure schema exists (no persistence in Apicurio)
  triggers_replace = timestamp()

  provisioner "local-exec" {
    command = <<-EOT
      echo "Waiting for Apicurio Registry to be ready..."
      for i in $(seq 1 30); do
        if curl -s -o /dev/null -w "%%{http_code}" http://localhost:8080/apis/ccompat/v7/subjects | grep -q "200"; then
          echo "Apicurio Registry (Confluent compat) is ready."
          break
        fi
        echo "Attempt $i/30 - Registry not ready yet, waiting 5s..."
        sleep 5
      done

      echo "Registering JSON schema via Confluent-compatible API..."
      curl -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": ${jsonencode(file("${path.module}/../config/schemas/sentinel_reading.json"))}, "schemaType": "JSON"}' \
        http://localhost:8080/apis/ccompat/v7/subjects/sentinel.readings-value/versions

      echo ""
      echo "Schema registration complete."
    EOT
  }

  depends_on = [
    docker_container.apicurio_registry
  ]
}

# ============================================================================
# Create Kafka Topics from config/topics.txt
# ============================================================================

resource "terraform_data" "create_topics" {
  # Force re-run on every apply to ensure topics exist (idempotent with --if-not-exists)
  triggers_replace = timestamp()

  provisioner "local-exec" {
    command = <<-EOT
      echo "Creating Kafka topics from config/topics.txt..."
      while IFS= read -r topic || [ -n "$topic" ]; do
        [ -z "$topic" ] && continue
        echo "  → Creating topic: $topic"
        docker exec kafka_cluster-kafka1-1 /opt/kafka/bin/kafka-topics.sh \
          --bootstrap-server kafka1:9092 \
          --create \
          --if-not-exists \
          --topic "$topic" \
          --partitions 3 \
          --replication-factor 3
      done < "${path.module}/../config/topics.txt"
      echo "Topic creation complete."
    EOT
  }
}
