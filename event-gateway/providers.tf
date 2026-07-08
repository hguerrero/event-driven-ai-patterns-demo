provider "konnect" {
  server_url            = var.konnect_server_url
  personal_access_token = var.konnect_token
}

provider "docker" {}
