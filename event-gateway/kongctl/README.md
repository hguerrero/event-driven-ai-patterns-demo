# kongctl Assets

This folder contains Kong Konnect control-plane resources used by this demo's bootstrap, applied declaratively with [`kongctl`](https://konghq.com/products/kong-konnect/event-gateway) rather than Terraform.

## Files

- `data_plane_certificate.yaml` — registers the gateway data-plane certificate object in Konnect. Apply this first.
- `config.yaml` — the full gateway configuration: backend cluster, Apicurio schema registry, listener, and the single `zion-mainframe` virtual cluster (with schema validation on `sentinel.readings`). Apply this second.
- `certs/tls.crt` — local certificate used by the data plane for Konnect mTLS (gitignored).
- `certs/key.crt` — private key paired with `tls.crt` (gitignored, never uploaded).

## How it's used

1. Generate a certificate/key pair into `certs/`.
2. `kongctl apply -f kongctl/data_plane_certificate.yaml`
3. Retrieve the gateway's cluster ID and paste the cert/key/cluster ID into `../konnect.env`.
4. `docker compose up -d` (from `event-gateway/`) to start the KEG data plane.
5. `kongctl apply -f kongctl/config.yaml` to push the backend cluster, virtual cluster, listener, and schema-validation policy.

Full bootstrap commands are documented in the root [README](../../README.md).

## Security notes

- Do not commit private keys from `certs/`.
- Rotate certificates if they are shared or exposed.
