# kongctl Assets

This folder provisions the `ai-gateway` Kong Gateway control plane in Konnect declaratively with [`kongctl`](https://developer.konghq.com/kongctl/), and pushes its service/route/plugin configuration via `kongctl`'s built-in decK integration (`_deck`) rather than loading a local declarative file directly into a DB-less Kong node.

## Files

- `data_plane_certificate.yaml` — creates the `ai-gateway` control plane and registers its data-plane mTLS certificate. Apply this first.
- `config.yaml` — adds a `_deck` block pointing at `kong.yaml`; `kongctl apply` runs `deck gateway apply` against the control plane on your behalf. Apply this second (and again any time `kong.yaml` changes).
- `kong.yaml` — the actual Kong declarative config: the `ai-gw` service, `/chat` route, and the `ai-proxy` / `ai-rag-injector` / `cors` plugins. This is a normal decK state file (`_format_version: "3.0"`), tagged `event-driven-ai-patterns` for safe `sync` scoping.
- `certs/tls.crt` / `certs/key.key` — local mTLS identity for the data-plane container in `../docker-compose.yaml` (gitignored; only `tls.crt` is uploaded to Konnect).

## How it's used

1. Generate a certificate/key pair into `certs/` (EC key — see the comment in `data_plane_certificate.yaml`).
2. `kongctl apply -f kongctl/data_plane_certificate.yaml`
3. Populate the control plane's cluster/telemetry endpoints into `../konnect.env` — the root README's Setup section does this automatically with `kongctl get gateway control-plane ai-gateway --output json --jq '.config.control_plane_endpoint'` (and `.config.telemetry_endpoint`); the Konnect UI (Gateway Manager → `ai-gateway` → **New Data Plane Node** → Docker tab) shows the same values if you'd rather copy them by hand.
4. `docker compose up -d` (from `ai-gateway/`) to start the Kong Gateway data-plane container + Redis.
5. `kongctl apply -f kongctl/config.yaml` to push the AI Gateway service/route/plugins via decK.

Full bootstrap commands are documented in the root [README](../../README.md).

## Why control planes + decK instead of a native kongctl resource

`kongctl`'s native declarative resources cover Konnect-level objects (control planes, portals, APIs, Event Gateway, etc.) — Kong Gateway's own entities (services, routes, plugins, consumers) are managed through decK, which `kongctl` invokes for you via the `_deck` pseudo-resource on a `control_planes` entry. This is the same mechanism used for any Kong Gateway control plane in Konnect, and it's why `kong.yaml` here still looks like an ordinary decK/Kong declarative file.

## Security notes

- Do not commit private keys from `certs/`.
- Rotate certificates if they are shared or exposed.
