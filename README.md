# twow-gm-tool

`twow-gm-tool` is a small Rust control-plane service for a local Turtle WoW
K3s lab. It exposes a narrow HTTP API and writes reviewed GM commands into
`tw_logon.pending_commands`, which is already consumed by the existing world
runtime.

This keeps the write path outside the legacy gameplay core:

- no `Player.cpp` or `Unit.cpp` changes
- no new world-thread API surface inside `mangosd`
- no direct DB write from untrusted callers beyond the existing queue contract

## API

All write endpoints require `Authorization: Bearer <JWS>`.
The service verifies an `HS256` compact JWS signed with `GM_TOOL_JWS_SECRET`
and requires a future `exp` claim. If `nbf` is present, it must already be
effective.

- `GET /healthz`
- `GET /readyz`
- `POST /api/v1/gm/commands`
- `POST /api/v1/gm/revive`
- `POST /api/v1/gm/teleport`

`POST /api/v1/gm/commands`

```json
{
  "command": "broadcast Maintenance in 5 minutes",
  "realm_id": 1,
  "run_after_seconds": 0
}
```

`POST /api/v1/gm/revive`

```json
{
  "character": "Qianfuren",
  "realm_id": 1
}
```

`POST /api/v1/gm/teleport`

```json
{
  "character": "Qianfuren",
  "teleport": "stormwind",
  "realm_id": 1
}
```

The service normalizes a leading `.` away from raw commands because
`pending_commands` stores console command text rather than chat-input literals.

## Environment

The deployment reuses the existing governance-plane config map and DB secret:

- `TWOW_DB_HOST`
- `TWOW_DB_PORT`
- `TWOW_DB_USER`
- `TWOW_DB_PASSWORD`
- `TWOW_LOGON_DB`
- `GM_TOOL_JWS_SECRET`
- `GM_TOOL_BIND_ADDR` optional, default `0.0.0.0:8080`
- `GM_TOOL_DEFAULT_REALM_ID` optional, default `1`

## Local Build

```bash
cd tools/twow-gm-tool
cargo test --offline
cargo build --release --locked --offline
```

## Container Build

```bash
podman build -t localhost/twow-gm-tool:local tools/twow-gm-tool
podman save --format docker-archive localhost/twow-gm-tool:local -o /tmp/twow-gm-tool-local.tar
sudo k3s ctr images import /tmp/twow-gm-tool-local.tar
```

## K3s Apply

Apply the main governance plane/domain experiment first so the namespaces,
runtime config map, and DB app secret already exist:

```bash
kubectl apply -k k8s/experiments/governance-plane-domain
kubectl apply -k k8s/experiments/twow-gm-tool-k3s
kubectl -n twow-control-plane rollout status deployment/twow-gm-tool
kubectl -n twow-control-plane get svc,pod -l app.kubernetes.io/name=twow-gm-tool
```

Create the JWS signing secret out of band before the second apply:

```bash
kubectl -n twow-control-plane create secret generic twow-gm-tool-secret \
  --from-literal=GM_TOOL_JWS_SECRET='replace-me'
```

For a non-systemd local proof path, see the parent repo helper:

```bash
bash scripts/run_twow_gm_tool_rootless_k3s_proof.sh --print-only
```

For the smallest runtime validation shape, the parent repo also ships a
single-Pod manifest that runs MariaDB and `twow-gm-tool` together:

```bash
podman build -t localhost/twow-gm-tool:local tools/twow-gm-tool
podman save --format docker-archive localhost/twow-gm-tool:local -o /tmp/twow-gm-tool-local.tar
sudo k3s ctr images import /tmp/twow-gm-tool-local.tar
kubectl -n twow-control-plane create secret generic twow-gm-tool-secret \
  --from-literal=GM_TOOL_JWS_SECRET='replace-me'
kubectl apply -f k8s/experiments/twow-gm-tool-k3s/proof-pod.yaml
```

## Local JWS Example

```bash
export GM_TOOL_JWS_SECRET='replace-me'
export GM_TOOL_JWS="$(python3 - <<'PY'
import base64
import hashlib
import hmac
import json
import os
import time

def b64u(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()

header = b64u(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
payload = b64u(json.dumps({
    "sub": "local-proof",
    "exp": int(time.time()) + 300
}, separators=(",", ":")).encode())
signature = b64u(hmac.new(
    os.environ["GM_TOOL_JWS_SECRET"].encode(),
    f"{header}.{payload}".encode(),
    hashlib.sha256,
).digest())
print(f"{header}.{payload}.{signature}")
PY
)"
```

## Curl Example

```bash
curl -sS \
  -H "Authorization: Bearer ${GM_TOOL_JWS}" \
  -H 'Content-Type: application/json' \
  http://twow-gm-tool.twow-control-plane.svc.cluster.local:8080/api/v1/gm/revive \
  -d '{"character":"Qianfuren","realm_id":1}'
```

## Repository Layout

This directory is intended to live as a dedicated git submodule:

- intended remote: `https://github.com/Hortus-Edenensis/twow-gm-tool.git`
- intended image: `ghcr.io/hortus-edenensis/twow-gm-tool`

The parent repo patch prepares the submodule metadata and local nested
repository shape. The GitHub repository exists and the initial child-repo
content is published on `main`.
