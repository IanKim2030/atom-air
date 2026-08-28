# Atom Air — cloud backend & hybrid web

```
Browser  <--/ws/live?store_id=S001-->  Cloud (FastAPI)  <--/ws/gateway/{id}-->  Store PC
 index.html                             cloud.db (WAL)                          atomair-gateway.exe (Go)
 Tailwind + Chart.js                                                            Mosquitto -> Atom Lite
```

The gateway dials **out** to the cloud, so a store behind NAT/firewall needs no inbound
port forwarding, and AC control rides back down the same socket.

## Run it

```bash
pip install -r ../requirements.txt

# terminal 1 — cloud
python -m uvicorn cloud.cloud_server:app --host 127.0.0.1 --port 8000   # from the repo root

# terminal 2 — store gateway (Go; --simulate needs no hardware or broker)
cd gateway && go build -o atomair-gateway.exe . && ./atomair-gateway.exe --store-id S001 --simulate
```

The gateway is a separate Go binary that installs as a Windows service on the
store PC — see [gateway/README.md](../gateway/README.md).

Open <http://127.0.0.1:8000/> and sign in (demo store: `S001` / `1234`).
`?tab=control|stats|settings` deep-links a mobile tab; desktop shows every panel at once.

## Web login & admin console

| page | who | credentials |
|---|---|---|
| `/login` | store staff / owner (점주) | store code **or** owner id + password — an owner account opens every store it holds |
| `/admin/login` | HQ operator | `ATOM_ADMIN_USER` / `ATOM_ADMIN_PASSWORD` (default `admin` / `admin123!`) |
| `/admin` | HQ operator | store registration, subscription state, password resets, SOTA equipment upgrades |

Sessions are HttpOnly cookies held in memory (24 h TTL); a server restart just re-prompts
login. Store staff see only their own store; an admin can open any store's dashboard via
`/?store_id=`. `/ws/live` enforces the same rule, so the dashboard needs a login. Stores
migrated from a pre-login database get the default store password (`ATOM_DEFAULT_STORE_PASSWORD`,
default `1234`) — reset it from the admin console.

## On-demand live stream

The cloud counts viewers per store:

| event | cloud → gateway | gateway behaviour |
|---|---|---|
| first viewer connects | `START_LIVE_STREAM` | bypasses every 1-second `SensorPacket` up, raw |
| more viewers join | *(nothing — already streaming)* | unchanged |
| last viewer leaves | `STOP_LIVE_STREAM` | back to normal mode: 1-minute stats only |
| gateway reconnects with viewers waiting | `START_LIVE_STREAM` re-issued | resumes with no user action |

Local SQLite writes, 1-minute downsampling and the licence check run in their own
goroutines on the gateway and are **never** gated on the stream or on the cloud being
reachable.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` | the hybrid web UI — needs a session, else redirects to `/login` |
| `GET` | `/login` · `/admin/login` · `/admin` | login pages + admin console |
| `WS` | `/ws/live?store_id=` | browser channel (session cookie required) |
| `WS` | `/ws/gateway/{store_id}?token=` | store gateway channel |
| `POST` | `/api/v1/auth/login` · `/api/v1/auth/admin/login` · `/api/v1/auth/logout` | session management |
| `GET` | `/api/v1/auth/me` | who am I (role + store) |
| `POST` | `/api/v1/store/authorize` | daily licence check + dynamic grace period |
| `GET` | `/api/v1/admin/stores` | admin: every store + licence, gateway, device count |
| `POST` | `/api/v1/admin/stores` | admin: register a store (id, name, password, terms) |
| `POST` | `/api/v1/admin/stores/{id}/password` | admin: reset a store's web password |
| `POST` | `/api/v1/admin/stores/{id}/sota` | admin: kick a firmware deploy over the gateway socket |
| `POST` | `/api/v1/stores/{id}/license` | admin: set state / grace days / expiry / plan / owner |
| `GET` | `/api/v1/stores/{id}/history` | audit trail for one store (`?category=&limit=`) |
| `POST` | `/api/v1/stores/{id}/settings` | store-wide prefs (AI auto-temp), audited |
| `GET` | `/api/v1/admin/history` | audit trail across all stores |
| `GET`·`POST` | `/api/v1/admin/owners` | owner (점주) accounts — one login, many stores |
| `DELETE` | `/api/v1/stores/{id}/devices/{dev_id}` | admin: retire a device's card (409 while it is still publishing) |
| `GET` | `/api/v1/stores/{id}/stats?minutes=` | 1-minute statistics |
| `GET` | `/api/v1/stores/{id}/status` | gateway online, viewers, AC state, licence, SOTA progress |
| `GET` | `/api/v1/ac/models` | AC brand/model catalog for the SOTA popup |
| `GET` | `/healthz` | liveness |

### WebSocket messages

**Browser → cloud** — `ac_control` · `sota_deploy` · `request_stats` · `ping`
**Cloud → browser** — `hello` · `live` · `stats` · `gateway_status` · `ac_state` · `ac_ack` · `sota_progress` · `error` · `pong`
**Gateway → cloud** — binary 12-byte sensor frames (one or more per frame) · `minute_stats` · `sota_progress` · `ac_ack` · `gateway_status`
**Cloud → gateway** — `START_LIVE_STREAM` · `STOP_LIVE_STREAM` · `AC_CONTROL` (packet as hex) · `DEPLOY_FIRMWARE`

## Licence & grace period

`grace_period_days` (default 30, per-store, operator-tunable) is the **offline allowance**:
how long the gateway may keep running past its last successful check. The gateway caches it
in `%ProgramData%\AtomAir\store_license_config.json`, so it survives restarts and cloud
outages.

| store state | result |
|---|---|
| `active` | `authorized`, `status: active`, grace clock reset |
| `expired`, still inside `expiry + grace` | `authorized`, `status: grace`, `days_remaining` counts down |
| `expired`, past the window | refused, `status: expired` |
| `suspended` | refused immediately, no grace |
| unknown store | refused, `status: unregistered` |

The response is always HTTP 200 — the gateway must read the grace terms even when refused.

## Wire protocol

Checksum is an **XOR of all preceding bytes** (sensor: bytes 0–10; AC: bytes 0–5).

The cloud implements the formats in [`common/protocol.py`](../common/protocol.py); the Go
gateway implements them in `gateway/protocol/`. Both assert against the shared vectors in
[`common/protocol_vectors.json`](../common/protocol_vectors.json), so the two implementations
cannot drift:

```bash
python -m unittest discover -s common       # cloud side
cd gateway && go test ./...                 # gateway side
```

## Configuration

| env var | default | |
|---|---|---|
| `ATOM_GATEWAY_TOKEN` | `dev-gateway-token` | **change this before deploying** |
| `ATOM_CLOUD_DB` | `cloud/cloud.db` | |
| `ATOM_DEFAULT_STORE_ID` | `S001` | |
| `ATOM_GRACE_DAYS` | `30` | grace window for newly seeded stores |
| `ATOM_ADMIN_USER` | `admin` | **change this before deploying** |
| `ATOM_ADMIN_PASSWORD` | `admin123!` | **change this before deploying** |
| `ATOM_DEFAULT_STORE_PASSWORD` | `1234` | seeded/migrated stores only; new stores set one at registration |

## Chart conventions

Every plot carries **one series** — temperature and humidity get their own charts rather
than sharing a dual axis, and the live view follows one device at a time (a store's devices
interleaved into one line render as a sawtooth, not a trend). Series colors are the first
three slots of a categorical palette validated for colour-vision deficiency in both light
and dark mode. The 1-minute charts are averaged across all devices in the store, which their
subtitles state.
