# Atom Air — cloud backend & hybrid web

```
Browser  <--/ws/live?store_id=S001-->  Cloud (FastAPI)  <--/ws/gateway/{id}-->  Store PC
 index.html                             cloud.db (WAL)                          gateway_service.py
 Tailwind + Chart.js                                                            Mosquitto -> Atom Lite
```

The gateway dials **out** to the cloud, so a store behind NAT/firewall needs no inbound
port forwarding, and AC control rides back down the same socket.

## Run it

```bash
pip install -r ../requirements.txt

# terminal 1 — cloud
python -m uvicorn cloud.cloud_server:app --host 127.0.0.1 --port 8000   # from the repo root

# terminal 2 — store gateway (--simulate needs no hardware or broker)
python gateway/gateway_service.py --store-id S001 --simulate --devices 2
```

Open <http://127.0.0.1:8000/?store_id=S001>. `?tab=control|stats|settings` deep-links a
mobile tab; desktop shows every panel at once.

## On-demand live stream

The cloud counts viewers per store:

| event | cloud → gateway | gateway behaviour |
|---|---|---|
| first viewer connects | `START_LIVE_STREAM` | bypasses every 1-second `SensorPacket` up, raw |
| more viewers join | *(nothing — already streaming)* | unchanged |
| last viewer leaves | `STOP_LIVE_STREAM` | back to normal mode: 1-minute stats only |
| gateway reconnects with viewers waiting | `START_LIVE_STREAM` re-issued | resumes with no user action |

Local SQLite writes, 1-minute downsampling and the licence check run in their own asyncio
tasks and are **never** gated on the stream or on the cloud being reachable.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` | the hybrid web UI (re-read per request, so HTML edits need no restart) |
| `WS` | `/ws/live?store_id=` | browser channel |
| `WS` | `/ws/gateway/{store_id}?token=` | store gateway channel |
| `POST` | `/api/v1/store/authorize` | daily licence check + dynamic grace period |
| `POST` | `/api/v1/stores/{id}/license` | operator: set state / grace days / expiry |
| `GET` | `/api/v1/stores/{id}/stats?minutes=` | 1-minute statistics |
| `GET` | `/api/v1/stores/{id}/status` | gateway online, viewers, AC state, licence |
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
in `gateway/store_license_config.json`, so it survives restarts and cloud outages.

| store state | result |
|---|---|
| `active` | `authorized`, `status: active`, grace clock reset |
| `expired`, still inside `expiry + grace` | `authorized`, `status: grace`, `days_remaining` counts down |
| `expired`, past the window | refused, `status: expired` |
| `suspended` | refused immediately, no grace |
| unknown store | refused, `status: unregistered` |

The response is always HTTP 200 — the gateway must read the grace terms even when refused.

## Wire protocol

Defined once in [`common/protocol.py`](../common/protocol.py) and imported by both sides, so
the formats cannot drift. Checksum is an **XOR of all preceding bytes** (sensor: bytes 0–10;
AC: bytes 0–5).

## Configuration

| env var | default | |
|---|---|---|
| `ATOM_GATEWAY_TOKEN` | `dev-gateway-token` | **change this before deploying** |
| `ATOM_CLOUD_DB` | `cloud/cloud.db` | |
| `ATOM_DEFAULT_STORE_ID` | `S001` | |
| `ATOM_GRACE_DAYS` | `30` | grace window for newly seeded stores |

## Chart conventions

Every plot carries **one series** — temperature and humidity get their own charts rather
than sharing a dual axis, and the live view follows one device at a time (a store's devices
interleaved into one line render as a sawtooth, not a trend). Series colors are the first
three slots of a categorical palette validated for colour-vision deficiency in both light
and dark mode. The 1-minute charts are averaged across all devices in the store, which their
subtitles state.
