"""Atom Air -- Store IoT cloud backend.

FastAPI + WebSocket relay hub sitting between store gateways and browsers::

    Browser <--/ws/live?store_id=S001--> Cloud <--/ws/gateway/{store_id}--> Store PC

The gateway dials *out* to the cloud, so stores behind NAT/firewall need no
inbound port forwarding, and AC control rides back down the same socket.

On-demand live stream policy: the cloud counts viewers per store. First viewer
in -> START_LIVE_STREAM to that store's gateway; last viewer out ->
STOP_LIVE_STREAM. 1-minute statistics keep flowing regardless of viewers.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR.parent / "common"))

from protocol import (  # noqa: E402  (the path juggling above is deliberate)
    AC_FAN_NAMES,
    AC_MODE_NAMES,
    SENSOR_SIZE,
    PacketError,
    clamp_target_temp,
    decode_sensor_packet,
    encode_ac_packet,
    normalize_fan,
    normalize_mode,
)

for _stream in (sys.stdout, sys.stderr):   # keep Korean log text readable on Windows
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=os.environ.get("ATOM_LOG_LEVEL", "INFO"),
    format="%(asctime)s  %(levelname)-7s %(name)s  %(message)s",
)
log = logging.getLogger("cloud")

DB_PATH = Path(os.environ.get("ATOM_CLOUD_DB", BASE_DIR / "cloud.db"))
INDEX_HTML = BASE_DIR / "templates" / "index.html"
GATEWAY_TOKEN = os.environ.get("ATOM_GATEWAY_TOKEN", "dev-gateway-token")
DEFAULT_STORE_ID = os.environ.get("ATOM_DEFAULT_STORE_ID", "S001")
DEFAULT_GRACE_DAYS = int(os.environ.get("ATOM_GRACE_DAYS", "30"))
STATS_WINDOW_MINUTES = 180


# --------------------------------------------------------------------------
# time helpers
# --------------------------------------------------------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(value: Any) -> datetime | None:
    """Tolerant parser: ISO-8601 with Z/offset, and sqlite's 'YYYY-MM-DD HH:MM:SS'."""
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# --------------------------------------------------------------------------
# SQLite (WAL) -- callers wrap these in asyncio.to_thread
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS stores (
    store_id            TEXT PRIMARY KEY,
    name                TEXT,
    license_state       TEXT    NOT NULL DEFAULT 'active',   -- active | expired | suspended
    grace_period_days   INTEGER NOT NULL DEFAULT 30,
    license_expires_at  TEXT,
    last_authorized_at  TEXT,
    device_fingerprint  TEXT,
    created_at          TEXT
);

CREATE TABLE IF NOT EXISTS minute_stats (
    store_id     TEXT    NOT NULL,
    dev_id       INTEGER NOT NULL,
    ts           INTEGER NOT NULL,     -- epoch seconds, truncated to the minute
    temp_avg     REAL, temp_min REAL, temp_max REAL,
    hum_avg      REAL, light_avg REAL,
    sample_count INTEGER,
    PRIMARY KEY (store_id, dev_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_minute_stats_lookup ON minute_stats (store_id, ts);

CREATE TABLE IF NOT EXISTS authorize_log (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id TEXT, ts TEXT, result TEXT, detail TEXT
);
"""


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=10.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with closing(_connect()) as con:
        con.executescript(SCHEMA)
        if not con.execute("SELECT COUNT(*) FROM stores").fetchone()[0]:
            con.execute(
                "INSERT INTO stores (store_id, name, license_state, grace_period_days,"
                " license_expires_at, created_at) VALUES (?,?,?,?,?,?)",
                (DEFAULT_STORE_ID, "강남 대로점", "active",
                 DEFAULT_GRACE_DAYS, iso(utcnow() + timedelta(days=365)), iso(utcnow())),
            )
            log.info("seeded demo store %s", DEFAULT_STORE_ID)
        con.commit()
    log.info("sqlite ready at %s", DB_PATH)


def db_store(store_id: str) -> dict | None:
    with closing(_connect()) as con:
        row = con.execute("SELECT * FROM stores WHERE store_id=?", (store_id,)).fetchone()
    return dict(row) if row else None


def db_save_minute_stats(store_id: str, points: Iterable[dict]) -> int:
    rows = []
    for p in points:
        try:
            rows.append((
                store_id, int(p.get("dev_id", 0)), int(p["ts"]) // 60 * 60,
                p.get("temp_avg"), p.get("temp_min"), p.get("temp_max"),
                p.get("hum_avg"), p.get("light_avg"), int(p.get("sample_count") or 0),
            ))
        except (KeyError, TypeError, ValueError):
            log.warning("dropping malformed minute stat: %r", p)
    if not rows:
        return 0
    with closing(_connect()) as con:
        con.executemany(
            "INSERT OR REPLACE INTO minute_stats (store_id, dev_id, ts, temp_avg, temp_min,"
            " temp_max, hum_avg, light_avg, sample_count) VALUES (?,?,?,?,?,?,?,?,?)", rows)
        con.commit()
    return len(rows)


def db_minute_stats(store_id: str, minutes: int = 120) -> list[dict]:
    """Minute stats aggregated across every device in the store, oldest first."""
    since = int(time.time()) - max(1, minutes) * 60
    with closing(_connect()) as con:
        rows = con.execute(
            "SELECT ts, AVG(temp_avg) AS temp_avg, MIN(temp_min) AS temp_min,"
            "       MAX(temp_max) AS temp_max, AVG(hum_avg) AS hum_avg,"
            "       AVG(light_avg) AS light_avg, SUM(sample_count) AS sample_count"
            "  FROM minute_stats WHERE store_id=? AND ts >= ?"
            " GROUP BY ts ORDER BY ts", (store_id, since)).fetchall()
    return [dict(r) for r in rows]


# --------------------------------------------------------------------------
# licensing: daily check + dynamic grace period
# --------------------------------------------------------------------------

def evaluate_license(store: dict | None, store_id: str, now: datetime | None = None) -> dict:
    """Decide whether a store may operate, and how long it may run offline.

    ``grace_period_days`` is the *offline allowance* the gateway persists
    locally: if the cloud becomes unreachable the gateway keeps running that
    many days past its last successful check. For an expired licence the same
    window is measured from ``license_expires_at`` instead.
    """
    now = now or utcnow()
    if store is None:
        return {
            "authorized": False, "status": "unregistered",
            "grace_period_days": DEFAULT_GRACE_DAYS, "license_expires_at": None,
            "grace_expires_at": None, "days_remaining": 0,
            "message": f"등록되지 않은 매장입니다: {store_id}",
        }

    grace_days = int(store.get("grace_period_days") or DEFAULT_GRACE_DAYS)
    state = (store.get("license_state") or "active").lower()
    expires_at = parse_iso(store.get("license_expires_at"))

    # An 'active' licence that has quietly sailed past its expiry date is expired.
    if state == "active" and expires_at and now > expires_at:
        state = "expired"

    if state == "suspended":
        return {
            "authorized": False, "status": "suspended", "grace_period_days": grace_days,
            "license_expires_at": iso(expires_at), "grace_expires_at": None,
            "days_remaining": 0,
            "message": "라이선스가 정지되었습니다. 본사에 문의하세요.",
        }

    if state == "expired":
        grace_until = (expires_at or now) + timedelta(days=grace_days)
        remaining = (grace_until - now).total_seconds() / 86400.0
        if remaining > 0:
            return {
                "authorized": True, "status": "grace", "grace_period_days": grace_days,
                "license_expires_at": iso(expires_at), "grace_expires_at": iso(grace_until),
                "days_remaining": int(remaining),
                "message": f"라이선스 만료. 유예기간 {int(remaining)}일 남았습니다.",
            }
        return {
            "authorized": False, "status": "expired", "grace_period_days": grace_days,
            "license_expires_at": iso(expires_at), "grace_expires_at": iso(grace_until),
            "days_remaining": 0,
            "message": "유예기간이 종료되었습니다. 라이선스를 갱신하세요.",
        }

    return {
        "authorized": True, "status": "active", "grace_period_days": grace_days,
        "license_expires_at": iso(expires_at),
        "grace_expires_at": iso(now + timedelta(days=grace_days)),
        "days_remaining": grace_days,
        "message": "정상 인증되었습니다.",
    }


def db_authorize(store_id: str, fingerprint: str | None, app_version: str | None,
                 requested_grace_days: int | None) -> dict:
    now = utcnow()
    with closing(_connect()) as con:
        row = con.execute("SELECT * FROM stores WHERE store_id=?", (store_id,)).fetchone()
        store = dict(row) if row else None

        if store is not None and requested_grace_days is not None:
            # Operator-tunable window, clamped so a compromised gateway cannot
            # grant itself an unbounded offline licence.
            grace = max(1, min(365, int(requested_grace_days)))
            con.execute("UPDATE stores SET grace_period_days=? WHERE store_id=?",
                        (grace, store_id))
            store["grace_period_days"] = grace

        verdict = evaluate_license(store, store_id, now)

        if store is not None and verdict["authorized"]:
            # Stamping this resets the gateway's local offline grace clock.
            con.execute(
                "UPDATE stores SET last_authorized_at=?,"
                " device_fingerprint=COALESCE(?, device_fingerprint) WHERE store_id=?",
                (iso(now), fingerprint, store_id))
        con.execute(
            "INSERT INTO authorize_log (store_id, ts, result, detail) VALUES (?,?,?,?)",
            (store_id, iso(now), verdict["status"],
             json.dumps({"fingerprint": fingerprint, "app_version": app_version},
                        ensure_ascii=False)))
        con.commit()

    verdict["store_id"] = store_id
    verdict["server_time"] = iso(now)
    verdict["last_authorized_at"] = iso(now) if verdict["authorized"] else (
        (store or {}).get("last_authorized_at"))
    return verdict


def db_set_license(store_id: str, patch: dict) -> dict:
    """Backs the operator endpoint that the verification steps drive."""
    with closing(_connect()) as con:
        row = con.execute("SELECT store_id FROM stores WHERE store_id=?", (store_id,)).fetchone()
        if row is None:
            con.execute(
                "INSERT INTO stores (store_id, name, license_state, grace_period_days,"
                " license_expires_at, created_at) VALUES (?,?,?,?,?,?)",
                (store_id, patch.get("name") or store_id,
                 patch.get("license_state") or "active",
                 int(patch.get("grace_period_days") or DEFAULT_GRACE_DAYS),
                 patch.get("license_expires_at"), iso(utcnow())))
        else:
            sets, args = [], []
            for column in ("name", "license_state", "grace_period_days", "license_expires_at"):
                if patch.get(column) is not None:
                    sets.append(f"{column}=?")
                    args.append(patch[column])
            if sets:
                args.append(store_id)
                con.execute(f"UPDATE stores SET {', '.join(sets)} WHERE store_id=?", args)
        con.commit()
        updated = dict(con.execute("SELECT * FROM stores WHERE store_id=?",
                                   (store_id,)).fetchone())
    return updated


# --------------------------------------------------------------------------
# connection hub
# --------------------------------------------------------------------------

def default_ac_state() -> dict:
    return {"target_id": 1, "power": 0, "mode": "cool", "temp": 24, "fan": "auto",
            "updated_at": None}


@dataclass
class StoreHub:
    store_id: str
    gateway: WebSocket | None = None
    viewers: set[WebSocket] = field(default_factory=set)
    ac_state: dict = field(default_factory=default_ac_state)
    latest: dict | None = None
    live_active: bool = False
    sota: dict | None = None
    gateway_info: dict = field(default_factory=dict)


async def _send_json(ws: WebSocket, payload: dict) -> bool:
    try:
        await ws.send_text(json.dumps(payload, ensure_ascii=False))
        return True
    except Exception:
        return False


class HubManager:
    def __init__(self) -> None:
        self._hubs: dict[str, StoreHub] = {}
        self._lock = asyncio.Lock()

    def get(self, store_id: str) -> StoreHub:
        hub = self._hubs.get(store_id)
        if hub is None:
            hub = self._hubs[store_id] = StoreHub(store_id=store_id)
        return hub

    def snapshot(self, store_id: str) -> dict:
        hub = self._hubs.get(store_id)
        if hub is None:
            return {"store_id": store_id, "gateway_online": False, "viewers": 0,
                    "live_active": False, "ac_state": default_ac_state(), "latest": None,
                    "gateway_info": {}}
        return {"store_id": store_id, "gateway_online": hub.gateway is not None,
                "viewers": len(hub.viewers), "live_active": hub.live_active,
                "ac_state": hub.ac_state, "latest": hub.latest,
                "gateway_info": hub.gateway_info}

    async def broadcast(self, hub: StoreHub, payload: dict) -> None:
        for ws in list(hub.viewers):
            if not await _send_json(ws, payload):
                hub.viewers.discard(ws)

    async def to_gateway(self, hub: StoreHub, payload: dict) -> bool:
        if hub.gateway is None:
            return False
        if await _send_json(hub.gateway, payload):
            return True
        hub.gateway = None
        return False

    async def _refresh_live(self, hub: StoreHub) -> None:
        """Reconcile the on-demand stream with the current viewer count."""
        desired = bool(hub.viewers) and hub.gateway is not None
        if desired == hub.live_active:
            return
        cmd = "START_LIVE_STREAM" if desired else "STOP_LIVE_STREAM"
        sent = await self.to_gateway(hub, {
            "cmd": cmd, "store_id": hub.store_id, "interval_ms": 1000,
            "viewers": len(hub.viewers), "ts": iso(utcnow())})
        if sent:
            hub.live_active = desired
            log.info("[%s] %s (viewers=%d)", hub.store_id, cmd, len(hub.viewers))
        elif not desired:
            hub.live_active = False

    async def add_viewer(self, store_id: str, ws: WebSocket) -> StoreHub:
        async with self._lock:
            hub = self.get(store_id)
            hub.viewers.add(ws)
            await self._refresh_live(hub)
        return hub

    async def remove_viewer(self, hub: StoreHub, ws: WebSocket) -> None:
        async with self._lock:
            hub.viewers.discard(ws)
            await self._refresh_live(hub)

    async def attach_gateway(self, store_id: str, ws: WebSocket, info: dict) -> StoreHub:
        async with self._lock:
            hub = self.get(store_id)
            previous = hub.gateway
            hub.gateway = ws
            hub.gateway_info = info
            hub.live_active = False  # a fresh socket always starts idle
            if previous is not None:
                try:
                    await previous.close(code=4409)
                except Exception:
                    pass
            # Re-issues START immediately if viewers are already waiting.
            await self._refresh_live(hub)
            await self.broadcast(hub, {"type": "gateway_status", "online": True, "info": info})
        return hub

    async def detach_gateway(self, hub: StoreHub, ws: WebSocket) -> None:
        async with self._lock:
            if hub.gateway is ws:
                hub.gateway = None
                hub.live_active = False
                hub.gateway_info = {}
                await self.broadcast(hub, {"type": "gateway_status", "online": False})


manager = HubManager()


# --------------------------------------------------------------------------
# app
# --------------------------------------------------------------------------

AC_MODEL_CATALOG = [
    {"brand": "Samsung", "models": [
        {"id": "samsung-ar09", "name": "AR09TXHZAWK", "protocol": "SAMSUNG_AC"},
        {"id": "samsung-generic", "name": "무풍 시리즈 (범용)",
         "protocol": "SAMSUNG_AC"}]},
    {"brand": "LG", "models": [
        {"id": "lg-s3nq", "name": "휘젠 S3NQ", "protocol": "LG2"},
        {"id": "lg-generic", "name": "LG 범용 (AKB)", "protocol": "LG"}]},
    {"brand": "Daikin", "models": [
        {"id": "daikin-ftxs", "name": "FTXS Series", "protocol": "DAIKIN"},
        {"id": "daikin-arc", "name": "ARC Series", "protocol": "DAIKIN216"}]},
    {"brand": "Mitsubishi", "models": [
        {"id": "mitsubishi-msz", "name": "MSZ Series", "protocol": "MITSUBISHI_AC"}]},
    {"brand": "Carrier", "models": [
        {"id": "carrier-42q", "name": "42Q Series", "protocol": "CARRIER_AC"}]},
]

app = FastAPI(title="Atom Air Cloud", version="1.0.0")


@app.on_event("startup")
async def _startup() -> None:
    await asyncio.to_thread(init_db)


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    if not INDEX_HTML.exists():
        raise HTTPException(status_code=500, detail=f"missing template: {INDEX_HTML}")
    # Read per request so HTML edits show up on refresh without restarting uvicorn.
    return HTMLResponse(INDEX_HTML.read_text(encoding="utf-8"))


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True, "server_time": iso(utcnow()), "default_store_id": DEFAULT_STORE_ID}


@app.get("/api/v1/ac/models")
async def ac_models() -> dict:
    return {"catalog": AC_MODEL_CATALOG}


@app.get("/api/v1/stores/{store_id}/status")
async def store_status(store_id: str) -> dict:
    snap = manager.snapshot(store_id)
    store = await asyncio.to_thread(db_store, store_id)
    snap["license"] = evaluate_license(store, store_id)
    return snap


@app.get("/api/v1/stores/{store_id}/stats")
async def store_stats(store_id: str, minutes: int = Query(120, ge=1, le=1440)) -> dict:
    points = await asyncio.to_thread(db_minute_stats, store_id, minutes)
    return {"store_id": store_id, "minutes": minutes, "points": points}


class AuthorizeRequest(BaseModel):
    store_id: str = Field(..., min_length=1, max_length=64)
    device_fingerprint: str | None = None
    app_version: str | None = None
    requested_grace_days: int | None = Field(None, ge=1, le=365)


@app.post("/api/v1/store/authorize")
async def store_authorize(req: AuthorizeRequest) -> JSONResponse:
    verdict = await asyncio.to_thread(
        db_authorize, req.store_id, req.device_fingerprint, req.app_version,
        req.requested_grace_days)
    log.info("[%s] authorize -> %s (authorized=%s)", req.store_id, verdict["status"],
             verdict["authorized"])
    # Always 200: the gateway must read the grace terms even when refused.
    return JSONResponse(verdict)


class LicensePatch(BaseModel):
    name: str | None = None
    license_state: str | None = Field(None, pattern="^(active|expired|suspended)$")
    grace_period_days: int | None = Field(None, ge=1, le=365)
    license_expires_at: str | None = None


@app.post("/api/v1/stores/{store_id}/license")
async def set_license(store_id: str, patch: LicensePatch) -> dict:
    updated = await asyncio.to_thread(db_set_license, store_id,
                                      patch.model_dump(exclude_none=True))
    return {"store": updated, "evaluated": evaluate_license(updated, store_id)}


# --------------------------------------------------------------------------
# WebSocket: browser
# --------------------------------------------------------------------------

async def handle_viewer_message(hub: StoreHub, ws: WebSocket, msg: dict) -> None:
    kind = msg.get("type")

    if kind == "ping":
        await _send_json(ws, {"type": "pong", "ts": iso(utcnow())})

    elif kind == "ac_control":
        state = dict(hub.ac_state)
        for key in ("target_id", "power", "mode", "temp", "fan"):
            if msg.get(key) is not None:
                state[key] = msg[key]
        try:
            state["target_id"] = int(state["target_id"])
            state["power"] = 1 if int(state["power"]) else 0
            state["mode"] = AC_MODE_NAMES[normalize_mode(state["mode"])]
            state["fan"] = AC_FAN_NAMES[normalize_fan(state["fan"])]
            state["temp"] = clamp_target_temp(state["temp"])
            packet = encode_ac_packet(state["target_id"], state["power"], state["mode"],
                                      state["temp"], state["fan"])
        except (PacketError, KeyError, TypeError, ValueError) as exc:
            await _send_json(ws, {"type": "error", "scope": "ac_control", "message": str(exc)})
            return

        delivered = await manager.to_gateway(hub, {
            "cmd": "AC_CONTROL", "store_id": hub.store_id,
            "packet_hex": packet.hex(), "state": state, "ts": iso(utcnow())})
        if not delivered:
            await _send_json(ws, {
                "type": "error", "scope": "ac_control",
                "message": "게이트웨이가 오프라인입니다. 명령을 전송할 수 없습니다."})
            return

        state["updated_at"] = iso(utcnow())
        hub.ac_state = state
        # Echo to every viewer so a phone and a PC stay in sync.
        await manager.broadcast(hub, {"type": "ac_state", "state": state,
                                      "packet_hex": packet.hex()})

    elif kind == "sota_deploy":
        payload = {"cmd": "DEPLOY_FIRMWARE", "store_id": hub.store_id,
                   "target_id": int(msg.get("target_id") or hub.ac_state["target_id"]),
                   "brand": msg.get("brand"), "model": msg.get("model"),
                   "model_id": msg.get("model_id"), "protocol": msg.get("protocol"),
                   "ts": iso(utcnow())}
        if not await manager.to_gateway(hub, payload):
            await _send_json(ws, {
                "type": "error", "scope": "sota",
                "message": "게이트웨이가 오프라인입니다. 배포할 수 없습니다."})
            return
        hub.sota = {"stage": "requested", "percent": 0, "model": msg.get("model")}
        await manager.broadcast(hub, {
            "type": "sota_progress", "stage": "requested", "percent": 0,
            "message": "배포 요청을 게이트웨이로 전송했습니다.",
            "model": msg.get("model")})

    elif kind == "request_stats":
        minutes = max(1, min(1440, int(msg.get("minutes") or 120)))
        points = await asyncio.to_thread(db_minute_stats, hub.store_id, minutes)
        await _send_json(ws, {"type": "stats", "mode": "replace", "points": points})

    else:
        await _send_json(ws, {"type": "error",
                              "message": f"알 수 없는 메시지 타입: {kind!r}"})


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket, store_id: str = Query(DEFAULT_STORE_ID)) -> None:
    await ws.accept()
    hub = await manager.add_viewer(store_id, ws)
    log.info("[%s] viewer connected (total=%d)", store_id, len(hub.viewers))
    try:
        store = await asyncio.to_thread(db_store, store_id)
        points = await asyncio.to_thread(db_minute_stats, store_id, STATS_WINDOW_MINUTES)
        await _send_json(ws, {
            "type": "hello", "store_id": store_id,
            "store_name": (store or {}).get("name") or store_id,
            "gateway_online": hub.gateway is not None,
            "gateway_info": hub.gateway_info,
            "live_active": hub.live_active,
            "ac_state": hub.ac_state, "latest": hub.latest,
            "license": evaluate_license(store, store_id),
            "server_time": iso(utcnow()), "stats": points,
        })
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await _send_json(ws, {"type": "error",
                                      "message": "JSON 형식이 아닙니다."})
                continue
            if isinstance(msg, dict):
                await handle_viewer_message(hub, ws, msg)
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("[%s] viewer socket error", store_id)
    finally:
        await manager.remove_viewer(hub, ws)
        log.info("[%s] viewer disconnected (total=%d)", store_id, len(hub.viewers))


# --------------------------------------------------------------------------
# WebSocket: store gateway
# --------------------------------------------------------------------------

async def handle_gateway_binary(hub: StoreHub, blob: bytes) -> None:
    """Raw sensor frames. Several 12-byte packets may be coalesced into one frame."""
    if not blob or len(blob) % SENSOR_SIZE:
        log.warning("[%s] dropping %d-byte binary frame", hub.store_id, len(blob))
        return
    now_ms = int(time.time() * 1000)
    for offset in range(0, len(blob), SENSOR_SIZE):
        try:
            reading = decode_sensor_packet(blob[offset:offset + SENSOR_SIZE])
        except PacketError as exc:
            log.warning("[%s] bad sensor packet: %s", hub.store_id, exc)
            continue
        reading["ts"] = now_ms
        hub.latest = reading
        await manager.broadcast(hub, {"type": "live", "data": reading})


async def handle_gateway_json(hub: StoreHub, msg: dict) -> None:
    kind = msg.get("type")

    if kind == "minute_stats":
        points = msg.get("points") or []
        saved = await asyncio.to_thread(db_save_minute_stats, hub.store_id, points)
        if saved:
            await manager.broadcast(hub, {"type": "stats", "mode": "append", "points": points})
            log.info("[%s] stored %d minute stat rows", hub.store_id, saved)

    elif kind == "sota_progress":
        hub.sota = {"stage": msg.get("stage"), "percent": msg.get("percent"),
                    "model": msg.get("model")}
        await manager.broadcast(hub, {
            "type": "sota_progress",
            **{k: msg.get(k) for k in ("stage", "percent", "message", "model", "ok")}})

    elif kind == "ac_ack":
        if isinstance(msg.get("state"), dict):
            hub.ac_state = {**hub.ac_state, **msg["state"], "updated_at": iso(utcnow())}
        await manager.broadcast(hub, {"type": "ac_ack", "ok": msg.get("ok", True),
                                      "state": hub.ac_state, "message": msg.get("message")})

    elif kind == "gateway_status":
        hub.gateway_info = {**hub.gateway_info, **(msg.get("info") or {})}
        await manager.broadcast(hub, {"type": "gateway_status", "online": True,
                                      "info": hub.gateway_info})

    elif kind == "pong":
        pass

    else:
        log.debug("[%s] unhandled gateway message %r", hub.store_id, kind)


@app.websocket("/ws/gateway/{store_id}")
async def ws_gateway(ws: WebSocket, store_id: str, token: str = Query("")) -> None:
    if token != GATEWAY_TOKEN:
        await ws.close(code=4401)
        log.warning("[%s] gateway rejected: bad token", store_id)
        return

    await ws.accept()
    hub = await manager.attach_gateway(store_id, ws, {"connected_at": iso(utcnow())})
    log.info("[%s] gateway connected", store_id)
    try:
        while True:
            message = await ws.receive()
            if message["type"] == "websocket.disconnect":
                break
            if message.get("bytes") is not None:
                await handle_gateway_binary(hub, message["bytes"])
            elif message.get("text") is not None:
                try:
                    payload = json.loads(message["text"])
                except json.JSONDecodeError:
                    log.warning("[%s] gateway sent non-JSON text", store_id)
                    continue
                if isinstance(payload, dict):
                    await handle_gateway_json(hub, payload)
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("[%s] gateway socket error", store_id)
    finally:
        await manager.detach_gateway(hub, ws)
        log.info("[%s] gateway disconnected", store_id)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=os.environ.get("ATOM_HOST", "0.0.0.0"),
                port=int(os.environ.get("ATOM_PORT", "8000")))
