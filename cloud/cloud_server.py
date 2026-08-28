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
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import re
import secrets
import sqlite3
import sys
import time
from collections import deque
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
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
TEMPLATES_DIR = BASE_DIR / "templates"
INDEX_HTML = TEMPLATES_DIR / "index.html"
# Only used to seed stores that predate per-store tokens, so an existing
# deployment keeps working across the upgrade. Gateways authenticate with the
# store's own stores.gateway_token, not with this.
GATEWAY_TOKEN = os.environ.get("ATOM_GATEWAY_TOKEN", "dev-gateway-token")
DEFAULT_STORE_ID = os.environ.get("ATOM_DEFAULT_STORE_ID", "S001")
DEFAULT_GRACE_DAYS = int(os.environ.get("ATOM_GRACE_DAYS", "30"))
STATS_WINDOW_MINUTES = 180

# --- web auth -------------------------------------------------------------
# Store staff sign in with store_id + password (hash kept in the stores row);
# HQ operators sign in on /admin/login with the credentials below.
ADMIN_USERNAME = os.environ.get("ATOM_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("ATOM_ADMIN_PASSWORD", "admin123!")
DEFAULT_STORE_PASSWORD = os.environ.get("ATOM_DEFAULT_STORE_PASSWORD", "1234")
SESSION_COOKIE = "atom_session"
SESSION_TTL_SECONDS = 24 * 3600


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
# web auth: password hashing + in-memory sessions
# --------------------------------------------------------------------------

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"),
                                 bytes.fromhex(salt), 120_000)
    return f"{salt}${digest.hex()}"


def verify_password(password: str, stored: str | None) -> bool:
    if not stored or "$" not in stored:
        return False
    salt, expected = stored.split("$", 1)
    try:
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"),
                                     bytes.fromhex(salt), 120_000)
    except ValueError:
        return False
    return hmac.compare_digest(digest.hex(), expected)


# token -> {"role": "store"|"owner"|"admin", "store_id", ..., "expires": epoch}
# An owner session carries "owner_id" and the owned "store_ids" resolved at
# login. In-memory on purpose: one uvicorn process, a restart re-prompts login.
_sessions: dict[str, dict] = {}


def create_session(role: str, store_id: str | None = None, **fields) -> str:
    token = secrets.token_urlsafe(32)
    _sessions[token] = {"role": role, "store_id": store_id, **fields,
                        "expires": time.time() + SESSION_TTL_SECONDS}
    return token


def get_session(token: str | None) -> dict | None:
    if not token:
        return None
    sess = _sessions.get(token)
    if sess is None:
        return None
    if time.time() > sess["expires"]:
        _sessions.pop(token, None)
        return None
    return sess


def drop_session(token: str | None) -> None:
    if token:
        _sessions.pop(token, None)


def session_of(request: Request) -> dict | None:
    return get_session(request.cookies.get(SESSION_COOKIE))


def require_admin(request: Request) -> dict:
    sess = session_of(request)
    if sess is None or sess["role"] != "admin":
        raise HTTPException(status_code=401, detail="관리자 로그인이 필요합니다.")
    return sess


def session_can_access(sess: dict | None, store_id: str) -> bool:
    """Admin sees every store; an owner their stores; store staff their own."""
    if sess is None:
        return False
    if sess["role"] == "admin":
        return True
    if sess["role"] == "owner":
        return store_id in (sess.get("store_ids") or [])
    return sess["store_id"] == store_id


def require_store_access(request: Request, store_id: str) -> dict:
    sess = session_of(request)
    if sess is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    if not session_can_access(sess, store_id):
        raise HTTPException(status_code=403, detail="다른 매장에는 접근할 수 없습니다.")
    return sess


def actor_of(sess: dict | None) -> dict:
    """Who performed a change, for the settings_history audit trail."""
    if sess is None:
        return {"type": "system", "id": None}
    ident = sess.get("owner_id") or sess.get("store_id") or sess.get("username")
    return {"type": sess["role"], "id": ident}


def set_session_cookie(response: JSONResponse, token: str) -> None:
    response.set_cookie(SESSION_COOKIE, token, max_age=SESSION_TTL_SECONDS,
                        httponly=True, samesite="lax")


# --------------------------------------------------------------------------
# SQLite (WAL) -- callers wrap these in asyncio.to_thread
# --------------------------------------------------------------------------

SCHEMA = """
-- One owner (점주) account holds one or more stores; web login is by owner id
-- (store-code login also works for single-store setups).
CREATE TABLE IF NOT EXISTS owners (
    owner_id      TEXT PRIMARY KEY,       -- login id, e.g. 'ceo_kim'
    name          TEXT NOT NULL,          -- 김대표
    phone         TEXT,
    password_hash TEXT,
    created_at    TEXT
);

CREATE TABLE IF NOT EXISTS stores (
    store_id            TEXT PRIMARY KEY,
    name                TEXT,
    owner_id            TEXT REFERENCES owners(owner_id),
    address             TEXT,
    plan                TEXT,                                 -- 구독 플랜명 (표시용)
    license_state       TEXT    NOT NULL DEFAULT 'active',   -- active | expired | suspended
    grace_period_days   INTEGER NOT NULL DEFAULT 30,
    license_started_at  TEXT,
    license_expires_at  TEXT,
    last_authorized_at  TEXT,
    device_fingerprint  TEXT,
    password_hash       TEXT,                                 -- store web login
    gateway_token       TEXT,                                 -- 이 매장 게이트웨이 전용 토큰
    created_at          TEXT
);

-- Self-service 매장 등록 신청 (from /login). Stays 'pending' until an HQ admin
-- approves it -- approval copies the row into stores (with the password chosen
-- at application time) and only then can the store sign in.
CREATE TABLE IF NOT EXISTS store_applications (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id      TEXT NOT NULL,
    name          TEXT NOT NULL,
    address       TEXT,
    phone         TEXT,
    password_hash TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',   -- pending | approved | rejected
    reason        TEXT,                              -- reject reason, shown on login
    created_at    TEXT,
    decided_at    TEXT,
    decided_by    TEXT
);
CREATE INDEX IF NOT EXISTS idx_applications_status ON store_applications (status, id);

-- Final desired AC state per device: the single row a dashboard restores from.
-- The 'devices' table stays identity/metadata; this is intent, written on every
-- accepted control command and survives server restarts.
CREATE TABLE IF NOT EXISTS ac_settings (
    store_id    TEXT    NOT NULL,
    dev_id      INTEGER NOT NULL,
    power       INTEGER NOT NULL DEFAULT 0,
    mode        TEXT    NOT NULL DEFAULT 'cool',
    target_temp INTEGER NOT NULL DEFAULT 24,
    fan         TEXT    NOT NULL DEFAULT 'auto',
    updated_at  TEXT,
    updated_by  TEXT,                       -- actor id (owner/store/admin)
    PRIMARY KEY (store_id, dev_id)
);

-- Store-wide preferences shown in the web UI (e.g. AI 자동온도 모드).
CREATE TABLE IF NOT EXISTS store_settings (
    store_id          TEXT PRIMARY KEY,
    auto_temp_control INTEGER NOT NULL DEFAULT 0,
    updated_at        TEXT,
    updated_by        TEXT
);

-- Append-only audit trail: every settings/subscription/equipment change.
-- before/after hold only the fields that changed, as JSON.
CREATE TABLE IF NOT EXISTS settings_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT    NOT NULL,
    store_id    TEXT    NOT NULL,
    dev_id      INTEGER,                    -- NULL = store-wide (bulk, license...)
    category    TEXT    NOT NULL,           -- ac | store | device_meta | license | sota | account
    action      TEXT    NOT NULL,           -- ac_control | bulk_control | license_update | ...
    actor_type  TEXT    NOT NULL,           -- owner | store | admin | system
    actor_id    TEXT,
    before_json TEXT,
    after_json  TEXT,
    detail      TEXT                        -- human-readable Korean summary
);
CREATE INDEX IF NOT EXISTS idx_history_store ON settings_history (store_id, id);

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

-- One row per Atom Lite. A device auto-registers the first time a packet
-- carrying its dev_id arrives; name and location are edited from the web.
CREATE TABLE IF NOT EXISTS devices (
    store_id   TEXT    NOT NULL,
    dev_id     INTEGER NOT NULL,
    name       TEXT,
    location   TEXT,
    brand      TEXT,               -- recorded when a SOTA deploy completes
    model      TEXT,
    protocol   TEXT,
    sw_version TEXT,               -- firmware version, once devices report one
    first_seen TEXT,
    last_seen  TEXT,
    PRIMARY KEY (store_id, dev_id)
);

-- 학습 리모컨 레지스트리: 관리자 화면에서 등록/수정. 모든 항목이 학습된
-- ir_codes를 재생한다 -- 기기가 브랜드별 프로토콜을 흉내내는 경로는 없다.
-- kind/protocol 컬럼은 그 시절의 잔재이며 마이그레이션이 모두 'raw'로 맞춘다.
CREATE TABLE IF NOT EXISTS ac_models (
    model_id   TEXT PRIMARY KEY,             -- slug, URL과 파일명에 쓰임
    brand      TEXT NOT NULL,
    name       TEXT NOT NULL,
    kind       TEXT NOT NULL DEFAULT 'raw',   -- 항상 'raw' (구버전 호환용)
    protocol   TEXT,                          -- 미사용 (구버전 호환용)
    notes      TEXT,
    created_at TEXT,
    updated_at TEXT
);

-- 리모컨에서 학습한 raw IR 타이밍. slot은 전체 상태 키: 'off', 'cool_24', ...
-- (에어컨 리모컨은 버튼이 아니라 전체 상태를 전송하므로 상태 조합별 1행)
CREATE TABLE IF NOT EXISTS ir_codes (
    model_id     TEXT    NOT NULL,
    slot         TEXT    NOT NULL,
    freq_khz     INTEGER NOT NULL DEFAULT 38,
    length       INTEGER NOT NULL,           -- mark/space entry count
    raw_json     TEXT    NOT NULL,           -- JSON array of uint16 microseconds
    captured_at  TEXT,
    captured_by  TEXT,                       -- admin actor id
    source_store TEXT,                       -- where the capture device lived
    source_dev   INTEGER,
    PRIMARY KEY (model_id, slot)
);
"""

DEVICE_COLUMNS = ("dev_id, name, location, brand, model, model_id, protocol,"
                  " sw_version, sort_order, first_seen, last_seen")
# NULL sort_order falls back to dev_id, so unordered devices keep their old spot.
DEVICE_ORDER_BY = " ORDER BY COALESCE(sort_order, dev_id), dev_id"


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
        # Older databases predate some columns; ALTER them in.
        migrations = {
            "stores": {"password_hash": "TEXT", "owner_id": "TEXT",
                       "address": "TEXT", "plan": "TEXT", "phone": "TEXT",
                       "gateway_ip": "TEXT", "license_started_at": "TEXT",
                       "gateway_token": "TEXT"},
            "devices": {"sw_version": "TEXT", "sort_order": "INTEGER",
                        "model_id": "TEXT"},
        }
        for table, wanted in migrations.items():
            columns = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
            for column, sqltype in wanted.items():
                if column not in columns:
                    con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {sqltype}")
        if not con.execute("SELECT COUNT(*) FROM stores").fetchone()[0]:
            con.execute(
                "INSERT INTO stores (store_id, name, license_state, grace_period_days,"
                " license_expires_at, password_hash, created_at) VALUES (?,?,?,?,?,?,?)",
                (DEFAULT_STORE_ID, "강남 대로점", "active",
                 DEFAULT_GRACE_DAYS, iso(utcnow() + timedelta(days=365)),
                 hash_password(DEFAULT_STORE_PASSWORD), iso(utcnow())),
            )
            log.info("seeded demo store %s", DEFAULT_STORE_ID)
        # Any store without a password gets the default one so its staff can
        # still sign in; the admin screen can reset it later.
        for row in con.execute(
                "SELECT store_id FROM stores WHERE password_hash IS NULL").fetchall():
            con.execute("UPDATE stores SET password_hash=? WHERE store_id=?",
                        (hash_password(DEFAULT_STORE_PASSWORD), row["store_id"]))
            log.warning("store %s had no password; set to the default store password",
                        row["store_id"])
        # Existing stores keep the shared token they already authenticate with,
        # so nothing that works today stops working; only stores created from
        # here on get a unique one. Regenerate from the admin console to cut a
        # store over.
        for row in con.execute(
                "SELECT store_id FROM stores WHERE gateway_token IS NULL"
                " OR gateway_token=''").fetchall():
            con.execute("UPDATE stores SET gateway_token=? WHERE store_id=?",
                        (GATEWAY_TOKEN, row["store_id"]))
            log.warning("store %s had no gateway token; seeded with the shared one",
                        row["store_id"])
        # There is no brand catalog to seed any more: control is entirely by
        # replaying a remote the installer captured, so a model row only means
        # anything once someone has learned into it. Rows written under the old
        # protocol scheme are converted rather than deleted -- they keep their
        # name and simply need learning before they can drive anything.
        converted = con.execute(
            "UPDATE ac_models SET kind='raw', protocol=NULL"
            " WHERE kind!='raw' OR protocol IS NOT NULL").rowcount
        if converted:
            log.warning("converted %d protocol-based AC model(s) to learned-remote;"
                        " they need IR learning before they can control anything",
                        converted)
        con.commit()
    log.info("sqlite ready at %s", DB_PATH)


def db_store(store_id: str) -> dict | None:
    with closing(_connect()) as con:
        row = con.execute("SELECT * FROM stores WHERE store_id=?", (store_id,)).fetchone()
    return dict(row) if row else None


def db_list_stores() -> list[dict]:
    """Every store row plus owner and device count, for the admin screen."""
    with closing(_connect()) as con:
        rows = con.execute(
            "SELECT s.store_id, s.name, s.owner_id, o.name AS owner_name,"
            "       s.address, s.phone, s.gateway_ip, s.gateway_token, s.plan,"
            "       s.license_state, s.grace_period_days,"
            "       s.license_started_at, s.license_expires_at,"
            "       s.last_authorized_at, s.created_at,"
            "       s.password_hash IS NOT NULL AS has_password,"
            "       (SELECT COUNT(*) FROM devices d WHERE d.store_id = s.store_id)"
            "         AS device_count"
            "  FROM stores s LEFT JOIN owners o ON o.owner_id = s.owner_id"
            " ORDER BY s.store_id").fetchall()
    return [dict(r) for r in rows]


def new_gateway_token() -> str:
    """A store's gateway credential. It is the only thing the gateway sends, so
    it has to be unguessable on its own."""
    return "gw_" + secrets.token_urlsafe(24)


def db_stores_by_gateway_token(token: str) -> list[dict]:
    """Every store this gateway token opens -- normally one.

    More than one is possible right after the upgrade, because stores that
    predate per-store tokens were all backfilled with the shared one. Callers
    must treat that as ambiguous rather than picking a winner. Compared in
    constant time across every candidate so a caller cannot time their way to a
    valid token.
    """
    if not token:
        return []
    with closing(_connect()) as con:
        rows = con.execute(
            "SELECT * FROM stores WHERE gateway_token IS NOT NULL"
            " AND gateway_token != ''").fetchall()
    return [dict(r) for r in rows
            if secrets.compare_digest(r["gateway_token"], token)]


def resolve_gateway_store(token: str, want_store: str | None) -> tuple[dict | None, str]:
    """Pick the store a connecting gateway belongs to.

    Returns (store, problem). ``want_store`` disambiguates a shared token, which
    is exactly what --store-id is for while a deployment is still being cut over
    to per-store tokens.
    """
    matches = db_stores_by_gateway_token(token)
    if not matches:
        return None, "알 수 없는 게이트웨이 토큰입니다."
    if want_store:
        for store in matches:
            if store["store_id"] == want_store:
                return store, ""
        return None, (f"이 토큰은 {want_store} 의 것이 아닙니다 "
                      f"(해당 토큰의 매장: {', '.join(m['store_id'] for m in matches)}).")
    if len(matches) > 1:
        return None, ("이 토큰을 여러 매장이 함께 쓰고 있어 매장을 특정할 수 없습니다: "
                      + ", ".join(m["store_id"] for m in matches)
                      + ". 관리자 콘솔에서 토큰을 재발급하거나 --store-id 로 지정하세요.")
    return matches[0], ""


def db_regenerate_gateway_token(store_id: str) -> str | None:
    token = new_gateway_token()
    with closing(_connect()) as con:
        changed = con.execute("UPDATE stores SET gateway_token=? WHERE store_id=?",
                              (token, store_id)).rowcount
        con.commit()
    return token if changed else None


def db_create_store(store_id: str, name: str, password: str, grace_days: int,
                    expires_at: str | None, owner_id: str | None = None,
                    address: str | None = None, plan: str | None = None) -> dict:
    now = iso(utcnow())
    with closing(_connect()) as con:
        if con.execute("SELECT 1 FROM stores WHERE store_id=?", (store_id,)).fetchone():
            raise ValueError(f"이미 등록된 매장입니다: {store_id}")
        if owner_id and not con.execute("SELECT 1 FROM owners WHERE owner_id=?",
                                        (owner_id,)).fetchone():
            raise ValueError(f"등록되지 않은 점주 계정입니다: {owner_id}")
        con.execute(
            "INSERT INTO stores (store_id, name, owner_id, address, plan,"
            " license_state, grace_period_days, license_started_at,"
            " license_expires_at, password_hash, gateway_token, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (store_id, name, owner_id, address, plan, "active", grace_days, now,
             expires_at or iso(utcnow() + timedelta(days=365)),
             hash_password(password), new_gateway_token(), now))
        con.commit()
        row = con.execute("SELECT * FROM stores WHERE store_id=?", (store_id,)).fetchone()
    return dict(row)


def db_set_store_password(store_id: str, password: str) -> bool:
    with closing(_connect()) as con:
        cur = con.execute("UPDATE stores SET password_hash=? WHERE store_id=?",
                          (hash_password(password), store_id))
        con.commit()
    return cur.rowcount > 0


def db_set_owner_password(owner_id: str, password: str) -> bool:
    with closing(_connect()) as con:
        cur = con.execute("UPDATE owners SET password_hash=? WHERE owner_id=?",
                          (hash_password(password), owner_id))
        con.commit()
    return cur.rowcount > 0


# --------------------------------------------------------------------------
# store applications: 매장 등록 신청 -> 관리자 승인 -> stores row
# --------------------------------------------------------------------------

# password_hash deliberately excluded from listings.
APPLICATION_COLUMNS = ("id, store_id, name, address, phone, status, reason,"
                       " created_at, decided_at, decided_by")


def db_create_application(store_id: str, name: str, password: str,
                          address: str | None, phone: str | None) -> dict:
    with closing(_connect()) as con:
        if con.execute("SELECT 1 FROM stores WHERE store_id=?", (store_id,)).fetchone():
            raise ValueError(f"이미 사용 중인 매장 ID입니다: {store_id}")
        if con.execute("SELECT 1 FROM store_applications WHERE store_id=?"
                       " AND status='pending'", (store_id,)).fetchone():
            raise ValueError(f"이미 승인 대기 중인 신청입니다: {store_id}")
        cur = con.execute(
            "INSERT INTO store_applications (store_id, name, address, phone,"
            " password_hash, status, created_at) VALUES (?,?,?,?,?,'pending',?)",
            (store_id, name, address, phone, hash_password(password), iso(utcnow())))
        con.commit()
        row = con.execute(
            f"SELECT {APPLICATION_COLUMNS} FROM store_applications WHERE id=?",
            (cur.lastrowid,)).fetchone()
    return dict(row)


def db_application_for_login(store_id: str) -> dict | None:
    """Newest application for a store code, hash included (login status hints)."""
    with closing(_connect()) as con:
        row = con.execute(
            "SELECT * FROM store_applications WHERE store_id=?"
            " ORDER BY id DESC LIMIT 1", (store_id,)).fetchone()
    return dict(row) if row else None


def db_list_applications(status: str | None = None, limit: int = 100) -> list[dict]:
    where, args = "", []
    if status:
        where = " WHERE status=?"
        args.append(status)
    with closing(_connect()) as con:
        rows = con.execute(
            f"SELECT {APPLICATION_COLUMNS} FROM store_applications{where}"
            " ORDER BY (status='pending') DESC, id DESC LIMIT ?",
            args + [max(1, min(500, limit))]).fetchall()
    return [dict(r) for r in rows]


def db_decide_application(app_id: int, approve: bool, decided_by: str | None,
                          reason: str | None = None) -> dict:
    """Approve (creates the stores row) or reject one pending application."""
    now = iso(utcnow())
    with closing(_connect()) as con:
        row = con.execute("SELECT * FROM store_applications WHERE id=?",
                          (app_id,)).fetchone()
        if row is None:
            raise LookupError("존재하지 않는 신청입니다.")
        application = dict(row)
        if application["status"] != "pending":
            raise ValueError("이미 처리된 신청입니다.")
        if approve:
            # A store may have been registered by other means since the apply.
            if con.execute("SELECT 1 FROM stores WHERE store_id=?",
                           (application["store_id"],)).fetchone():
                raise ValueError(f"이미 등록된 매장 ID입니다: {application['store_id']}")
            con.execute(
                "INSERT INTO stores (store_id, name, address, phone, plan,"
                " license_state, grace_period_days, license_started_at,"
                " license_expires_at, password_hash, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (application["store_id"], application["name"], application["address"],
                 application["phone"], "Standard", "active", DEFAULT_GRACE_DAYS, now,
                 iso(utcnow() + timedelta(days=365)),
                 application["password_hash"], now))
        con.execute(
            "UPDATE store_applications SET status=?, reason=?, decided_at=?,"
            " decided_by=? WHERE id=?",
            ("approved" if approve else "rejected", reason, now, decided_by, app_id))
        con.commit()
    application.update(status="approved" if approve else "rejected", reason=reason,
                       decided_at=now, decided_by=decided_by)
    application.pop("password_hash", None)
    return application


# --------------------------------------------------------------------------
# owners (점주): one login, many stores
# --------------------------------------------------------------------------

def db_owner(owner_id: str) -> dict | None:
    with closing(_connect()) as con:
        row = con.execute("SELECT * FROM owners WHERE owner_id=?", (owner_id,)).fetchone()
    return dict(row) if row else None


def db_owner_store_ids(owner_id: str) -> list[str]:
    with closing(_connect()) as con:
        rows = con.execute("SELECT store_id FROM stores WHERE owner_id=? ORDER BY store_id",
                           (owner_id,)).fetchall()
    return [r["store_id"] for r in rows]


def db_create_owner(owner_id: str, name: str, password: str,
                    phone: str | None = None) -> dict:
    with closing(_connect()) as con:
        if con.execute("SELECT 1 FROM owners WHERE owner_id=?", (owner_id,)).fetchone():
            raise ValueError(f"이미 등록된 점주 계정입니다: {owner_id}")
        con.execute(
            "INSERT INTO owners (owner_id, name, phone, password_hash, created_at)"
            " VALUES (?,?,?,?,?)",
            (owner_id, name, phone, hash_password(password), iso(utcnow())))
        con.commit()
        row = con.execute("SELECT * FROM owners WHERE owner_id=?", (owner_id,)).fetchone()
    return dict(row)


def db_list_owners() -> list[dict]:
    with closing(_connect()) as con:
        rows = con.execute(
            "SELECT o.owner_id, o.name, o.phone, o.created_at,"
            "       COUNT(s.store_id) AS store_count"
            "  FROM owners o LEFT JOIN stores s ON s.owner_id = o.owner_id"
            " GROUP BY o.owner_id ORDER BY o.owner_id").fetchall()
    return [dict(r) for r in rows]


# --------------------------------------------------------------------------
# persisted settings + audit trail
# --------------------------------------------------------------------------

def db_ac_settings(store_id: str) -> dict[int, dict]:
    """Final desired AC state per device, shaped like a hub ac_state."""
    with closing(_connect()) as con:
        rows = con.execute(
            "SELECT dev_id, power, mode, target_temp, fan, updated_at"
            "  FROM ac_settings WHERE store_id=?", (store_id,)).fetchall()
    return {r["dev_id"]: {
        "target_id": r["dev_id"], "power": r["power"], "mode": r["mode"],
        "temp": r["target_temp"], "fan": r["fan"], "updated_at": r["updated_at"],
    } for r in rows}


def db_save_ac_setting(store_id: str, dev_id: int, state: dict,
                       actor_id: str | None) -> None:
    with closing(_connect()) as con:
        con.execute(
            "INSERT INTO ac_settings (store_id, dev_id, power, mode, target_temp,"
            " fan, updated_at, updated_by) VALUES (?,?,?,?,?,?,?,?)"
            " ON CONFLICT(store_id, dev_id) DO UPDATE SET"
            "  power=excluded.power, mode=excluded.mode,"
            "  target_temp=excluded.target_temp, fan=excluded.fan,"
            "  updated_at=excluded.updated_at, updated_by=excluded.updated_by",
            (store_id, dev_id, int(state["power"]), state["mode"],
             int(state["temp"]), state["fan"], iso(utcnow()), actor_id))
        con.commit()


def db_store_settings(store_id: str) -> dict:
    with closing(_connect()) as con:
        row = con.execute("SELECT auto_temp_control, updated_at FROM store_settings"
                          " WHERE store_id=?", (store_id,)).fetchone()
    if row is None:
        return {"auto_temp_control": False, "updated_at": None}
    return {"auto_temp_control": bool(row["auto_temp_control"]),
            "updated_at": row["updated_at"]}


def db_save_store_settings(store_id: str, auto_temp_control: bool,
                           actor_id: str | None) -> None:
    with closing(_connect()) as con:
        con.execute(
            "INSERT INTO store_settings (store_id, auto_temp_control, updated_at,"
            " updated_by) VALUES (?,?,?,?)"
            " ON CONFLICT(store_id) DO UPDATE SET"
            "  auto_temp_control=excluded.auto_temp_control,"
            "  updated_at=excluded.updated_at, updated_by=excluded.updated_by",
            (store_id, int(auto_temp_control), iso(utcnow()), actor_id))
        con.commit()


def db_log_history(store_id: str, category: str, action: str, actor: dict,
                   dev_id: int | None = None, before: dict | None = None,
                   after: dict | None = None, detail: str | None = None) -> None:
    """Append one audit row. before/after carry only the changed fields."""
    dump = lambda d: json.dumps(d, ensure_ascii=False) if d else None  # noqa: E731
    with closing(_connect()) as con:
        con.execute(
            "INSERT INTO settings_history (ts, store_id, dev_id, category, action,"
            " actor_type, actor_id, before_json, after_json, detail)"
            " VALUES (?,?,?,?,?,?,?,?,?,?)",
            (iso(utcnow()), store_id, dev_id, category, action,
             actor.get("type") or "system", actor.get("id"),
             dump(before), dump(after), detail))
        con.commit()


def db_history(store_id: str | None = None, category: str | None = None,
               limit: int = 100) -> list[dict]:
    """Newest first; store_id=None means every store (admin view)."""
    where, args = [], []
    if store_id:
        where.append("store_id=?")
        args.append(store_id)
    if category:
        where.append("category=?")
        args.append(category)
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    with closing(_connect()) as con:
        rows = con.execute(
            f"SELECT * FROM settings_history{clause} ORDER BY id DESC LIMIT ?",
            args + [max(1, min(500, limit))]).fetchall()
    out = []
    for r in rows:
        entry = dict(r)
        for key in ("before_json", "after_json"):
            text = entry.pop(key)
            entry[key.removesuffix("_json")] = json.loads(text) if text else None
        out.append(entry)
    return out


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


def db_minute_stats(store_id: str, minutes: int = 120,
                    bucket: int = 60, dev_id: int | None = None) -> list[dict]:
    """Stats aggregated across every device in the store (or one device when
    ``dev_id`` is given), oldest first.

    ``bucket`` (seconds) coarsens long ranges so a 30-day query returns
    hundreds of points, not 43k -- rows are grouped onto bucket boundaries.
    """
    since = int(time.time()) - max(1, minutes) * 60
    b = max(60, int(bucket))
    dev_filter = " AND dev_id=?" if dev_id is not None else ""
    params: list = [b, b, store_id, since]
    if dev_id is not None:
        params.append(dev_id)
    with closing(_connect()) as con:
        rows = con.execute(
            "SELECT (ts / ?) * ? AS tb, AVG(temp_avg) AS temp_avg,"
            "       MIN(temp_min) AS temp_min, MAX(temp_max) AS temp_max,"
            "       AVG(hum_avg) AS hum_avg, AVG(light_avg) AS light_avg,"
            "       SUM(sample_count) AS sample_count"
            f"  FROM minute_stats WHERE store_id=? AND ts >= ?{dev_filter}"
            " GROUP BY tb ORDER BY tb", params).fetchall()
    out = []
    for r in rows:
        entry = dict(r)
        entry["ts"] = entry.pop("tb")
        out.append(entry)
    return out


def stats_bucket_for(minutes: int) -> int:
    """1-minute points up to 6h, 10-minute up to 3 days, hourly beyond."""
    if minutes <= 360:
        return 60
    if minutes <= 1440 * 3:
        return 600
    return 3600


# --------------------------------------------------------------------------
# device registry
# --------------------------------------------------------------------------

def db_devices(store_id: str) -> list[dict]:
    with closing(_connect()) as con:
        rows = con.execute(
            f"SELECT {DEVICE_COLUMNS}"
            f"  FROM devices WHERE store_id=?{DEVICE_ORDER_BY}", (store_id,)).fetchall()
    return [dict(r) for r in rows]


def db_set_device_order(store_id: str, order: list[int]) -> list[dict]:
    """Persist the display order: sort_order = position in ``order``."""
    now = iso(utcnow())
    with closing(_connect()) as con:
        for position, dev_id in enumerate(order):
            # A device the cloud has not seen a packet from yet can still be placed.
            con.execute(
                "INSERT OR IGNORE INTO devices (store_id, dev_id, name, first_seen)"
                " VALUES (?,?,?,?)",
                (store_id, dev_id, f"디바이스 {dev_id}", now))
            con.execute("UPDATE devices SET sort_order=? WHERE store_id=? AND dev_id=?",
                        (position, store_id, dev_id))
        con.commit()
        rows = con.execute(
            f"SELECT {DEVICE_COLUMNS}"
            f"  FROM devices WHERE store_id=?{DEVICE_ORDER_BY}", (store_id,)).fetchall()
    return [dict(r) for r in rows]


def db_register_device(store_id: str, dev_id: int) -> dict:
    """Create the row for a newly seen device, or just refresh last_seen."""
    now = iso(utcnow())
    with closing(_connect()) as con:
        con.execute(
            "INSERT INTO devices (store_id, dev_id, name, first_seen, last_seen)"
            " VALUES (?,?,?,?,?)"
            " ON CONFLICT(store_id, dev_id) DO UPDATE SET last_seen=excluded.last_seen",
            (store_id, dev_id, f"디바이스 {dev_id}", now, now))
        con.commit()
        row = con.execute(
            f"SELECT {DEVICE_COLUMNS}"
            "  FROM devices WHERE store_id=? AND dev_id=?", (store_id, dev_id)).fetchone()
    return dict(row)


def db_device_last_stat(store_id: str, dev_id: int) -> int | None:
    """Newest minute-stat timestamp for one device (epoch seconds), if any."""
    with closing(_connect()) as con:
        row = con.execute(
            "SELECT MAX(ts) AS ts FROM minute_stats WHERE store_id=? AND dev_id=?",
            (store_id, dev_id)).fetchone()
    return row["ts"] if row and row["ts"] is not None else None


def db_delete_device(store_id: str, dev_id: int) -> bool:
    """Drop a device's registry row and its saved AC state.

    minute_stats is deliberately left alone: those readings are the store's
    environment record for the year, not a property of the card. If the same
    dev_id ever registers again it simply picks its own history back up.
    """
    with closing(_connect()) as con:
        cur = con.execute("DELETE FROM devices WHERE store_id=? AND dev_id=?",
                          (store_id, dev_id))
        con.execute("DELETE FROM ac_settings WHERE store_id=? AND dev_id=?",
                    (store_id, dev_id))
        con.commit()
    return cur.rowcount > 0


def db_update_device(store_id: str, dev_id: int, patch: dict) -> dict | None:
    """Set any of name / location / brand / model / model_id / protocol."""
    columns = [c for c in ("name", "location", "brand", "model", "model_id",
                           "protocol", "sw_version")
               if patch.get(c) is not None]
    with closing(_connect()) as con:
        # An operator can name a device the cloud has not seen a packet from yet.
        con.execute(
            "INSERT OR IGNORE INTO devices (store_id, dev_id, name, first_seen)"
            " VALUES (?,?,?,?)",
            (store_id, dev_id, f"디바이스 {dev_id}", iso(utcnow())))
        if columns:
            assignments = ", ".join(f"{c}=?" for c in columns)
            con.execute(f"UPDATE devices SET {assignments} WHERE store_id=? AND dev_id=?",
                        [patch[c] for c in columns] + [store_id, dev_id])
        con.commit()
        row = con.execute(
            f"SELECT {DEVICE_COLUMNS}"
            "  FROM devices WHERE store_id=? AND dev_id=?", (store_id, dev_id)).fetchone()
    return dict(row) if row else None


# --------------------------------------------------------------------------
# AC model registry + learned IR codes
# --------------------------------------------------------------------------

# Every state combination a raw-mode device can express. An AC remote sends the
# whole state per keypress, so each combination is one learned code ("slot").
# One slot per remote *button*, not per state combination. An installer presses
# nine buttons instead of stepping a remote through 27 settings, and the device
# reaches a requested state by replaying the buttons that get it there.
#
# This assumes the remote sends a discrete command per button. Many AC remotes
# instead transmit their whole state on every press -- on those, replaying the
# "temp up" capture re-sends the temperature it was captured at rather than
# incrementing, which the installer will see the first time they test it.
IR_SLOTS = ["power_on", "power_off",
            "mode_cool", "mode_heat", "mode_dry",
            "temp_up", "temp_down",
            "fan_up", "fan_down"]
# Capture sanity bounds; anything outside is a mis-read, not a remote.
IR_RAW_MIN_LEN, IR_RAW_MAX_LEN = 20, 1023
IR_RAW_MIN_US, IR_RAW_MAX_US = 10, 65000
IR_FREQ_MIN_KHZ, IR_FREQ_MAX_KHZ = 30, 60
# devices.protocol sentinel for raw-replay devices.
RAW_PROTOCOL = "RAW"


def slots_for_state(power: int, mode: str, temp: int) -> list[str]:
    """The buttons a device needs before it can express this state at all.

    Only the unconditional ones: temp and fan are reached by repeating their
    step buttons, and how many presses that takes depends on where the device
    currently is, which only the device knows.
    """
    if not power:
        return ["power_off"]
    needed = ["power_on"]
    if mode in ("cool", "heat", "dry"):
        needed.append(f"mode_{mode}")
    return needed


def _slugify_model_id(brand: str, name: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", f"{brand} {name}".lower()).strip("-")
    return base or "model"


def db_ac_catalog() -> list[dict]:
    """Brand-grouped catalog of learned remotes.

    Every entry reports protocol "RAW" and its captured slot list, so the
    dashboards can gate the remote UI to the combinations actually learned.
    "brand" is just how the installer chose to file it -- nothing reads it.
    """
    with closing(_connect()) as con:
        models = con.execute(
            "SELECT model_id, brand, name FROM ac_models"
            " ORDER BY brand, name").fetchall()
        code_rows = con.execute(
            "SELECT model_id, slot FROM ir_codes ORDER BY model_id, slot").fetchall()
    captured: dict[str, list[str]] = {}
    for row in code_rows:
        captured.setdefault(row["model_id"], []).append(row["slot"])
    grouped: dict[str, list[dict]] = {}
    for m in models:
        grouped.setdefault(m["brand"], []).append({
            "id": m["model_id"], "name": m["name"], "kind": "raw",
            "protocol": RAW_PROTOCOL,
            "slots": captured.get(m["model_id"], []),
            "slot_total": len(IR_SLOTS)})
    return [{"brand": brand, "models": entries}
            for brand, entries in grouped.items()]


def db_ac_model(model_id: str) -> dict | None:
    with closing(_connect()) as con:
        row = con.execute("SELECT * FROM ac_models WHERE model_id=?",
                          (model_id,)).fetchone()
        if row is None:
            return None
        slots = [r["slot"] for r in con.execute(
            "SELECT slot FROM ir_codes WHERE model_id=? ORDER BY slot",
            (model_id,)).fetchall()]
    model = dict(row)
    model["slots"] = slots
    return model


def db_create_ac_model(brand: str, name: str, notes: str | None) -> dict:
    now = iso(utcnow())
    with closing(_connect()) as con:
        base = _slugify_model_id(brand, name)
        model_id, suffix = base, 2
        while con.execute("SELECT 1 FROM ac_models WHERE model_id=?",
                          (model_id,)).fetchone():
            model_id, suffix = f"{base}-{suffix}", suffix + 1
        con.execute(
            "INSERT INTO ac_models (model_id, brand, name, kind, protocol,"
            " notes, created_at, updated_at) VALUES (?,?,?,'raw',NULL,?,?,?)",
            (model_id, brand, name, notes, now, now))
        con.commit()
    return db_ac_model(model_id)


def db_update_ac_model(model_id: str, patch: dict) -> dict | None:
    columns = [c for c in ("brand", "name", "notes")
               if patch.get(c) is not None]
    with closing(_connect()) as con:
        if not con.execute("SELECT 1 FROM ac_models WHERE model_id=?",
                           (model_id,)).fetchone():
            return None
        if columns:
            assignments = ", ".join(f"{c}=?" for c in columns)
            con.execute(
                f"UPDATE ac_models SET {assignments}, updated_at=? WHERE model_id=?",
                [patch[c] for c in columns] + [iso(utcnow()), model_id])
            con.commit()
    return db_ac_model(model_id)


def db_delete_ac_model(model_id: str) -> None:
    with closing(_connect()) as con:
        if not con.execute("SELECT 1 FROM ac_models WHERE model_id=?",
                           (model_id,)).fetchone():
            raise LookupError(f"등록되지 않은 모델입니다: {model_id}")
        used = con.execute(
            "SELECT COUNT(*) FROM devices WHERE model_id=?",
            (model_id,)).fetchone()[0]
        if used:
            raise ValueError(f"{used}대의 장비가 이 모델을 사용 중입니다. 먼저 장비를 재배포하세요.")
        con.execute("DELETE FROM ir_codes WHERE model_id=?", (model_id,))
        con.execute("DELETE FROM ac_models WHERE model_id=?", (model_id,))
        con.commit()


def db_ir_code_list(model_id: str) -> list[dict]:
    """Slot metadata for the admin grid -- raw arrays stay out of the payload."""
    with closing(_connect()) as con:
        rows = con.execute(
            "SELECT slot, freq_khz, length, captured_at, captured_by,"
            "       source_store, source_dev"
            "  FROM ir_codes WHERE model_id=? ORDER BY slot", (model_id,)).fetchall()
    return [dict(r) for r in rows]


def db_save_ir_code(model_id: str, slot: str, freq_khz: int, raw: list[int],
                    captured_by: str | None, source_store: str | None,
                    source_dev: int | None) -> None:
    with closing(_connect()) as con:
        con.execute(
            "INSERT INTO ir_codes (model_id, slot, freq_khz, length, raw_json,"
            " captured_at, captured_by, source_store, source_dev)"
            " VALUES (?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(model_id, slot) DO UPDATE SET"
            "   freq_khz=excluded.freq_khz, length=excluded.length,"
            "   raw_json=excluded.raw_json, captured_at=excluded.captured_at,"
            "   captured_by=excluded.captured_by,"
            "   source_store=excluded.source_store, source_dev=excluded.source_dev",
            (model_id, slot, freq_khz, len(raw), json.dumps(raw),
             iso(utcnow()), captured_by, source_store, source_dev))
        con.commit()


def db_delete_ir_code(model_id: str, slot: str) -> bool:
    with closing(_connect()) as con:
        cur = con.execute("DELETE FROM ir_codes WHERE model_id=? AND slot=?",
                          (model_id, slot))
        con.commit()
    return cur.rowcount > 0


def db_ir_bundle(model_id: str) -> dict | None:
    """The deployable code bundle the device stores in SPIFFS."""
    with closing(_connect()) as con:
        rows = con.execute(
            "SELECT slot, freq_khz, raw_json FROM ir_codes WHERE model_id=?"
            " ORDER BY slot", (model_id,)).fetchall()
    if not rows:
        return None
    freqs = [r["freq_khz"] for r in rows]
    return {"v": 1, "model_id": model_id,
            # One carrier for the bundle: all codes come from one remote, so
            # take the most common reported frequency.
            "freq_khz": max(set(freqs), key=freqs.count),
            "slots": {r["slot"]: json.loads(r["raw_json"]) for r in rows}}


def db_ac_models_admin() -> list[dict]:
    """Flat model rows with capture/usage counts for the admin table."""
    with closing(_connect()) as con:
        rows = con.execute(
            "SELECT m.model_id, m.brand, m.name, m.kind, m.protocol, m.notes,"
            "       m.created_at, m.updated_at,"
            "  (SELECT COUNT(*) FROM ir_codes c WHERE c.model_id = m.model_id)"
            "       AS captured,"
            "  (SELECT COUNT(*) FROM devices d WHERE d.model_id = m.model_id)"
            "       AS devices_using"
            "  FROM ac_models m ORDER BY m.brand, m.name").fetchall()
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
            # owner_id "" means 배정 해제; a non-empty owner must exist.
            if patch.get("owner_id"):
                if not con.execute("SELECT 1 FROM owners WHERE owner_id=?",
                                   (patch["owner_id"],)).fetchone():
                    raise ValueError(f"등록되지 않은 점주 계정입니다: {patch['owner_id']}")
            if patch.get("gateway_ip"):
                for part in patch["gateway_ip"].split(","):
                    part = part.strip()
                    if not part:
                        continue
                    try:
                        ipaddress.ip_address(part)
                    except ValueError:
                        raise ValueError(f"올바른 IP 형식이 아닙니다: {part}")
            sets, args = [], []
            for column in ("name", "owner_id", "address", "phone", "gateway_ip",
                           "plan", "license_state", "grace_period_days",
                           "license_started_at", "license_expires_at"):
                if patch.get(column) is not None:
                    value = patch[column]
                    if column == "owner_id" and value == "":
                        value = None
                    sets.append(f"{column}=?")
                    args.append(value)
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

def default_ac_state(dev_id: int = 1) -> dict:
    return {"target_id": dev_id, "power": 0, "mode": "cool", "temp": 24, "fan": "auto",
            "updated_at": None}


# How long a device may go quiet before the UI calls it offline.
DEVICE_STALE_SECONDS = 15
# How long a device must produce nothing at all before its card may be retired.
# Minute stats arrive a minute at a time, so this has to clear two of them.
DEVICE_QUIET_SECONDS = 180
# How much board output one card's 디버깅 panel remembers. Enough to cover a
# boot banner plus an OTA, small enough that ten devices cost nothing.
DEVICE_LOG_LINES = 300
# last_seen is refreshed at most this often per device: at 12 devices x 1Hz a
# per-packet write would be a dozen writes a second for no benefit.
LAST_SEEN_WRITE_INTERVAL = 60.0


@dataclass
class StoreHub:
    store_id: str
    gateway: WebSocket | None = None
    viewers: set[WebSocket] = field(default_factory=set)
    # Both keyed by dev_id -- a store holds ten or more Atom Lite units, and one
    # device's setpoint must not overwrite another's.
    ac_states: dict[int, dict] = field(default_factory=dict)
    latest: dict[int, dict] = field(default_factory=dict)
    devices: dict[int, dict] = field(default_factory=dict)   # dev_id -> registry row
    last_seen_written: dict[int, float] = field(default_factory=dict)
    live_active: bool = False
    sota: dict | None = None
    sota_target: dict | None = None   # what the in-flight deploy is flashing
    learn: dict | None = None         # in-flight IR learn session, if any
    gateway_info: dict = field(default_factory=dict)
    # dev_id -> the last DEVICE_LOG_LINES console lines the unit printed. In
    # memory only, and deliberately so: this is what a technician would read off
    # a USB monitor, not an audit record, and it should die with the process.
    logs: dict[int, deque] = field(default_factory=dict)

    def log_for(self, dev_id: int) -> deque:
        buf = self.logs.get(dev_id)
        if buf is None:
            buf = self.logs[dev_id] = deque(maxlen=DEVICE_LOG_LINES)
        return buf

    def ac_state_for(self, dev_id: int) -> dict:
        state = self.ac_states.get(dev_id)
        if state is None:
            state = self.ac_states[dev_id] = default_ac_state(dev_id)
        return state

    def known_device_ids(self) -> list[int]:
        """Every device we have registry metadata or a reading for."""
        return sorted(set(self.devices) | set(self.latest) | set(self.ac_states))

    def device_payload(self) -> list[dict]:
        """The per-device view the browser renders cards from, in display order."""
        def display_key(dev_id: int) -> tuple:
            sort_order = (self.devices.get(dev_id) or {}).get("sort_order")
            return (sort_order if sort_order is not None else dev_id, dev_id)
        return [{
            **(self.devices.get(dev_id) or {"dev_id": dev_id, "name": f"디바이스 {dev_id}"}),
            "dev_id": dev_id,
            "ac_state": self.ac_state_for(dev_id),
            "latest": self.latest.get(dev_id),
        } for dev_id in sorted(self.known_device_ids(), key=display_key)]


def learn_view(hub: StoreHub) -> dict | None:
    """The learn session as shown to pollers; expires it lazily on read.

    No background task: the deadline (device timeout + grace for the MQTT hop)
    is checked whenever someone looks, which the admin poller does every 1.5s.
    """
    learn = hub.learn
    if learn and learn.get("status") == "waiting" \
            and time.time() > learn.get("deadline", 0):
        learn["status"] = "timeout"
        learn["error"] = "장비 응답이 없습니다. 수신기 연결과 장비 상태를 확인하세요."
    return learn


def learn_in_progress(hub: StoreHub) -> bool:
    learn = learn_view(hub)
    return bool(learn and learn.get("status") == "waiting")


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
                    "live_active": False, "devices": [], "gateway_info": {},
                    "sota": None, "learn": None}
        return {"store_id": store_id, "gateway_online": hub.gateway is not None,
                "viewers": len(hub.viewers), "live_active": hub.live_active,
                "devices": hub.device_payload(),
                "gateway_info": hub.gateway_info, "sota": hub.sota,
                "learn": learn_view(hub)}

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

app = FastAPI(title="Atom Air Cloud", version="1.0.0")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


@app.on_event("startup")
async def _startup() -> None:
    await asyncio.to_thread(init_db)


def _page(name: str) -> HTMLResponse:
    path = TEMPLATES_DIR / name
    if not path.exists():
        raise HTTPException(status_code=500, detail=f"missing template: {path}")
    # Read per request so HTML edits show up on refresh without restarting uvicorn.
    return HTMLResponse(path.read_text(encoding="utf-8"))


@app.get("/", response_class=HTMLResponse)
async def index(request: Request, store_id: str | None = Query(None)):
    sess = session_of(request)
    if sess is None:
        return RedirectResponse("/login", status_code=303)
    # Store staff always see their own store; an owner picks among their own
    # stores; an admin may inspect any store via ?store_id=. The resolved id is
    # injected so the page needs no guess.
    if sess["role"] == "store":
        effective = sess["store_id"]
    elif sess["role"] == "owner":
        owned = sess.get("store_ids") or []
        effective = store_id if store_id in owned else (
            owned[0] if owned else DEFAULT_STORE_ID)
    else:
        effective = store_id or DEFAULT_STORE_ID
    if not INDEX_HTML.exists():
        raise HTTPException(status_code=500, detail=f"missing template: {INDEX_HTML}")
    html = INDEX_HTML.read_text(encoding="utf-8").replace(
        '"%%STORE_ID%%"', json.dumps(effective, ensure_ascii=False))
    return HTMLResponse(html)


@app.get("/login")
async def login_page(request: Request):
    sess = session_of(request)
    if sess is not None:
        return RedirectResponse("/" if sess["role"] == "store" else "/admin",
                                status_code=303)
    return _page("login.html")


@app.get("/admin/login")
async def admin_login_page(request: Request):
    sess = session_of(request)
    if sess is not None and sess["role"] == "admin":
        return RedirectResponse("/admin", status_code=303)
    return _page("admin_login.html")


@app.get("/admin")
async def admin_page(request: Request):
    sess = session_of(request)
    if sess is None or sess["role"] != "admin":
        return RedirectResponse("/admin/login", status_code=303)
    return _page("admin.html")


# --------------------------------------------------------------------------
# auth API
# --------------------------------------------------------------------------

class StoreLoginRequest(BaseModel):
    store_id: str = Field(..., min_length=1, max_length=64)   # 매장 ID
    password: str = Field(..., min_length=1, max_length=128)


@app.post("/api/v1/auth/login")
async def auth_login(req: StoreLoginRequest) -> JSONResponse:
    login_id = req.store_id.strip()

    store = await asyncio.to_thread(db_store, login_id)
    if store is None:
        # A code still in the approval queue gets a clear status message --
        # but only with the password chosen at application time, so store
        # codes cannot be probed anonymously.
        application = await asyncio.to_thread(db_application_for_login, login_id)
        if application is not None and await asyncio.to_thread(
                verify_password, req.password, application.get("password_hash")):
            if application["status"] == "pending":
                raise HTTPException(
                    status_code=403,
                    detail="매장 등록 신청이 승인 대기 중입니다."
                           " 관리자 승인 후 로그인할 수 있습니다.")
            if application["status"] == "rejected":
                reason = application.get("reason")
                raise HTTPException(
                    status_code=403,
                    detail="매장 등록 신청이 거절되었습니다. "
                           + (f"사유: {reason}" if reason else "본사에 문의하세요."))
    ok = await asyncio.to_thread(verify_password, req.password,
                                 (store or {}).get("password_hash"))
    if not ok:
        raise HTTPException(status_code=401,
                            detail="아이디 또는 비밀번호가 올바르지 않습니다.")
    token = create_session("store", login_id)
    resp = JSONResponse({"ok": True, "role": "store", "store_id": login_id,
                         "store_name": store.get("name") or login_id})
    set_session_cookie(resp, token)
    log.info("[%s] store web login", login_id)
    return resp


class StoreRegisterRequest(BaseModel):
    store_id: str = Field(..., min_length=2, max_length=32,
                          pattern=r"^[A-Za-z0-9_-]+$")
    name: str = Field(..., min_length=1, max_length=64)
    address: str | None = Field(None, max_length=128)
    phone: str | None = Field(None, max_length=32)
    password: str = Field(..., min_length=4, max_length=64)


@app.post("/api/v1/auth/register")
async def auth_register(req: StoreRegisterRequest) -> dict:
    """Self-service 매장 등록 신청: stays pending until an admin approves it."""
    try:
        application = await asyncio.to_thread(
            db_create_application, req.store_id.strip(), req.name.strip(),
            req.password, (req.address or "").strip() or None,
            (req.phone or "").strip() or None)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    await asyncio.to_thread(
        db_log_history, application["store_id"], "account", "store_apply",
        {"type": "store", "id": application["store_id"]}, None, None,
        {"name": application["name"]},
        f"매장 등록 신청: {application['name']}")
    log.info("[%s] store application submitted", application["store_id"])
    return {"ok": True, "application": application,
            "store_id": application["store_id"],
            "message": f"신청이 접수되었습니다. 관리자 승인 후 매장 ID"
                       f" [{application['store_id']}] 와 설정한 비밀번호로 로그인하세요."}


class AdminLoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=1, max_length=128)


@app.post("/api/v1/auth/admin/login")
async def auth_admin_login(req: AdminLoginRequest) -> JSONResponse:
    if not (hmac.compare_digest(req.username, ADMIN_USERNAME)
            and hmac.compare_digest(req.password, ADMIN_PASSWORD)):
        raise HTTPException(status_code=401,
                            detail="관리자 계정 정보가 올바르지 않습니다.")
    token = create_session("admin", None, username=req.username)
    resp = JSONResponse({"ok": True, "role": "admin"})
    set_session_cookie(resp, token)
    log.info("admin web login (%s)", req.username)
    return resp


class PasswordChangeRequest(BaseModel):
    current_password: str = Field(..., min_length=1, max_length=128)
    new_password: str = Field(..., min_length=4, max_length=64)


@app.post("/api/v1/auth/password")
async def auth_change_password(req: PasswordChangeRequest,
                               request: Request) -> dict:
    """A signed-in store or owner changes its own password."""
    sess = session_of(request)
    if sess is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    if sess["role"] == "store":
        store = await asyncio.to_thread(db_store, sess["store_id"])
        if not await asyncio.to_thread(verify_password, req.current_password,
                                       (store or {}).get("password_hash")):
            raise HTTPException(status_code=403,
                                detail="현재 비밀번호가 올바르지 않습니다.")
        await asyncio.to_thread(db_set_store_password, sess["store_id"],
                                req.new_password)
        await asyncio.to_thread(
            db_log_history, sess["store_id"], "account", "password_change",
            actor_of(sess), None, None, None, "매장 로그인 비밀번호 변경")
        log.info("[%s] store password changed by itself", sess["store_id"])
    elif sess["role"] == "owner":
        owner = await asyncio.to_thread(db_owner, sess["owner_id"])
        if not await asyncio.to_thread(verify_password, req.current_password,
                                       (owner or {}).get("password_hash")):
            raise HTTPException(status_code=403,
                                detail="현재 비밀번호가 올바르지 않습니다.")
        await asyncio.to_thread(db_set_owner_password, sess["owner_id"],
                                req.new_password)
        log.info("[%s] owner password changed by itself", sess["owner_id"])
    else:
        raise HTTPException(status_code=403,
                            detail="관리자 비밀번호는 서버 설정에서 변경합니다.")
    return {"ok": True, "message": "비밀번호가 변경되었습니다."}


@app.post("/api/v1/auth/logout")
async def auth_logout(request: Request) -> JSONResponse:
    drop_session(request.cookies.get(SESSION_COOKIE))
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE)
    return resp


@app.get("/api/v1/auth/me")
async def auth_me(request: Request) -> dict:
    sess = session_of(request)
    if sess is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return {k: v for k, v in sess.items() if k != "expires"}


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True, "server_time": iso(utcnow()), "default_store_id": DEFAULT_STORE_ID}


@app.get("/api/v1/ac/models")
async def ac_models() -> dict:
    return {"catalog": await asyncio.to_thread(db_ac_catalog)}


@app.get("/api/v1/stores/{store_id}/status")
async def store_status(store_id: str) -> dict:
    # Hydrate so the owner's all-stores overview sees registered devices and
    # their persisted settings even when nobody is streaming that store.
    await hydrate_hub(manager.get(store_id))
    snap = manager.snapshot(store_id)
    store = await asyncio.to_thread(db_store, store_id)
    snap["store_name"] = (store or {}).get("name") or store_id
    snap["address"] = (store or {}).get("address")
    snap["plan"] = (store or {}).get("plan")
    snap["license"] = evaluate_license(store, store_id)
    snap["settings"] = await asyncio.to_thread(db_store_settings, store_id)
    return snap


class StoreSettingsPatch(BaseModel):
    auto_temp_control: bool


@app.post("/api/v1/stores/{store_id}/settings")
async def set_store_settings(store_id: str, patch: StoreSettingsPatch,
                             request: Request) -> dict:
    sess = require_store_access(request, store_id)
    actor = actor_of(sess)
    before = await asyncio.to_thread(db_store_settings, store_id)
    await asyncio.to_thread(db_save_store_settings, store_id,
                            patch.auto_temp_control, actor["id"])
    if before["auto_temp_control"] != patch.auto_temp_control:
        await asyncio.to_thread(
            db_log_history, store_id, "store", "store_settings", actor,
            None, {"auto_temp_control": before["auto_temp_control"]},
            {"auto_temp_control": patch.auto_temp_control},
            f"AI 자동온도 모드 {'켬' if patch.auto_temp_control else '끔'}")
    return {"settings": await asyncio.to_thread(db_store_settings, store_id)}


@app.get("/api/v1/stores/{store_id}/history")
async def store_history(store_id: str, request: Request,
                        category: str | None = Query(None),
                        limit: int = Query(100, ge=1, le=500)) -> dict:
    require_store_access(request, store_id)
    entries = await asyncio.to_thread(db_history, store_id, category, limit)
    return {"store_id": store_id, "entries": entries}


@app.get("/api/v1/stores/{store_id}/stats")
async def store_stats(store_id: str,
                      minutes: int = Query(120, ge=1, le=43200),
                      dev_id: int | None = Query(None, ge=1, le=255)) -> dict:
    bucket = stats_bucket_for(minutes)
    points = await asyncio.to_thread(db_minute_stats, store_id, minutes,
                                     bucket, dev_id)
    return {"store_id": store_id, "minutes": minutes, "bucket": bucket,
            "dev_id": dev_id, "points": points}


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


@app.get("/api/v1/stores/{store_id}/devices")
async def list_devices(store_id: str, request: Request) -> dict:
    require_store_access(request, store_id)
    rows = await asyncio.to_thread(db_devices, store_id)
    snap = manager.snapshot(store_id)
    live = {d["dev_id"]: d for d in snap["devices"]}
    # Merge the registry with whatever the hub currently holds, so a device
    # seen this session but not yet flushed to disk still appears.
    merged = {row["dev_id"]: {**row, **live.get(row["dev_id"], {})} for row in rows}
    for dev_id, entry in live.items():
        merged.setdefault(dev_id, entry)
    def display_key(dev_id: int) -> tuple:
        sort_order = merged[dev_id].get("sort_order")
        return (sort_order if sort_order is not None else dev_id, dev_id)
    return {"store_id": store_id,
            "devices": [merged[k] for k in sorted(merged, key=display_key)],
            "stale_seconds": DEVICE_STALE_SECONDS}


class DeviceOrderRequest(BaseModel):
    order: list[int] = Field(..., min_length=1, max_length=255)


# NOTE: must be registered before the /devices/{dev_id} route below, or FastAPI
# would try to parse the literal path segment "order" as a dev_id.
@app.post("/api/v1/stores/{store_id}/devices/order")
async def set_device_order(store_id: str, req: DeviceOrderRequest,
                           request: Request) -> dict:
    """Persist the dashboard's device display order (list of dev_id, front first)."""
    sess = require_store_access(request, store_id)
    if len(set(req.order)) != len(req.order):
        raise HTTPException(status_code=422, detail="순서 목록에 중복된 장비가 있습니다.")
    rows = await asyncio.to_thread(db_set_device_order, store_id, req.order)
    hub = manager.get(store_id)
    for row in rows:
        hub.devices[row["dev_id"]] = row
    await asyncio.to_thread(
        db_log_history, store_id, "device_meta", "device_reorder", actor_of(sess),
        None, None, {"order": req.order}, "장비 표시 순서 변경")
    # Every open dashboard rearranges immediately, the sender included.
    await manager.broadcast(hub, {"type": "device_order",
                                  "order": [r["dev_id"] for r in rows]})
    return {"devices": rows}


class DevicePatch(BaseModel):
    name: str | None = Field(None, max_length=64)
    location: str | None = Field(None, max_length=64)


@app.post("/api/v1/stores/{store_id}/devices/{dev_id}")
async def update_device(store_id: str, dev_id: int, patch: DevicePatch,
                        request: Request) -> dict:
    sess = require_store_access(request, store_id)
    changes = patch.model_dump(exclude_none=True)
    hub = manager.get(store_id)
    previous = hub.devices.get(dev_id) or {}
    row = await asyncio.to_thread(db_update_device, store_id, dev_id, changes)
    if row is None:
        raise HTTPException(status_code=404, detail="device not found")
    hub.devices[dev_id] = row
    changed = {k: v for k, v in changes.items() if previous.get(k) != v}
    if changed:
        await asyncio.to_thread(
            db_log_history, store_id, "device_meta", "device_update",
            actor_of(sess), dev_id, {k: previous.get(k) for k in changed},
            changed, "디바이스 정보 변경: " + ", ".join(changed))
    await manager.broadcast(hub, {"type": "device_meta", "device": row})
    return {"device": row}


@app.get("/api/v1/stores/{store_id}/devices/{dev_id}/log")
async def device_log(store_id: str, dev_id: int, request: Request) -> dict:
    """The board's recent console output. HQ admin only.

    The backlog the 디버깅 panel opens with; everything after that arrives on the
    store WebSocket as `device_log`. Admin-only for the same reason the delete
    button is: these lines carry the unit's SSID, its gateway address and its
    OTA URLs, which is diagnostic detail for HQ and noise for shop staff.
    """
    require_admin(request)
    hub = manager.get(store_id)
    return {"dev_id": dev_id, "lines": list(hub.logs.get(dev_id) or []),
            "capacity": DEVICE_LOG_LINES}


class IrMonitorRequest(BaseModel):
    timeout_s: int = Field(20, ge=5, le=120)
    cancel: bool = False


@app.post("/api/v1/stores/{store_id}/devices/{dev_id}/ir-monitor")
async def start_ir_monitor(store_id: str, dev_id: int, req: IrMonitorRequest,
                           request: Request) -> dict:
    """Arm the device's IR receiver and print what it hears. HQ admin only.

    Not a learn session: nothing is stored, and no slot is claimed. It answers
    the question that comes before learning -- is the remote reaching the unit,
    and what does the frame actually look like -- with the timings themselves
    landing in the device console the 디버깅 popup is already showing.
    """
    require_admin(request)
    hub = manager.get(store_id)
    if learn_in_progress(hub):
        raise HTTPException(status_code=409,
                            detail="IR 학습이 진행 중입니다. 완료 후 사용하세요.")
    sent = await manager.to_gateway(hub, {
        "cmd": "IR_MONITOR", "store_id": store_id, "target_id": dev_id,
        "timeout_s": req.timeout_s, "cancel": req.cancel})
    if not sent:
        raise HTTPException(status_code=409, detail="게이트웨이가 오프라인입니다.")
    return {"ok": True, "dev_id": dev_id, "timeout_s": req.timeout_s,
            "cancel": req.cancel}


@app.delete("/api/v1/stores/{store_id}/devices/{dev_id}/log")
async def clear_device_log(store_id: str, dev_id: int, request: Request) -> dict:
    """Drop the buffered console for one device. HQ admin only.

    Not an erasure of anything of record -- the ring is diagnostic scratch. It
    exists so an operator can wipe the noise, run one operation, and read only
    what that operation printed.
    """
    require_admin(request)
    manager.get(store_id).logs.pop(dev_id, None)
    return {"ok": True, "dev_id": dev_id}


@app.delete("/api/v1/stores/{store_id}/devices/{dev_id}")
async def delete_device(store_id: str, dev_id: int, request: Request) -> dict:
    """Remove a retired unit's card from the dashboard. HQ admin only.

    Store staff can rename and reorder cards but not delete them: the card of a
    unit that is merely unplugged for the afternoon looks exactly like the card
    of one that has been decommissioned, and only HQ knows which it is.
    """
    sess = require_admin(request)
    hub = manager.get(store_id)
    previous = dict(hub.devices.get(dev_id) or {})

    # A unit still publishing would re-register on its very next packet, so
    # deleting it now just makes the card blink. Say so instead of pretending.
    # Two signals, because neither covers the other: hub.latest only fills while
    # some viewer holds the live stream open, whereas the gateway uploads minute
    # stats whether or not anyone is watching.
    now_s = int(time.time())
    latest = hub.latest.get(dev_id)
    streaming = latest is not None and now_s * 1000 - latest["ts"] < DEVICE_STALE_SECONDS * 1000
    last_stat = await asyncio.to_thread(db_device_last_stat, store_id, dev_id)
    reporting = last_stat is not None and now_s - last_stat < DEVICE_QUIET_SECONDS
    if streaming or reporting:
        raise HTTPException(
            status_code=409,
            detail="아직 데이터를 보내고 있는 장비입니다. 전원을 내리거나 게이트웨이에서 "
                   "분리하고 몇 분 기다린 뒤 삭제하세요 — 살아 있으면 곧바로 다시 등록됩니다.")

    removed = await asyncio.to_thread(db_delete_device, store_id, dev_id)
    if not removed and dev_id not in hub.known_device_ids():
        raise HTTPException(status_code=404, detail="등록되지 않은 장비입니다.")

    # known_device_ids() unions three dicts, so a card survives until the
    # dev_id is gone from every one of them.
    hub.devices.pop(dev_id, None)
    hub.latest.pop(dev_id, None)
    hub.ac_states.pop(dev_id, None)
    hub.last_seen_written.pop(dev_id, None)

    await asyncio.to_thread(
        db_log_history, store_id, "device_meta", "device_delete", actor_of(sess),
        dev_id, {"name": previous.get("name"), "model": previous.get("model")},
        None, f"장비 {dev_id} 카드 삭제")
    await manager.broadcast(hub, {"type": "device_removed", "dev_id": dev_id})
    log.info("[%s] device %d deleted by admin %s", store_id, dev_id,
             actor_of(sess)["id"])
    return {"ok": True, "dev_id": dev_id}


async def hydrate_hub(hub: StoreHub) -> None:
    """Load registry rows and persisted final settings into a fresh hub."""
    for row in await asyncio.to_thread(db_devices, hub.store_id):
        hub.devices.setdefault(row["dev_id"], row)
    for dev_id, saved in (await asyncio.to_thread(db_ac_settings,
                                                  hub.store_id)).items():
        hub.ac_states.setdefault(dev_id, saved)


class AcControlRequest(BaseModel):
    target_id: int | str = Field(..., description='dev_id 또는 "all"')
    power: int | None = Field(None, ge=0, le=1)
    mode: str | None = None
    temp: int | None = Field(None, ge=16, le=30)
    fan: str | None = None


@app.post("/api/v1/stores/{store_id}/ac")
async def rest_ac_control(store_id: str, req: AcControlRequest,
                          request: Request) -> dict:
    """HTTP mirror of the WebSocket ac_control message.

    Lets the owner portal run bulk commands across stores it is not currently
    streaming (전체 매장 일괄 끄기 등) without opening a socket per store.
    """
    sess = require_store_access(request, store_id)
    hub = manager.get(store_id)
    await hydrate_hub(hub)
    updated, bulk, errors = await apply_ac_control(
        hub, req.model_dump(exclude_none=True), actor_of(sess))
    if errors and not updated:
        raise HTTPException(status_code=409, detail=errors[0])
    return {"ok": not errors, "bulk": bulk, "updates": updated, "errors": errors}


class LicensePatch(BaseModel):
    name: str | None = None
    owner_id: str | None = None
    address: str | None = None
    phone: str | None = Field(None, max_length=32)
    gateway_ip: str | None = Field(None, max_length=256)   # CSV allowlist, "" = 제한 없음
    plan: str | None = None
    license_state: str | None = Field(None, pattern="^(active|expired|suspended)$")
    grace_period_days: int | None = Field(None, ge=1, le=365)
    license_started_at: str | None = None
    license_expires_at: str | None = None


@app.post("/api/v1/stores/{store_id}/license")
async def set_license(store_id: str, patch: LicensePatch, request: Request) -> dict:
    sess = require_admin(request)
    changes = patch.model_dump(exclude_none=True)
    previous = await asyncio.to_thread(db_store, store_id) or {}
    try:
        updated = await asyncio.to_thread(db_set_license, store_id, changes)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    updated.pop("password_hash", None)

    diff_before = {k: previous.get(k) for k in changes if previous.get(k) != changes[k]}
    diff_after = {k: changes[k] for k in diff_before}
    if diff_after:
        await asyncio.to_thread(
            db_log_history, store_id, "license", "license_update",
            actor_of(sess), None, diff_before, diff_after,
            "구독/매장 정보 변경: " + ", ".join(diff_after))
    return {"store": updated, "evaluated": evaluate_license(updated, store_id)}


# --------------------------------------------------------------------------
# admin API: store registry, subscriptions, equipment upgrades
# --------------------------------------------------------------------------

@app.get("/api/v1/admin/stores")
async def admin_list_stores(request: Request) -> dict:
    require_admin(request)
    stores = await asyncio.to_thread(db_list_stores)
    for s in stores:
        snap = manager.snapshot(s["store_id"])
        s["has_password"] = bool(s["has_password"])
        s["license"] = evaluate_license(s, s["store_id"])
        s["gateway_online"] = snap["gateway_online"]
        s["gateway_info"] = snap["gateway_info"]
        s["viewers"] = snap["viewers"]
        s["sota"] = snap["sota"]
        # A device seen this session but not yet flushed still counts.
        s["device_count"] = max(int(s["device_count"] or 0), len(snap["devices"]))
    return {"stores": stores, "server_time": iso(utcnow())}


class StoreCreateRequest(BaseModel):
    store_id: str = Field(..., min_length=2, max_length=32,
                          pattern=r"^[A-Za-z0-9_-]+$")
    name: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=4, max_length=64)
    grace_period_days: int = Field(DEFAULT_GRACE_DAYS, ge=1, le=365)
    license_days: int = Field(365, ge=1, le=3650)
    address: str | None = Field(None, max_length=128)
    plan: str | None = Field(None, max_length=32)
    owner_id: str | None = Field(None, min_length=2, max_length=32,
                                 pattern=r"^[A-Za-z0-9_-]+$")
    # When owner_id is new, these create the owner account in the same call.
    owner_name: str | None = Field(None, max_length=64)
    owner_password: str | None = Field(None, min_length=4, max_length=64)


@app.post("/api/v1/admin/stores")
async def admin_create_store(req: StoreCreateRequest, request: Request) -> dict:
    sess = require_admin(request)
    actor = actor_of(sess)
    store_id = req.store_id
    try:
        if req.owner_id and await asyncio.to_thread(db_owner, req.owner_id) is None:
            if not (req.owner_name and req.owner_password):
                raise HTTPException(
                    status_code=422,
                    detail="새 점주 계정에는 owner_name과 owner_password가 필요합니다.")
            await asyncio.to_thread(db_create_owner, req.owner_id,
                                    req.owner_name, req.owner_password)
            await asyncio.to_thread(
                db_log_history, store_id, "account", "owner_created", actor,
                None, None, {"owner_id": req.owner_id, "name": req.owner_name},
                f"점주 계정 생성: {req.owner_name} ({req.owner_id})")

        expires = iso(utcnow() + timedelta(days=req.license_days))
        store = await asyncio.to_thread(
            db_create_store, store_id, req.name, req.password,
            req.grace_period_days, expires, req.owner_id, req.address, req.plan)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    store.pop("password_hash", None)
    await asyncio.to_thread(
        db_log_history, store_id, "account", "store_created", actor, None,
        None, {"name": req.name, "owner_id": req.owner_id, "plan": req.plan,
               "license_days": req.license_days},
        f"매장 등록: {req.name}")
    log.info("[%s] store registered by admin", store_id)
    return {"store": store, "license": evaluate_license(store, store_id)}


@app.get("/api/v1/admin/applications")
async def admin_list_applications(
        request: Request,
        status: str | None = Query(None, pattern="^(pending|approved|rejected)$"),
        limit: int = Query(100, ge=1, le=500)) -> dict:
    require_admin(request)
    applications = await asyncio.to_thread(db_list_applications, status, limit)
    return {"applications": applications,
            "pending": sum(1 for a in applications if a["status"] == "pending")}


class ApplicationRejectRequest(BaseModel):
    reason: str | None = Field(None, max_length=200)


@app.post("/api/v1/admin/applications/{app_id}/approve")
async def admin_approve_application(app_id: int, request: Request) -> dict:
    sess = require_admin(request)
    actor = actor_of(sess)
    try:
        application = await asyncio.to_thread(
            db_decide_application, app_id, True, actor["id"])
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    await asyncio.to_thread(
        db_log_history, application["store_id"], "account", "store_approved",
        actor, None, None, {"name": application["name"]},
        f"매장 등록 승인: {application['name']}")
    log.info("[%s] store application approved by %s",
             application["store_id"], actor["id"])
    store = await asyncio.to_thread(db_store, application["store_id"]) or {}
    store.pop("password_hash", None)
    return {"ok": True, "application": application, "store": store,
            "license": evaluate_license(store or None, application["store_id"])}


@app.post("/api/v1/admin/applications/{app_id}/reject")
async def admin_reject_application(app_id: int, req: ApplicationRejectRequest,
                                   request: Request) -> dict:
    sess = require_admin(request)
    actor = actor_of(sess)
    try:
        application = await asyncio.to_thread(
            db_decide_application, app_id, False, actor["id"],
            (req.reason or "").strip() or None)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    await asyncio.to_thread(
        db_log_history, application["store_id"], "account", "store_rejected",
        actor, None, None,
        {"name": application["name"], "reason": application.get("reason")},
        f"매장 등록 거절: {application['name']}")
    log.info("[%s] store application rejected by %s",
             application["store_id"], actor["id"])
    return {"ok": True, "application": application}


class OwnerCreateRequest(BaseModel):
    owner_id: str = Field(..., min_length=2, max_length=32,
                          pattern=r"^[A-Za-z0-9_-]+$")
    name: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=4, max_length=64)
    phone: str | None = Field(None, max_length=32)


@app.get("/api/v1/admin/owners")
async def admin_list_owners(request: Request) -> dict:
    require_admin(request)
    return {"owners": await asyncio.to_thread(db_list_owners)}


@app.post("/api/v1/admin/owners")
async def admin_create_owner(req: OwnerCreateRequest, request: Request) -> dict:
    sess = require_admin(request)
    try:
        owner = await asyncio.to_thread(db_create_owner, req.owner_id, req.name,
                                        req.password, req.phone)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    owner.pop("password_hash", None)
    return {"owner": owner}


@app.get("/api/v1/admin/history")
async def admin_history(request: Request, store_id: str | None = Query(None),
                        category: str | None = Query(None),
                        limit: int = Query(100, ge=1, le=500)) -> dict:
    require_admin(request)
    return {"entries": await asyncio.to_thread(db_history, store_id, category, limit)}


class PasswordResetRequest(BaseModel):
    password: str = Field(..., min_length=4, max_length=64)


@app.post("/api/v1/admin/stores/{store_id}/gateway-token")
async def admin_regenerate_gateway_token(store_id: str, request: Request) -> dict:
    """Issue a fresh gateway token. The store's gateway stops connecting until
    it is given the new one, which is the point: this is how a leaked token is
    revoked."""
    sess = require_admin(request)
    token = await asyncio.to_thread(db_regenerate_gateway_token, store_id)
    if token is None:
        raise HTTPException(status_code=404, detail="등록되지 않은 매장입니다.")
    await asyncio.to_thread(
        db_log_history, store_id, "account", "gateway_token_reset", actor_of(sess),
        None, None, None, "게이트웨이 토큰 재발급")
    log.info("[%s] gateway token regenerated by admin", store_id)
    return {"ok": True, "gateway_token": token}


@app.post("/api/v1/admin/stores/{store_id}/password")
async def admin_reset_password(store_id: str, req: PasswordResetRequest,
                               request: Request) -> dict:
    sess = require_admin(request)
    if not await asyncio.to_thread(db_set_store_password, store_id, req.password):
        raise HTTPException(status_code=404, detail="등록되지 않은 매장입니다.")
    await asyncio.to_thread(
        db_log_history, store_id, "account", "password_reset", actor_of(sess),
        None, None, None, "매장 로그인 비밀번호 재설정")
    log.info("[%s] store password reset by admin", store_id)
    return {"ok": True}


class SotaDeployRequest(BaseModel):
    target_id: int = Field(1, ge=1, le=255)
    brand: str | None = None
    model: str | None = None
    model_id: str | None = None
    protocol: str | None = None


@app.post("/api/v1/admin/stores/{store_id}/sota")
async def admin_sota_deploy(store_id: str, req: SotaDeployRequest,
                            request: Request) -> dict:
    require_admin(request)
    hub = manager.get(store_id)
    error = await start_sota_deploy(hub, req.model_dump(), actor_of(session_of(request)))
    if error:
        raise HTTPException(status_code=409, detail=error)
    return {"ok": True, "sota": hub.sota}


# --------------------------------------------------------------------------
# admin: AC model registry + IR learning
# --------------------------------------------------------------------------

class AcModelCreate(BaseModel):
    brand: str = Field(..., min_length=1, max_length=64)
    name: str = Field(..., min_length=1, max_length=64)
    notes: str | None = Field(None, max_length=256)


class AcModelPatch(BaseModel):
    brand: str | None = Field(None, min_length=1, max_length=64)
    name: str | None = Field(None, min_length=1, max_length=64)
    notes: str | None = Field(None, max_length=256)


@app.get("/api/v1/admin/ac/models")
async def admin_list_ac_models(request: Request) -> dict:
    require_admin(request)
    return {"models": await asyncio.to_thread(db_ac_models_admin),
            "slot_keys": IR_SLOTS}


@app.post("/api/v1/admin/ac/models")
async def admin_create_ac_model(req: AcModelCreate, request: Request) -> dict:
    require_admin(request)
    model = await asyncio.to_thread(
        db_create_ac_model, req.brand.strip(), req.name.strip(), req.notes)
    log.info("learned-remote entry registered: %s (%s)",
             model["model_id"], model["name"])
    return {"ok": True, "model": model}


@app.patch("/api/v1/admin/ac/models/{model_id}")
async def admin_update_ac_model(model_id: str, req: AcModelPatch,
                                request: Request) -> dict:
    require_admin(request)
    model = await asyncio.to_thread(
        db_update_ac_model, model_id, req.model_dump(exclude_none=True))
    if model is None:
        raise HTTPException(status_code=404, detail="등록되지 않은 모델입니다.")
    return {"ok": True, "model": model}


@app.delete("/api/v1/admin/ac/models/{model_id}")
async def admin_delete_ac_model(model_id: str, request: Request) -> dict:
    require_admin(request)
    try:
        await asyncio.to_thread(db_delete_ac_model, model_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {"ok": True}


@app.get("/api/v1/admin/ac/models/{model_id}/codes")
async def admin_list_ir_codes(model_id: str, request: Request) -> dict:
    require_admin(request)
    model = await asyncio.to_thread(db_ac_model, model_id)
    if model is None:
        raise HTTPException(status_code=404, detail="등록되지 않은 모델입니다.")
    return {"model_id": model_id, "slot_keys": IR_SLOTS,
            "codes": await asyncio.to_thread(db_ir_code_list, model_id)}


@app.delete("/api/v1/admin/ac/models/{model_id}/codes/{slot}")
async def admin_delete_ir_code(model_id: str, slot: str,
                               request: Request) -> dict:
    require_admin(request)
    if not await asyncio.to_thread(db_delete_ir_code, model_id, slot):
        raise HTTPException(status_code=404, detail="학습된 코드가 없습니다.")
    return {"ok": True}


class LearnStartRequest(BaseModel):
    dev_id: int = Field(..., ge=1, le=255)
    model_id: str = Field(..., min_length=1, max_length=64)
    slot: str = Field(..., min_length=1, max_length=16)
    timeout_s: int = Field(30, ge=5, le=120)


@app.post("/api/v1/admin/stores/{store_id}/learn")
async def admin_start_learn(store_id: str, req: LearnStartRequest,
                            request: Request) -> dict:
    sess = require_admin(request)
    if req.slot not in IR_SLOTS:
        raise HTTPException(status_code=422, detail=f"알 수 없는 슬롯: {req.slot}")
    model = await asyncio.to_thread(db_ac_model, req.model_id)
    if model is None:
        raise HTTPException(status_code=404, detail="등록되지 않은 모델입니다.")
    if model["kind"] != "raw":
        raise HTTPException(status_code=409,
                            detail="protocol 방식 모델은 학습이 필요 없습니다.")
    hub = manager.get(store_id)
    if hub.gateway is None:
        raise HTTPException(status_code=409,
                            detail="게이트웨이가 오프라인입니다. 학습을 시작할 수 없습니다.")
    if learn_in_progress(hub):
        raise HTTPException(status_code=409, detail="이미 학습이 진행 중입니다.")
    # "verify" is how a deploy ends when the device never confirmed -- also
    # terminal, or a single failed deploy would block learning forever.
    if hub.sota and hub.sota.get("stage") not in (None, "done", "failed", "verify"):
        raise HTTPException(status_code=409,
                            detail="장비 업그레이드가 진행 중입니다. 완료 후 학습하세요.")
    session_id = secrets.token_hex(4)
    actor = actor_of(sess)
    sent = await manager.to_gateway(hub, {
        "cmd": "LEARN_IR", "store_id": store_id, "target_id": req.dev_id,
        "session_id": session_id, "slot": req.slot,
        "timeout_s": req.timeout_s, "ts": iso(utcnow())})
    if not sent:
        raise HTTPException(status_code=409,
                            detail="게이트웨이 연결이 끊겼습니다. 다시 시도하세요.")
    hub.learn = {"session_id": session_id, "dev_id": req.dev_id,
                 "model_id": req.model_id, "slot": req.slot,
                 "status": "waiting", "started_at": iso(utcnow()),
                 "timeout_s": req.timeout_s,
                 # device timeout + slack for the MQTT/WS hops
                 "deadline": time.time() + req.timeout_s + 5,
                 "actor_id": actor.get("id")}
    log.info("[%s] IR learn started: dev=%d model=%s slot=%s session=%s",
             store_id, req.dev_id, req.model_id, req.slot, session_id)
    return {"ok": True, "learn": hub.learn}


@app.delete("/api/v1/admin/stores/{store_id}/learn")
async def admin_cancel_learn(store_id: str, request: Request) -> dict:
    require_admin(request)
    hub = manager.get(store_id)
    learn = hub.learn
    if not learn or learn.get("status") != "waiting":
        return {"ok": True, "learn": learn_view(hub)}
    learn["status"] = "canceled"
    await manager.to_gateway(hub, {
        "cmd": "LEARN_CANCEL", "store_id": store_id,
        "target_id": learn["dev_id"], "session_id": learn["session_id"],
        "ts": iso(utcnow())})
    log.info("[%s] IR learn canceled (session=%s)", store_id, learn["session_id"])
    return {"ok": True, "learn": learn}


# --------------------------------------------------------------------------
# WebSocket: browser
# --------------------------------------------------------------------------

async def start_sota_deploy(hub: StoreHub, req: dict, actor: dict) -> str | None:
    """Kick a firmware/IR-data deploy toward the gateway; returns an error or None.

    Shared by the store dashboard (WebSocket) and the admin screen (HTTP).
    A kind=protocol model flashes the protocol-named firmware image
    (DEPLOY_FIRMWARE); a kind=raw model ships the learned code bundle inline
    (DEPLOY_IRDATA) -- the gateway writes it next to the firmware images and
    the device fetches it over the same OTA HTTP server.
    """
    target_id = int(req.get("target_id") or 1)
    if learn_in_progress(hub):
        return "IR 학습이 진행 중입니다. 완료 후 배포하세요."
    model_id = req.get("model_id")
    model = await asyncio.to_thread(db_ac_model, model_id) if model_id else None
    # The DB row wins over whatever the browser sent: the deploy must match
    # the registry even if the client's cached catalog is stale.
    brand = (model or {}).get("brand") or req.get("brand")
    model_name = (model or {}).get("name") or req.get("model")
    # Two deploys, and which one it is depends only on whether a remote was
    # picked: with one, push its learned codes (the gateway flashes the IR
    # image first if the device is still bare); with none, just put the IR
    # image on so the device can be learned into at all.
    bundle = None
    if model_id:
        bundle = await asyncio.to_thread(db_ir_bundle, model_id)
        if not bundle:
            return "학습된 리모컨 코드가 없습니다. 먼저 IR 학습을 진행하세요."
    # Remember what was asked for; the device row is stamped once the
    # gateway reports the deploy finished.
    hub.sota_target = {"dev_id": target_id, "brand": brand, "model": model_name,
                       "model_id": model_id, "protocol": RAW_PROTOCOL}
    payload = {"cmd": "DEPLOY_IRDATA" if bundle else "DEPLOY_FIRMWARE",
               "store_id": hub.store_id, "target_id": target_id,
               "brand": brand, "model": model_name,
               "model_id": model_id,
               "ts": iso(utcnow())}
    if bundle:
        payload["bundle"] = bundle
    if not await manager.to_gateway(hub, payload):
        hub.sota_target = None
        return "게이트웨이가 오프라인입니다. 배포할 수 없습니다."
    hub.sota = {"stage": "requested", "percent": 0, "model": model_name}
    detail = f"장비 업그레이드 요청: {brand or ''} {model_name or ''}".strip()
    if bundle:
        detail += f" (학습 코드 {len(bundle['slots'])}개)"
    await asyncio.to_thread(
        db_log_history, hub.store_id, "sota", "sota_requested", actor, target_id,
        None, {"brand": brand, "model": model_name,
               "learned_slots": len(bundle["slots"]) if bundle else 0},
        detail)
    await manager.broadcast(hub, {
        "type": "sota_progress", "stage": "requested", "percent": 0,
        "message": "배포 요청을 게이트웨이로 전송했습니다.",
        "model": model_name})
    return None


AC_FIELD_LABELS = {"power": "전원", "mode": "모드", "temp": "희망온도", "fan": "바람세기"}


def summarize_ac_change(after: dict) -> str:
    """Korean one-liner for the history table, e.g. '전원 ON, 희망온도 24°C'."""
    parts = []
    if "power" in after:
        parts.append("전원 " + ("ON" if after["power"] else "OFF"))
    if "mode" in after:
        parts.append(f"모드 {after['mode']}")
    if "temp" in after:
        parts.append(f"희망온도 {after['temp']}°C")
    if "fan" in after:
        parts.append(f"바람세기 {after['fan']}")
    return ", ".join(parts) or "변경 없음"


async def apply_ac_control(hub: StoreHub, msg: dict,
                           actor: dict) -> tuple[list[dict], bool, list[str]]:
    """Drive one device, or every device when target_id is "all".

    Bulk is expanded here into one AC_CONTROL per device, so the gateway keeps
    handling exactly one command shape and needs no change. Every accepted
    command is persisted as the device's final setting and audited. Shared by
    the WebSocket handler and the REST endpoint; returns (updates, bulk,
    error messages).
    """
    errors: list[str] = []
    raw_target = msg.get("target_id")
    bulk = isinstance(raw_target, str) and raw_target.lower() == "all"

    if bulk:
        targets = hub.known_device_ids()
        if not targets:
            return [], bulk, ["제어할 디바이스가 없습니다."]
    else:
        try:
            targets = [int(raw_target)]
        except (TypeError, ValueError):
            return [], bulk, ["target_id가 필요합니다."]

    if hub.gateway is None:
        return [], bulk, ["게이트웨이가 오프라인입니다. 명령을 전송할 수 없습니다."]

    updated: list[dict] = []
    changed_fields = {k: msg[k] for k in ("power", "mode", "temp", "fan")
                      if msg.get(k) is not None}
    raw_slots_cache: dict[str, set[str]] = {}
    for dev_id in targets:
        # Each device keeps its own mode/fan; a bulk command only overrides
        # the fields it actually carries.
        previous = dict(hub.ac_state_for(dev_id))
        state = dict(previous)
        state["target_id"] = dev_id
        for key in ("power", "mode", "temp", "fan"):
            if msg.get(key) is not None:
                state[key] = msg[key]
        try:
            state["power"] = 1 if int(state["power"]) else 0
            state["mode"] = AC_MODE_NAMES[normalize_mode(state["mode"])]
            state["fan"] = AC_FAN_NAMES[normalize_fan(state["fan"])]
            state["temp"] = clamp_target_temp(state["temp"])
            packet = encode_ac_packet(dev_id, state["power"], state["mode"],
                                      state["temp"], state["fan"])
        except (PacketError, KeyError, TypeError, ValueError) as exc:
            errors.append(f"디바이스 {dev_id}: {exc}")
            continue

        # A raw-replay device can only express combinations that were learned;
        # sending anything else would be silently dropped on the device, so
        # refuse it here where the user can see why.
        device = hub.devices.get(dev_id) or {}
        if device.get("protocol") == RAW_PROTOCOL:
            model_id = device.get("model_id") or ""
            if model_id not in raw_slots_cache:
                model = await asyncio.to_thread(db_ac_model, model_id) \
                    if model_id else None
                raw_slots_cache[model_id] = set((model or {}).get("slots") or [])
            needed = slots_for_state(state["power"], state["mode"], state["temp"])
            missing = [s for s in needed if s not in raw_slots_cache[model_id]]
            if missing:
                errors.append(
                    f"디바이스 {dev_id}: 아직 학습되지 않은 버튼이 필요합니다"
                    f" ({', '.join(missing)})")
                continue

        if not await manager.to_gateway(hub, {
                "cmd": "AC_CONTROL", "store_id": hub.store_id,
                "packet_hex": packet.hex(), "state": state, "ts": iso(utcnow())}):
            errors.append("게이트웨이 연결이 끊겼습니다. 일부 명령이 전송되지 않았습니다.")
            break

        state["updated_at"] = iso(utcnow())
        hub.ac_states[dev_id] = state

        # Persist the final setting; the dashboard restores from this row.
        await asyncio.to_thread(db_save_ac_setting, hub.store_id, dev_id,
                                state, actor.get("id"))
        if not bulk:
            keys = [k for k in changed_fields if previous.get(k) != state.get(k)]
            await asyncio.to_thread(
                db_log_history, hub.store_id, "ac", "ac_control", actor, dev_id,
                {k: previous.get(k) for k in keys},
                {k: state.get(k) for k in keys},
                summarize_ac_change({k: state.get(k) for k in changed_fields}))

        updated.append({"dev_id": dev_id, "state": state, "packet_hex": packet.hex()})

    if updated:
        # One echo for the whole batch, so a bulk press is a single repaint.
        await manager.broadcast(hub, {"type": "ac_state", "bulk": bulk, "updates": updated})
        if bulk:
            # One audit row for the whole batch: dev_id NULL = store-wide.
            await asyncio.to_thread(
                db_log_history, hub.store_id, "ac", "bulk_control", actor, None,
                None, {**changed_fields, "applied_devices": len(updated)},
                f"전체 제어 ({len(updated)}대): "
                + summarize_ac_change(changed_fields))
            log.info("[%s] bulk AC control -> %d devices", hub.store_id, len(updated))
    return updated, bulk, errors


async def handle_ac_control(hub: StoreHub, ws: WebSocket, msg: dict,
                            actor: dict) -> None:
    _, _, errors = await apply_ac_control(hub, msg, actor)
    for message in errors:
        await _send_json(ws, {"type": "error", "scope": "ac_control",
                              "message": message})


async def handle_viewer_message(hub: StoreHub, ws: WebSocket, msg: dict,
                                actor: dict) -> None:
    kind = msg.get("type")

    if kind == "ping":
        await _send_json(ws, {"type": "pong", "ts": iso(utcnow())})

    elif kind == "ac_control":
        await handle_ac_control(hub, ws, msg, actor)

    elif kind == "device_update":
        try:
            dev_id = int(msg["dev_id"])
        except (KeyError, TypeError, ValueError):
            await _send_json(ws, {"type": "error", "scope": "device",
                                  "message": "dev_id가 필요합니다."})
            return
        patch = {k: msg.get(k) for k in ("name", "location") if msg.get(k) is not None}
        previous = hub.devices.get(dev_id) or {}
        row = await asyncio.to_thread(db_update_device, hub.store_id, dev_id, patch)
        if row is None:
            return
        hub.devices[dev_id] = row
        changed = {k: v for k, v in patch.items() if previous.get(k) != v}
        if changed:
            await asyncio.to_thread(
                db_log_history, hub.store_id, "device_meta", "device_update",
                actor, dev_id, {k: previous.get(k) for k in changed}, changed,
                "디바이스 정보 변경: " + ", ".join(changed))
        await manager.broadcast(hub, {"type": "device_meta", "device": row})

    elif kind == "sota_deploy":
        error = await start_sota_deploy(hub, msg, actor)
        if error:
            await _send_json(ws, {"type": "error", "scope": "sota", "message": error})

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
    # Same session rule as the HTTP API: admin any store, owner their stores,
    # store staff their own.
    sess = get_session(ws.cookies.get(SESSION_COOKIE))
    if not session_can_access(sess, store_id):
        await _send_json(ws, {"type": "error", "scope": "auth",
                              "message": "로그인이 필요합니다."})
        await ws.close(code=4401)
        log.warning("[%s] viewer rejected: no session", store_id)
        return
    actor = actor_of(sess)
    hub = await manager.add_viewer(store_id, ws)
    log.info("[%s] viewer connected (total=%d)", store_id, len(hub.viewers))
    try:
        store = await asyncio.to_thread(db_store, store_id)
        points = await asyncio.to_thread(db_minute_stats, store_id, STATS_WINDOW_MINUTES)
        # Registry rows (cards exist before the first packet) + each device's
        # persisted final setpoints, so the panel restores what was last set.
        await hydrate_hub(hub)
        await _send_json(ws, {
            "type": "hello", "store_id": store_id,
            "store_name": (store or {}).get("name") or store_id,
            "address": (store or {}).get("address"),
            "plan": (store or {}).get("plan"),
            "gateway_online": hub.gateway is not None,
            "gateway_info": hub.gateway_info,
            "live_active": hub.live_active,
            "devices": hub.device_payload(),
            "stale_seconds": DEVICE_STALE_SECONDS,
            "license": evaluate_license(store, store_id),
            "settings": await asyncio.to_thread(db_store_settings, store_id),
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
                await handle_viewer_message(hub, ws, msg, actor)
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
        dev_id = reading["dev_id"]
        hub.latest[dev_id] = reading

        # Register a device the first time we see it, and refresh last_seen at
        # most once a minute -- a write per packet would be a dozen a second.
        now = time.monotonic()
        if now - hub.last_seen_written.get(dev_id, 0.0) > LAST_SEEN_WRITE_INTERVAL:
            hub.last_seen_written[dev_id] = now
            known = dev_id in hub.devices
            row = await asyncio.to_thread(db_register_device, hub.store_id, dev_id)
            hub.devices[dev_id] = row
            if not known:
                log.info("[%s] registered device %d (%s)", hub.store_id, dev_id, row["name"])
                await manager.broadcast(hub, {"type": "device_meta", "device": row})

        await manager.broadcast(hub, {"type": "live", "data": reading})


async def handle_ir_capture(hub: StoreHub, msg: dict) -> None:
    """A capture (or capture failure) relayed up from a learning device."""
    learn = hub.learn
    session_id = msg.get("session_id")
    if not learn or learn.get("session_id") != session_id:
        log.warning("[%s] stray ir_capture (session=%r) -- dropped",
                    hub.store_id, session_id)
        return
    if learn.get("status") != "waiting":
        # Cancelled or already timed out on our side; a late frame must not
        # silently land in the registry.
        log.info("[%s] late ir_capture for %s session -- dropped",
                 hub.store_id, learn.get("status"))
        return

    if not msg.get("ok"):
        error = str(msg.get("error") or "capture_failed")
        learn["status"] = "timeout" if error == "timeout" else (
            "canceled" if error == "canceled" else "failed")
        learn["error"] = {
            "timeout": "제한 시간 안에 리모컨 신호가 없었습니다.",
            "canceled": "학습이 취소되었습니다.",
            "too_long": "신호가 너무 깁니다. 리모컨을 수신기에 더 가까이 대고 다시 시도하세요.",
        }.get(error, f"캡처 실패: {error}")
        return
    if msg.get("slot") != learn["slot"]:
        learn["status"] = "failed"
        learn["error"] = "요청한 슬롯과 다른 응답입니다. 다시 시도하세요."
        return

    raw = msg.get("raw")
    if not isinstance(raw, list) \
            or not (IR_RAW_MIN_LEN <= len(raw) <= IR_RAW_MAX_LEN):
        learn["status"] = "failed"
        learn["error"] = "신호 길이가 올바르지 않습니다. 다시 시도하세요."
        return
    try:
        raw = [int(v) for v in raw]
    except (TypeError, ValueError):
        learn["status"] = "failed"
        learn["error"] = "신호 데이터 형식이 올바르지 않습니다."
        return
    if not all(IR_RAW_MIN_US <= v <= IR_RAW_MAX_US for v in raw):
        learn["status"] = "failed"
        learn["error"] = "신호 값이 허용 범위를 벗어났습니다. 다시 시도하세요."
        return
    freq = msg.get("freq_khz")
    freq = int(freq) if isinstance(freq, (int, float)) \
        and IR_FREQ_MIN_KHZ <= freq <= IR_FREQ_MAX_KHZ else 38

    await asyncio.to_thread(
        db_save_ir_code, learn["model_id"], learn["slot"], freq, raw,
        learn.get("actor_id"), hub.store_id, learn["dev_id"])
    learn["status"] = "captured"
    learn["length"] = len(raw)
    learn["freq_khz"] = freq
    await asyncio.to_thread(
        db_log_history, hub.store_id, "ir_learn", "ir_captured",
        {"type": "admin", "id": learn.get("actor_id")}, learn["dev_id"], None,
        {"model_id": learn["model_id"], "slot": learn["slot"],
         "length": len(raw)},
        f"리모컨 학습 완료: {learn['model_id']} / {learn['slot']}")
    log.info("[%s] IR capture stored: model=%s slot=%s len=%d",
             hub.store_id, learn["model_id"], learn["slot"], len(raw))


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

        # A finished deploy is what makes a device searchable by AC model.
        if msg.get("stage") == "done" and hub.sota_target:
            target = hub.sota_target
            hub.sota_target = None
            row = await asyncio.to_thread(
                db_update_device, hub.store_id, target["dev_id"],
                {k: target.get(k)
                 for k in ("brand", "model", "model_id", "protocol")})
            if row:
                hub.devices[target["dev_id"]] = row
                await asyncio.to_thread(
                    db_log_history, hub.store_id, "sota", "sota_done",
                    {"type": "system", "id": "gateway"}, target["dev_id"], None,
                    {k: target.get(k)
                     for k in ("brand", "model", "model_id", "protocol")},
                    f"장비 업그레이드 완료: {target.get('brand') or ''} "
                    f"{target.get('model') or ''}".strip())
                await manager.broadcast(hub, {"type": "device_meta", "device": row})

    elif kind == "device_log":
        dev_id = msg.get("dev_id")
        lines = msg.get("lines") or []
        if not isinstance(dev_id, int) or not isinstance(lines, list):
            return
        stamped = [{"ts": int(time.time() * 1000), "text": str(t)[:200]}
                   for t in lines[:50]]
        hub.log_for(dev_id).extend(stamped)
        # Only viewers with the panel open care, but a broadcast is cheaper than
        # tracking who does; the browser drops lines for cards it is not showing.
        await manager.broadcast(hub, {"type": "device_log", "dev_id": dev_id,
                                      "lines": stamped})

    elif kind == "ir_capture":
        await handle_ir_capture(hub, msg)

    elif kind == "ac_ack":
        state = msg.get("state")
        if isinstance(state, dict) and state.get("target_id") is not None:
            dev_id = int(state["target_id"])
            merged = {**hub.ac_state_for(dev_id), **state, "updated_at": iso(utcnow())}
            hub.ac_states[dev_id] = merged
            state = merged
        await manager.broadcast(hub, {"type": "ac_ack", "ok": msg.get("ok", True),
                                      "state": state, "message": msg.get("message")})

    elif kind == "gateway_status":
        hub.gateway_info = {**hub.gateway_info, **(msg.get("info") or {})}
        await manager.broadcast(hub, {"type": "gateway_status", "online": True,
                                      "info": hub.gateway_info})

    elif kind == "pong":
        pass

    else:
        log.debug("[%s] unhandled gateway message %r", hub.store_id, kind)


def gateway_ip_allowed(store: dict | None, client_ip: str) -> bool:
    """NULL/empty gateway_ip means no restriction; otherwise a CSV allowlist."""
    allowed = ((store or {}).get("gateway_ip") or "").strip()
    if not allowed:
        return True
    return client_ip in {ip.strip() for ip in allowed.split(",") if ip.strip()}


@app.get("/api/v1/gateway/identify")
async def gateway_identify(request: Request, token: str = Query("")) -> dict:
    """Which store does this gateway token belong to?

    The gateway calls this once at startup so it no longer has to be told its
    own store id -- the token is the identity, and the IP allowlist is still
    checked here and again on the socket.
    """
    client_ip = request.client.host if request.client else ""
    store, problem = await asyncio.to_thread(resolve_gateway_store, token, None)
    if store is None:
        log.warning("gateway identify refused from %s: %s", client_ip, problem)
        # 409 for a shared token, 401 for an unknown one: the first is a
        # configuration the operator can fix, the second is a bad credential.
        raise HTTPException(
            status_code=409 if "여러 매장" in problem else 401, detail=problem)
    if not gateway_ip_allowed(store, client_ip):
        log.warning("[%s] gateway identify refused: unregistered IP %s (allowed: %s)",
                    store["store_id"], client_ip, store.get("gateway_ip"))
        raise HTTPException(
            status_code=403,
            detail=f"이 매장에 등록되지 않은 IP입니다: {client_ip}")
    log.info("[%s] gateway identified from %s", store["store_id"], client_ip)
    return {"store_id": store["store_id"], "name": store.get("name"),
            "grace_period_days": store.get("grace_period_days")}


async def gateway_socket(ws: WebSocket, want_store: str | None, token: str) -> None:
    """Shared body of both gateway socket routes.

    The token alone says which store this is; ``want_store`` is only present on
    the legacy path-addressed route, where it has to agree with the token.
    """
    client_ip = ws.client.host if ws.client else ""
    store, problem = await asyncio.to_thread(resolve_gateway_store, token, want_store)
    if store is None:
        # Refuse rather than quietly filing this gateway's data under whichever
        # store the token happened to match first.
        await ws.close(code=4401)
        log.warning("gateway rejected from %s (asked for %s): %s",
                    client_ip, want_store or "-", problem)
        return
    store_id = store["store_id"]
    if not gateway_ip_allowed(store, client_ip):
        # The gateway retries with backoff, so this warning repeats until the
        # IP is registered or the gateway is pointed elsewhere.
        await ws.close(code=4403)
        log.warning("[%s] gateway rejected: unregistered IP %s (allowed: %s)",
                    store_id, client_ip, store.get("gateway_ip"))
        return

    await ws.accept()
    hub = await manager.attach_gateway(
        store_id, ws, {"connected_at": iso(utcnow()), "ip": client_ip})
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


@app.websocket("/ws/gateway")
async def ws_gateway_by_token(ws: WebSocket, token: str = Query("")) -> None:
    await gateway_socket(ws, None, token)


@app.websocket("/ws/gateway/{store_id}")
async def ws_gateway(ws: WebSocket, store_id: str, token: str = Query("")) -> None:
    await gateway_socket(ws, store_id, token)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=os.environ.get("ATOM_HOST", "0.0.0.0"),
                port=int(os.environ.get("ATOM_PORT", "8000")))
