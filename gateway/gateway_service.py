"""Atom Air -- Store Management PC gateway service.

Sits between the in-store Atom Lite devices and the cloud::

    Atom Lite <--MQTT (Mosquitto)--> gateway_service <--WebSocket--> Cloud

Concurrency model -- every one of these is an independent asyncio task, so the
on-demand live stream can be switched on and off without ever interrupting the
local persistence or the licence checks:

    ingest      MQTT/simulator frames -> decode -> buffer  (+ bypass when live)
    persist     flush the 1-second buffer into SQLite (WAL)      [always on]
    minute      downsample to 1-minute stats, queue for upload   [always on]
    retention   purge raw rows older than a year                 [always on]
    license     daily POST /api/v1/store/authorize + grace check [always on]
    cloud       WebSocket link, reconnect w/ backoff, command handling

Operating modes
    normal  -- only 1-minute statistics are pushed to the cloud
    live    -- entered on START_LIVE_STREAM: every 1-second SensorPacket is
               bypassed to the cloud as-is, in real time. STOP_LIVE_STREAM (or
               losing the socket) drops straight back to normal mode.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import sqlite3
import sys
import threading
import time
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR.parent / "common"))

from protocol import (  # noqa: E402
    FLAG_AC_ON,
    FLAG_IR_READY,
    SENSOR_SIZE,
    PacketError,
    decode_ac_packet,
    decode_sensor_packet,
    encode_sensor_packet,
)

log = logging.getLogger("gateway")

DEFAULT_CLOUD_HTTP = os.environ.get("ATOM_CLOUD_HTTP", "http://127.0.0.1:8000")
DEFAULT_CLOUD_WS = os.environ.get("ATOM_CLOUD_WS", "ws://127.0.0.1:8000")
DEFAULT_TOKEN = os.environ.get("ATOM_GATEWAY_TOKEN", "dev-gateway-token")
APP_VERSION = "1.0.0"
RAW_RETENTION_DAYS = 365


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(value: Any) -> datetime | None:
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
# local storage -- SQLite in WAL mode, 1 year of 1-second raw readings
# --------------------------------------------------------------------------

LOCAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_readings (
    ts     INTEGER NOT NULL,          -- epoch seconds
    dev_id INTEGER NOT NULL,
    seq    INTEGER,
    temp   REAL, hum REAL, light INTEGER, flags INTEGER,
    PRIMARY KEY (dev_id, ts)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_raw_ts ON raw_readings (ts);

CREATE TABLE IF NOT EXISTS minute_stats (
    ts           INTEGER NOT NULL,
    dev_id       INTEGER NOT NULL,
    temp_avg REAL, temp_min REAL, temp_max REAL,
    hum_avg  REAL, light_avg REAL,
    sample_count INTEGER,
    uploaded     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (dev_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_minute_outbox ON minute_stats (uploaded, ts);
"""


class LocalStore:
    """All SQLite work happens here; callers hop into a thread via asyncio.to_thread."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path, timeout=15.0)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        return con

    def init(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, closing(self._connect()) as con:
            con.executescript(LOCAL_SCHEMA)
            con.commit()
        log.info("local sqlite (WAL) ready at %s", self.path)

    def insert_raw(self, rows: list[tuple]) -> int:
        if not rows:
            return 0
        with self._lock, closing(self._connect()) as con:
            con.executemany(
                "INSERT OR REPLACE INTO raw_readings (ts, dev_id, seq, temp, hum, light, flags)"
                " VALUES (?,?,?,?,?,?,?)", rows)
            con.commit()
        return len(rows)

    def upsert_minute(self, rows: list[tuple]) -> int:
        if not rows:
            return 0
        with self._lock, closing(self._connect()) as con:
            con.executemany(
                "INSERT OR REPLACE INTO minute_stats (ts, dev_id, temp_avg, temp_min, temp_max,"
                " hum_avg, light_avg, sample_count, uploaded) VALUES (?,?,?,?,?,?,?,?,0)", rows)
            con.commit()
        return len(rows)

    def outbox(self, limit: int = 240) -> list[dict]:
        with self._lock, closing(self._connect()) as con:
            rows = con.execute(
                "SELECT ts, dev_id, temp_avg, temp_min, temp_max, hum_avg, light_avg,"
                " sample_count FROM minute_stats WHERE uploaded=0 ORDER BY ts LIMIT ?",
                (limit,)).fetchall()
        return [dict(r) for r in rows]

    def mark_uploaded(self, keys: list[tuple]) -> None:
        if not keys:
            return
        with self._lock, closing(self._connect()) as con:
            con.executemany("UPDATE minute_stats SET uploaded=1 WHERE dev_id=? AND ts=?", keys)
            con.commit()

    def purge_raw(self, older_than_days: int = RAW_RETENTION_DAYS) -> int:
        cutoff = int(time.time()) - older_than_days * 86400
        with self._lock, closing(self._connect()) as con:
            deleted = con.execute("DELETE FROM raw_readings WHERE ts < ?", (cutoff,)).rowcount
            con.commit()
        return deleted or 0

    def counts(self) -> dict:
        with self._lock, closing(self._connect()) as con:
            raw = con.execute("SELECT COUNT(*) FROM raw_readings").fetchone()[0]
            minutes = con.execute("SELECT COUNT(*) FROM minute_stats").fetchone()[0]
            pending = con.execute(
                "SELECT COUNT(*) FROM minute_stats WHERE uploaded=0").fetchone()[0]
        return {"raw_rows": raw, "minute_rows": minutes, "pending_upload": pending}


# --------------------------------------------------------------------------
# licence: daily cloud check + locally persisted dynamic grace period
# --------------------------------------------------------------------------

class LicenseManager:
    """Keeps the store operating for `grace_period_days` after the last good check.

    The grace window is dictated by the cloud and cached in
    ``store_license_config.json`` so it survives restarts and cloud outages.
    """

    def __init__(self, store_id: str, config_path: Path, cloud_http: str,
                 default_grace_days: int = 30) -> None:
        self.store_id = store_id
        self.config_path = config_path
        self.cloud_http = cloud_http.rstrip("/")
        self.config = {
            "store_id": store_id,
            "grace_period_days": default_grace_days,
            "last_authorized_at": None,
            "last_status": "unknown",
            "license_expires_at": None,
            "device_fingerprint": None,
            "updated_at": None,
        }
        self.load()
        if not self.config.get("device_fingerprint"):
            self.config["device_fingerprint"] = f"{store_id}-{uuid.getnode():012x}"
            self.save()

    def load(self) -> None:
        if not self.config_path.exists():
            return
        try:
            self.config.update(json.loads(self.config_path.read_text(encoding="utf-8")))
            log.info("loaded licence config: grace=%sd last_ok=%s",
                     self.config.get("grace_period_days"),
                     self.config.get("last_authorized_at"))
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("could not read %s (%s); using defaults", self.config_path, exc)

    def save(self) -> None:
        self.config["updated_at"] = iso(utcnow())
        tmp = self.config_path.with_suffix(".tmp")
        try:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps(self.config, indent=2, ensure_ascii=False),
                           encoding="utf-8")
            tmp.replace(self.config_path)  # atomic: never leave a half-written config
        except OSError as exc:
            log.error("could not persist licence config: %s", exc)

    def evaluate_offline(self) -> dict:
        """Decide locally, using only the cached config -- the cloud may be down."""
        last_ok = parse_iso(self.config.get("last_authorized_at"))
        grace_days = int(self.config.get("grace_period_days") or 30)
        if last_ok is None:
            return {"operational": False, "reason": "no_successful_check",
                    "days_remaining": 0,
                    "message": "최초 인증이 완료되지 않았습니다."}
        deadline = last_ok + timedelta(days=grace_days)
        remaining = (deadline - utcnow()).total_seconds() / 86400.0
        if remaining <= 0:
            return {"operational": False, "reason": "grace_expired", "days_remaining": 0,
                    "message": f"유예기간 {grace_days}일이 만료되었습니다. 클라우드 인증이 필요합니다."}
        return {"operational": True, "reason": self.config.get("last_status") or "cached",
                "days_remaining": int(remaining),
                "grace_expires_at": iso(deadline),
                "message": f"오프라인 유예 {int(remaining)}일 남음"}

    def check_now(self) -> dict:
        """Blocking POST /api/v1/store/authorize. Runs in a worker thread."""
        import urllib.error
        import urllib.request

        payload = json.dumps({
            "store_id": self.store_id,
            "device_fingerprint": self.config.get("device_fingerprint"),
            "app_version": APP_VERSION,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"{self.cloud_http}/api/v1/store/authorize", data=payload,
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                verdict = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, json.JSONDecodeError, TimeoutError) as exc:
            offline = self.evaluate_offline()
            log.warning("licence check failed (%s) -- falling back to grace: %s",
                        exc, offline["message"])
            return {**offline, "online": False}

        # The cloud owns the grace terms; cache them for the next outage.
        self.config["grace_period_days"] = int(
            verdict.get("grace_period_days") or self.config["grace_period_days"])
        self.config["last_status"] = verdict.get("status")
        self.config["license_expires_at"] = verdict.get("license_expires_at")
        if verdict.get("authorized"):
            self.config["last_authorized_at"] = verdict.get("server_time") or iso(utcnow())
        self.save()

        if verdict.get("authorized"):
            return {"operational": True, "online": True, "reason": verdict.get("status"),
                    "days_remaining": verdict.get("days_remaining"),
                    "grace_expires_at": verdict.get("grace_expires_at"),
                    "message": verdict.get("message")}
        return {"operational": False, "online": True, "reason": verdict.get("status"),
                "days_remaining": 0, "message": verdict.get("message")}


# --------------------------------------------------------------------------
# local HTTP OTA server (port 8080) used by the SOTA pipeline
# --------------------------------------------------------------------------

class _QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        log.debug("ota-http %s", fmt % args)


class OtaHttpServer:
    """Serves the firmware directory so an Atom can pull a .bin over the LAN in ~5s."""

    def __init__(self, directory: Path, port: int = 8080) -> None:
        self.directory = directory
        self.port = port
        self._httpd: ThreadingHTTPServer | None = None

    def start(self) -> bool:
        self.directory.mkdir(parents=True, exist_ok=True)
        handler = partial(_QuietHandler, directory=str(self.directory))
        try:
            self._httpd = ThreadingHTTPServer(("0.0.0.0", self.port), handler)
        except OSError as exc:
            log.error("OTA HTTP server could not bind :%d (%s)", self.port, exc)
            return False
        threading.Thread(target=self._httpd.serve_forever, name="ota-http",
                         daemon=True).start()
        log.info("OTA HTTP server serving %s on :%d", self.directory, self.port)
        return True

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd = None


# --------------------------------------------------------------------------
# MQTT bridge (Mosquitto). Optional: --simulate runs without a broker.
# --------------------------------------------------------------------------

class MqttBridge:
    """paho-mqtt runs its own network thread; frames are handed to asyncio safely."""

    def __init__(self, host: str, port: int, store_id: str,
                 loop: asyncio.AbstractEventLoop, on_sensor) -> None:
        self.host, self.port, self.store_id = host, port, store_id
        self.loop, self.on_sensor = loop, on_sensor
        self.client = None
        self.connected = False
        self.topic_sensor = f"atom/{store_id}/sensor"
        self.topic_ac = f"atom/{store_id}/ac"
        self.topic_ota = f"atom/{store_id}/ota"

    def start(self) -> bool:
        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            log.warning("paho-mqtt is not installed -- MQTT disabled "
                        "(use --simulate to run without hardware)")
            return False

        client_id = f"gw-{self.store_id}-{os.getpid()}"
        if hasattr(mqtt, "CallbackAPIVersion"):   # paho-mqtt 2.x
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id,
                                      clean_session=True)
        else:                                     # paho-mqtt 1.x
            self.client = mqtt.Client(client_id=client_id, clean_session=True)

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        try:
            self.client.connect_async(self.host, self.port, keepalive=30)
            self.client.loop_start()
        except OSError as exc:
            log.warning("MQTT connect to %s:%d failed: %s", self.host, self.port, exc)
            return False
        return True

    # The trailing *_ absorbs the extra `properties` argument paho 2.x passes.
    def _on_connect(self, client, _userdata, _flags, reason_code, *_) -> None:
        if getattr(reason_code, "value", reason_code) == 0:
            self.connected = True
            client.subscribe([(self.topic_sensor, 0), (f"{self.topic_sensor}/+", 0)])
            log.info("MQTT connected to %s:%d, subscribed to %s/#",
                     self.host, self.port, self.topic_sensor)
        else:
            log.warning("MQTT connection refused (%s)", reason_code)

    def _on_disconnect(self, _client, _userdata, *args) -> None:
        # v1 passes (rc,); v2 passes (disconnect_flags, reason_code, properties).
        reason = args[1] if len(args) >= 3 else (args[0] if args else "unknown")
        self.connected = False
        log.warning("MQTT disconnected (%s); paho will retry", reason)

    def _on_message(self, _client, _userdata, message) -> None:
        payload = message.payload
        # Hop from the paho thread onto the event loop.
        self.loop.call_soon_threadsafe(self.on_sensor, payload)

    def publish_ac(self, target_id: int, packet: bytes) -> bool:
        if self.client is None or not self.connected:
            return False
        self.client.publish(f"{self.topic_ac}/{target_id}", packet, qos=1)
        return True

    def publish_ota(self, target_id: int, command: dict) -> bool:
        if self.client is None or not self.connected:
            return False
        self.client.publish(f"{self.topic_ota}/{target_id}",
                            json.dumps(command, ensure_ascii=False), qos=1)
        return True

    def stop(self) -> None:
        if self.client is not None:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                pass


# --------------------------------------------------------------------------
# the service
# --------------------------------------------------------------------------

@dataclass
class MinuteBucket:
    temps: list[float] = field(default_factory=list)
    hums: list[float] = field(default_factory=list)
    lights: list[float] = field(default_factory=list)

    def add(self, temp: float, hum: float, light: float) -> None:
        self.temps.append(temp)
        self.hums.append(hum)
        self.lights.append(light)

    def finalize(self) -> dict:
        return {
            "temp_avg": round(sum(self.temps) / len(self.temps), 3),
            "temp_min": round(min(self.temps), 3),
            "temp_max": round(max(self.temps), 3),
            "hum_avg": round(sum(self.hums) / len(self.hums), 3),
            "light_avg": round(sum(self.lights) / len(self.lights), 1),
            "sample_count": len(self.temps),
        }


class GatewayService:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.store_id = args.store_id
        self.store = LocalStore(Path(args.db))
        self.license = LicenseManager(args.store_id, Path(args.license_config),
                                      args.cloud_http, args.grace_days)
        self.ota_http = OtaHttpServer(Path(args.firmware_dir), args.ota_port)

        self.loop: asyncio.AbstractEventLoop | None = None
        self.mqtt: MqttBridge | None = None
        self.cloud = None                      # active websocket connection, if any

        self.live_streaming = False            # <- the on-demand switch
        self.licensed = True                   # updated by the licence task
        self.raw_buffer: list[tuple] = []
        self.minutes: dict[tuple[int, int], MinuteBucket] = {}
        self.ingest_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=4096)
        self.ac_state = {"target_id": 1, "power": 0, "mode": "cool", "temp": 24,
                         "fan": "auto"}
        self.ir_ready: set[int] = set()
        self.stats = {"packets": 0, "bypassed": 0, "dropped": 0}
        self._stop = asyncio.Event()

    # -- ingest ------------------------------------------------------------

    def on_sensor_frame(self, payload: bytes) -> None:
        """Called from the MQTT thread (via call_soon_threadsafe) and the simulator."""
        try:
            self.ingest_queue.put_nowait(payload)
        except asyncio.QueueFull:
            self.stats["dropped"] += 1
            if self.stats["dropped"] % 100 == 1:
                log.warning("ingest queue full; dropped %d frames", self.stats["dropped"])

    async def task_ingest(self) -> None:
        """Decode -> always persist locally -> bypass to cloud only while live."""
        while not self._stop.is_set():
            payload = await self.ingest_queue.get()
            for offset in range(0, len(payload) - len(payload) % SENSOR_SIZE, SENSOR_SIZE):
                frame = payload[offset:offset + SENSOR_SIZE]
                try:
                    reading = decode_sensor_packet(frame)
                except PacketError as exc:
                    log.warning("bad sensor packet: %s", exc)
                    continue

                self.stats["packets"] += 1
                now = int(time.time())
                if reading["ir_ready"]:
                    self.ir_ready.add(reading["dev_id"])

                # (1) Local persistence -- unconditional, never gated on the cloud.
                self.raw_buffer.append((now, reading["dev_id"], reading["seq"],
                                        reading["temp"], reading["hum"],
                                        reading["light"], reading["flags"]))
                bucket = self.minutes.setdefault((reading["dev_id"], now // 60 * 60),
                                                 MinuteBucket())
                bucket.add(reading["temp"], reading["hum"], reading["light"])

                # (2) On-demand bypass -- the raw frame goes up untouched.
                if self.live_streaming and self.cloud is not None:
                    try:
                        await self.cloud.send(frame)
                        self.stats["bypassed"] += 1
                    except Exception as exc:
                        log.warning("live bypass failed: %s", exc)
                        self.live_streaming = False

    # -- persistence -------------------------------------------------------

    async def task_persist(self) -> None:
        """Flush the 1-second buffer to SQLite once a second. Always running."""
        while not self._stop.is_set():
            await asyncio.sleep(1.0)
            if not self.raw_buffer:
                continue
            batch, self.raw_buffer = self.raw_buffer, []
            try:
                await asyncio.to_thread(self.store.insert_raw, batch)
            except sqlite3.Error as exc:
                log.error("local raw insert failed (%d rows lost): %s", len(batch), exc)

    async def task_minute(self) -> None:
        """Close finished minute buckets, store them, and queue them for the cloud."""
        while not self._stop.is_set():
            await asyncio.sleep(5.0)
            current_minute = int(time.time()) // 60 * 60
            closed = [key for key in self.minutes if key[1] < current_minute]
            if not closed:
                continue

            rows, points = [], []
            for key in closed:
                dev_id, ts = key
                summary = self.minutes.pop(key).finalize()
                rows.append((ts, dev_id, summary["temp_avg"], summary["temp_min"],
                             summary["temp_max"], summary["hum_avg"],
                             summary["light_avg"], summary["sample_count"]))
                points.append({"dev_id": dev_id, "ts": ts, **summary})

            try:
                await asyncio.to_thread(self.store.upsert_minute, rows)
            except sqlite3.Error as exc:
                log.error("minute stat write failed: %s", exc)
                continue
            log.info("downsampled %d minute bucket(s); live=%s", len(points),
                     self.live_streaming)
            await self.flush_outbox()

    async def flush_outbox(self) -> None:
        """Ship pending minute stats. Anything undelivered stays queued in SQLite."""
        if self.cloud is None or not self.licensed:
            return
        pending = await asyncio.to_thread(self.store.outbox)
        if not pending:
            return
        try:
            await self.cloud.send(json.dumps({"type": "minute_stats", "points": pending},
                                             ensure_ascii=False))
        except Exception as exc:
            log.warning("minute stat upload failed, keeping %d rows queued: %s",
                        len(pending), exc)
            return
        await asyncio.to_thread(self.store.mark_uploaded,
                                [(p["dev_id"], p["ts"]) for p in pending])
        log.info("uploaded %d minute stat row(s)", len(pending))

    async def task_retention(self) -> None:
        """Keep a rolling year of 1-second data (~5GB)."""
        while not self._stop.is_set():
            try:
                deleted = await asyncio.to_thread(self.store.purge_raw, RAW_RETENTION_DAYS)
                if deleted:
                    log.info("retention: purged %d raw rows older than %d days",
                             deleted, RAW_RETENTION_DAYS)
            except sqlite3.Error as exc:
                log.error("retention purge failed: %s", exc)
            await asyncio.sleep(6 * 3600)

    # -- licence -----------------------------------------------------------

    async def task_license(self) -> None:
        """Daily authorize call. Independent of the cloud socket and of live mode."""
        while not self._stop.is_set():
            verdict = await asyncio.to_thread(self.license.check_now)
            was = self.licensed
            self.licensed = bool(verdict.get("operational"))
            level = log.info if self.licensed else log.error
            level("licence: %s (%s, %s일 남음)", verdict.get("message"),
                  verdict.get("reason"), verdict.get("days_remaining"))
            if was and not self.licensed:
                log.error("licence lapsed -- AC control and cloud upload are now blocked; "
                          "local logging continues")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.args.license_interval)
            except asyncio.TimeoutError:
                pass

    # -- cloud link --------------------------------------------------------

    async def task_cloud(self) -> None:
        try:
            from websockets.asyncio.client import connect as ws_connect  # websockets >= 13
        except ImportError:  # pragma: no cover - older releases
            from websockets.client import connect as ws_connect

        url = (f"{self.args.cloud_ws.rstrip('/')}/ws/gateway/{self.store_id}"
               f"?token={self.args.token}")
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with ws_connect(url, max_queue=64, ping_interval=20) as conn:
                    self.cloud = conn
                    backoff = 1.0
                    log.info("cloud link established: %s", url.split("?")[0])
                    await conn.send(json.dumps({
                        "type": "gateway_status",
                        "info": {"app_version": APP_VERSION,
                                 "mqtt": bool(self.mqtt and self.mqtt.connected),
                                 "simulated": self.args.simulate,
                                 "licensed": self.licensed}}, ensure_ascii=False))
                    await self.flush_outbox()   # drain anything buffered while offline
                    async for message in conn:
                        await self.handle_cloud_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("cloud link down (%s); retrying in %.0fs", exc, backoff)
            finally:
                self.cloud = None
                if self.live_streaming:
                    log.info("cloud link lost -> leaving live mode, back to 1-minute stats")
                    self.live_streaming = False
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 30.0)

    async def handle_cloud_message(self, message) -> None:
        if isinstance(message, bytes):
            return
        try:
            msg = json.loads(message)
        except json.JSONDecodeError:
            log.warning("cloud sent non-JSON text")
            return

        cmd = msg.get("cmd")

        if cmd == "START_LIVE_STREAM":
            if not self.live_streaming:
                self.live_streaming = True
                log.info("START_LIVE_STREAM (viewers=%s) -> bypassing 1s packets to cloud",
                         msg.get("viewers"))

        elif cmd == "STOP_LIVE_STREAM":
            if self.live_streaming:
                self.live_streaming = False
                log.info("STOP_LIVE_STREAM -> back to normal mode "
                         "(1-minute statistics only)")

        elif cmd == "AC_CONTROL":
            await self.handle_ac_control(msg)

        elif cmd == "DEPLOY_FIRMWARE":
            asyncio.create_task(self.run_sota(msg))   # long-running; don't block the reader

        else:
            log.debug("unhandled cloud command %r", cmd)

    async def handle_ac_control(self, msg: dict) -> None:
        try:
            packet = bytes.fromhex(msg["packet_hex"])
            decoded = decode_ac_packet(packet)
        except (KeyError, ValueError, PacketError) as exc:
            log.warning("rejecting AC command: %s", exc)
            await self.send_cloud({"type": "ac_ack", "ok": False, "message": str(exc)})
            return

        if not self.licensed:
            await self.send_cloud({"type": "ac_ack", "ok": False,
                                   "message": "라이선스 유예기간 만료로 제어가 차단되었습니다."})
            return

        target = decoded["target_id"]
        log.info("AC_CONTROL -> dev %d: power=%s mode=%s temp=%d fan=%s  [%s]",
                 target, decoded["power"], decoded["mode"], decoded["temp"],
                 decoded["fan"], packet.hex())

        if target not in self.ir_ready:
            log.warning("device %d has not reported IR-capable firmware; "
                        "sending anyway (run SOTA if it does not respond)", target)

        delivered = self.mqtt.publish_ac(target, packet) if self.mqtt else False
        if not delivered and not self.args.simulate:
            await self.send_cloud({"type": "ac_ack", "ok": False,
                                   "message": "MQTT 브로커에 연결되어 있지 않습니다."})
            return

        self.ac_state = {k: decoded[k] for k in ("target_id", "power", "mode", "temp", "fan")}
        await self.send_cloud({"type": "ac_ack", "ok": True, "state": self.ac_state,
                               "message": "IR 명령을 전송했습니다."})

    async def send_cloud(self, payload: dict) -> bool:
        if self.cloud is None:
            return False
        try:
            await self.cloud.send(json.dumps(payload, ensure_ascii=False))
            return True
        except Exception:
            return False

    # -- SOTA --------------------------------------------------------------

    async def run_sota(self, msg: dict) -> None:
        """Stage the AC-capable firmware and drive the local fast HTTP OTA."""
        target = int(msg.get("target_id") or 1)
        model = msg.get("model") or msg.get("model_id") or "unknown"
        proto = msg.get("protocol") or "GENERIC"

        async def report(stage: str, percent: int, message: str, ok: bool = True) -> None:
            await self.send_cloud({"type": "sota_progress", "stage": stage,
                                   "percent": percent, "message": message,
                                   "model": model, "ok": ok})
            log.info("SOTA[%s] %d%% - %s", stage, percent, message)

        await report("prepare", 10, f"{model} ({proto}) 펌웨어를 준비합니다.")
        firmware_dir = Path(self.args.firmware_dir)
        binary = firmware_dir / f"atom_ac_{proto.lower()}.bin"

        if not binary.exists():
            if not self.args.simulate:
                await report("failed", 10,
                             f"펌웨어 파일이 없습니다: {binary}", ok=False)
                return
            # Simulation only: stand in for the real IRremoteESP8266 build output.
            firmware_dir.mkdir(parents=True, exist_ok=True)
            binary.write_bytes(b"ATOMAIR-SIMULATED-FIRMWARE\x00" * 4096)
            await report("prepare", 20,
                         f"[시뮬레이션] 임시 펌웨어 이미지를 생성했습니다: {binary.name}")

        size_kb = binary.stat().st_size / 1024
        url = f"http://{self.args.ota_host}:{self.args.ota_port}/{binary.name}"
        await report("serve", 35, f"로컬 OTA 서버에서 배포 중 ({size_kb:.0f}KB) - {url}")

        sent = self.mqtt.publish_ota(target, {
            "cmd": "OTA", "url": url, "protocol": proto, "model": model,
            "size": binary.stat().st_size}) if self.mqtt else False
        if not sent and not self.args.simulate:
            await report("failed", 35, "MQTT 브로커에 연결되어 있지 않아 OTA를 지시할 수 없습니다.",
                         ok=False)
            return
        await report("notify", 50, f"디바이스 {target}에 OTA 명령을 전달했습니다.")

        # Rule 4 budgets ~5s for the LAN flash; poll for the device to come back.
        for percent in (65, 80, 90):
            await asyncio.sleep(1.6)
            await report("flashing", percent, "디바이스가 펌웨어를 내려받아 기록하는 중입니다...")

        if self.args.simulate:
            self.ir_ready.add(target)
        deadline = time.time() + 15
        while target not in self.ir_ready and time.time() < deadline:
            await asyncio.sleep(0.5)

        if target in self.ir_ready:
            await report("done", 100, f"완료. 디바이스 {target}에서 AC IR 제어를 사용할 수 있습니다.")
        else:
            await report("verify", 95,
                         "플래싱은 지시했으나 디바이스가 아직 IR 준비 상태를 보고하지 않았습니다.",
                         ok=False)

    # -- device simulator (no hardware / no broker) ------------------------

    async def task_simulate(self) -> None:
        """Synthesize Atom Lite traffic through the exact same ingest path."""
        devices = []
        for dev_id in range(1, self.args.devices + 1):
            devices.append({"dev_id": dev_id, "seq": 0,
                            "temp": 26.0 + random.uniform(-1.5, 1.5),
                            "hum": 48.0 + random.uniform(-5, 5),
                            "light": 700 + random.randint(-150, 150)})
        log.info("simulator: emitting %d device(s) at 1Hz", len(devices))
        while not self._stop.is_set():
            for dev in devices:
                dev["seq"] = (dev["seq"] + 1) & 0xFFFF
                flags = 0
                if self.ac_state["power"] and dev["dev_id"] == self.ac_state["target_id"]:
                    flags |= FLAG_AC_ON
                    # Drift toward the setpoint so the remote visibly moves the chart.
                    target = float(self.ac_state["temp"])
                    dev["temp"] += (target - dev["temp"]) * 0.06
                    dev["hum"] += (42.0 - dev["hum"]) * 0.03
                else:
                    dev["temp"] += random.uniform(-0.08, 0.10)
                    dev["hum"] += random.uniform(-0.15, 0.15)
                if dev["dev_id"] in self.ir_ready:
                    flags |= FLAG_IR_READY

                dev["temp"] = max(14.0, min(38.0, dev["temp"] + random.uniform(-0.04, 0.04)))
                dev["hum"] = max(20.0, min(85.0, dev["hum"]))
                dev["light"] = max(0, min(2000, dev["light"] + random.randint(-25, 25)))

                self.on_sensor_frame(encode_sensor_packet(
                    dev["dev_id"], dev["seq"], dev["temp"], dev["hum"],
                    dev["light"], flags))
            await asyncio.sleep(1.0)

    async def task_heartbeat(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(60)
            counts = await asyncio.to_thread(self.store.counts)
            log.info("status: mode=%s packets=%d bypassed=%d dropped=%d "
                     "raw=%d minute=%d pending=%d licensed=%s",
                     "LIVE" if self.live_streaming else "normal",
                     self.stats["packets"], self.stats["bypassed"], self.stats["dropped"],
                     counts["raw_rows"], counts["minute_rows"], counts["pending_upload"],
                     self.licensed)

    # -- lifecycle ---------------------------------------------------------

    async def run(self) -> None:
        self.loop = asyncio.get_running_loop()
        await asyncio.to_thread(self.store.init)
        self.ota_http.start()

        self.mqtt = MqttBridge(self.args.mqtt_host, self.args.mqtt_port, self.store_id,
                               self.loop, self.on_sensor_frame)
        if not self.mqtt.start():
            self.mqtt = None
            if not self.args.simulate:
                log.error("no MQTT and no --simulate: this gateway will receive no data")

        tasks = [
            asyncio.create_task(self.task_ingest(), name="ingest"),
            asyncio.create_task(self.task_persist(), name="persist"),
            asyncio.create_task(self.task_minute(), name="minute"),
            asyncio.create_task(self.task_retention(), name="retention"),
            asyncio.create_task(self.task_license(), name="license"),
            asyncio.create_task(self.task_cloud(), name="cloud"),
            asyncio.create_task(self.task_heartbeat(), name="heartbeat"),
        ]
        if self.args.simulate:
            tasks.append(asyncio.create_task(self.task_simulate(), name="simulate"))

        log.info("gateway '%s' running -- normal mode (1-minute stats); "
                 "waiting for START_LIVE_STREAM", self.store_id)
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            self._stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self.mqtt:
                self.mqtt.stop()
            self.ota_http.stop()
            if self.raw_buffer:
                await asyncio.to_thread(self.store.insert_raw, self.raw_buffer)
                log.info("flushed %d buffered readings on shutdown", len(self.raw_buffer))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Atom Air store gateway service")
    p.add_argument("--store-id", default=os.environ.get("ATOM_STORE_ID", "S001"))
    p.add_argument("--cloud-ws", default=DEFAULT_CLOUD_WS)
    p.add_argument("--cloud-http", default=DEFAULT_CLOUD_HTTP)
    p.add_argument("--token", default=DEFAULT_TOKEN)
    p.add_argument("--mqtt-host", default=os.environ.get("ATOM_MQTT_HOST", "127.0.0.1"))
    p.add_argument("--mqtt-port", type=int,
                   default=int(os.environ.get("ATOM_MQTT_PORT", "1883")))
    p.add_argument("--db", default=str(BASE_DIR / "store_data.db"))
    p.add_argument("--license-config", default=str(BASE_DIR / "store_license_config.json"))
    p.add_argument("--grace-days", type=int, default=30,
                   help="fallback grace window before the cloud has ever answered")
    p.add_argument("--license-interval", type=float, default=86400.0,
                   help="seconds between authorize calls (default: daily)")
    p.add_argument("--firmware-dir", default=str(BASE_DIR / "firmware"))
    p.add_argument("--ota-host", default=os.environ.get("ATOM_OTA_HOST", "127.0.0.1"),
                   help="address the Atom devices use to reach this PC")
    p.add_argument("--ota-port", type=int, default=8080)
    p.add_argument("--simulate", action="store_true",
                   help="generate Atom Lite traffic locally (no hardware/broker needed)")
    p.add_argument("--devices", type=int, default=2, help="simulated device count")
    p.add_argument("--log-level", default=os.environ.get("ATOM_LOG_LEVEL", "INFO"))
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # Windows consoles default to a legacy codepage, which mangles the Korean
    # status messages this service logs.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=args.log_level.upper(),
                        format="%(asctime)s  %(levelname)-7s %(name)s  %(message)s")
    service = GatewayService(args)
    try:
        asyncio.run(service.run())
    except KeyboardInterrupt:
        log.info("shutting down")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
