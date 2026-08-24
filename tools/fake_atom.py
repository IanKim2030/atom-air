"""A fake Atom Lite device, speaking the real MQTT contract.

This is the executable specification of what the ESP32 firmware must do, and it
lets the whole pipeline run without hardware:

    publishes  atom/{store}/sensor        12-byte SensorPacket, 1 Hz
    subscribes atom/{store}/ac/{dev_id}   8-byte AC control frame
    subscribes atom/{store}/ota/{dev_id}  JSON OTA command

It behaves like the real thing in the ways that matter:

  * an AC frame changes the setpoint, so the temperature it reports really
    drifts toward it;
  * an OTA command makes it download the firmware over HTTP and only then raise
    FLAG_IR_READY -- which is what makes the gateway's SOTA "verify" stage pass
    for real rather than by simulation;
  * flashed firmware **survives a restart**, as it would on real hardware, so a
    demo does not have to re-run SOTA every time;
  * illuminance follows the clock, and the sensor occasionally faults.

    python tools/fake_atom.py --store-id S001 --devices 2
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
import threading
import time
import urllib.request
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

import protocol as p  # noqa: E402

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.exit("paho-mqtt is required: pip install paho-mqtt")


def ambient_light(now: datetime) -> float:
    """A store's illuminance over the day: dim at night, bright at midday."""
    hour = now.hour + now.minute / 60
    if hour < 6 or hour >= 22:
        return 8.0                       # closed, security lighting only
    # Half sine from opening to closing, peaking mid-afternoon.
    phase = (hour - 6) / 16
    return 120 + 1400 * math.sin(math.pi * phase)


class FakeDevice:
    def __init__(self, dev_id: int, rng: random.Random) -> None:
        self.dev_id = dev_id
        self.rng = rng
        self.seq = 0
        self.temp = 26.0 + rng.uniform(-1.5, 1.5)
        self.hum = 48.0 + rng.uniform(-5, 5)
        self.light = ambient_light(datetime.now())
        self.ir_ready = False        # set only after a successful OTA
        self.protocol = None         # which IR protocol was flashed
        self.ac_on = False
        self.setpoint = 24.0
        self.faulted = False

    def step(self) -> bytes:
        self.seq = (self.seq + 1) & 0xFFFF
        rng = self.rng
        flags = 0

        # A faulty read repeats the last values and raises the fault flag,
        # which is what a real DHT sensor timing out looks like downstream.
        if self.faulted:
            flags |= p.FLAG_SENSOR_FAULT
        else:
            if self.ac_on:
                self.temp += (self.setpoint - self.temp) * 0.06
                self.hum += (42.0 - self.hum) * 0.03
            else:
                self.temp += rng.uniform(-0.08, 0.10)
                self.hum += rng.uniform(-0.15, 0.15)
            self.temp = max(14.0, min(38.0, self.temp + rng.uniform(-0.04, 0.04)))
            self.hum = max(20.0, min(85.0, self.hum))
            target_light = ambient_light(datetime.now())
            self.light += (target_light - self.light) * 0.05 + rng.uniform(-8, 8)
            self.light = max(0.0, min(2000.0, self.light))

        if self.ac_on:
            flags |= p.FLAG_AC_ON
        if self.ir_ready:
            flags |= p.FLAG_IR_READY

        return p.encode_sensor_packet(self.dev_id, self.seq, self.temp, self.hum,
                                      int(self.light), flags)


class FakeAtom:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        rng = random.Random(args.seed)
        self.devices = {i: FakeDevice(i, random.Random(rng.random()))
                        for i in range(1, args.devices + 1)}
        self.topic_sensor = f"atom/{args.store_id}/sensor"
        self.topic_ac = f"atom/{args.store_id}/ac"
        self.topic_ota = f"atom/{args.store_id}/ota"
        self.state_path = Path(args.state_file)
        self.stop = threading.Event()
        self._load_state()

        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                      client_id=f"fake-atom-{args.store_id}")
        except AttributeError:  # paho-mqtt 1.x
            self.client = mqtt.Client(client_id=f"fake-atom-{args.store_id}")
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

    # -- flashed firmware persists across restarts, as on real hardware ----

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            saved = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        for dev_id_text, entry in (saved.get(self.args.store_id) or {}).items():
            dev = self.devices.get(int(dev_id_text))
            if dev is not None and entry.get("ir_ready"):
                dev.ir_ready = True
                dev.protocol = entry.get("protocol")
                print(f"[atom] dev{dev.dev_id} retains flashed firmware "
                      f"({dev.protocol}) from a previous run")

    def _save_state(self) -> None:
        saved = {}
        if self.state_path.exists():
            try:
                saved = json.loads(self.state_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                saved = {}
        saved[self.args.store_id] = {
            str(d.dev_id): {"ir_ready": d.ir_ready, "protocol": d.protocol}
            for d in self.devices.values()
        }
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(json.dumps(saved, indent=2), encoding="utf-8")
        except OSError as exc:
            print(f"[atom] could not persist device state: {exc}")

    # -- MQTT --------------------------------------------------------------

    def _on_connect(self, client, *_):
        client.subscribe([(f"{self.topic_ac}/+", 1), (f"{self.topic_ota}/+", 1)])
        print(f"[atom] connected to {self.args.broker}:{self.args.port}, "
              f"listening on {self.topic_ac}/+ and {self.topic_ota}/+")

    def _on_disconnect(self, *_):
        print("[atom] broker connection lost; paho will retry")

    def _on_message(self, _client, _userdata, msg):
        try:
            dev_id = int(msg.topic.rsplit("/", 1)[-1])
        except ValueError:
            return
        dev = self.devices.get(dev_id)
        if dev is None:
            print(f"[atom] command for unknown device {dev_id}, ignoring")
            return

        if msg.topic.startswith(self.topic_ac):
            self._handle_ac(dev, msg.payload)
        elif msg.topic.startswith(self.topic_ota):
            threading.Thread(target=self._handle_ota, args=(dev, msg.payload),
                             daemon=True).start()

    def _handle_ac(self, dev: FakeDevice, payload: bytes) -> None:
        try:
            cmd = p.decode_ac_packet(payload)
        except p.PacketError as exc:
            print(f"[atom] dev{dev.dev_id} REJECTED AC frame: {exc}")
            return
        dev.ac_on = bool(cmd["power"])
        dev.setpoint = float(cmd["temp"])
        note = f"IR 송신 ({dev.protocol})" if dev.ir_ready else "IR 미탑재 -- 무시했을 것"
        print(f"[atom] dev{dev.dev_id} AC  {payload.hex()}  -> power={cmd['power']} "
              f"mode={cmd['mode']} temp={cmd['temp']} fan={cmd['fan']}  [{note}]")

    def _handle_ota(self, dev: FakeDevice, payload: bytes) -> None:
        try:
            cmd = json.loads(payload)
        except json.JSONDecodeError as exc:
            print(f"[atom] dev{dev.dev_id} bad OTA command: {exc}")
            return
        url = cmd.get("url", "")
        print(f"[atom] dev{dev.dev_id} OTA start <- {url}")
        try:
            started = time.time()
            with urllib.request.urlopen(url, timeout=20) as resp:
                blob = resp.read()
            elapsed = time.time() - started
        except Exception as exc:
            print(f"[atom] dev{dev.dev_id} OTA FAILED to fetch: {exc}")
            return

        expected = cmd.get("size")
        if expected is not None and len(blob) != expected:
            print(f"[atom] dev{dev.dev_id} OTA size mismatch: "
                  f"got {len(blob)}, announced {expected}")
            return

        # A real device writes the image, reboots, then comes back IR-capable.
        time.sleep(1.0)
        dev.ir_ready = True
        dev.protocol = cmd.get("protocol")
        self._save_state()
        print(f"[atom] dev{dev.dev_id} OTA OK: {len(blob)} bytes in {elapsed:.2f}s "
              f"({dev.protocol}) -- rebooted, FLAG_IR_READY now set")

    # -- main loop ---------------------------------------------------------

    def _maybe_fault(self, dev: FakeDevice) -> None:
        if self.args.fault_rate <= 0:
            return
        if dev.faulted:
            if dev.rng.random() < 0.25:          # faults clear after a few seconds
                dev.faulted = False
                print(f"[atom] dev{dev.dev_id} sensor recovered")
        elif dev.rng.random() < self.args.fault_rate:
            dev.faulted = True
            print(f"[atom] dev{dev.dev_id} sensor FAULT (FLAG_SENSOR_FAULT set)")

    def run(self) -> None:
        self.client.connect(self.args.broker, self.args.port, 30)
        self.client.loop_start()
        print(f"[atom] publishing {len(self.devices)} device(s) at 1Hz "
              f"to {self.topic_sensor}")
        try:
            while not self.stop.is_set():
                for dev in self.devices.values():
                    self._maybe_fault(dev)
                    self.client.publish(self.topic_sensor, dev.step(), qos=0)
                time.sleep(1.0)
        except KeyboardInterrupt:
            pass
        finally:
            self.client.loop_stop()
            self.client.disconnect()
            print("[atom] stopped")


def main() -> None:
    # Windows consoles default to a legacy codepage, which mangles the Korean
    # status text below.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    default_state = Path(__file__).resolve().parent.parent / ".mockdata" / "fake_atom_state.json"

    ap = argparse.ArgumentParser(description="Fake Atom Lite over MQTT")
    ap.add_argument("--store-id", default="S001")
    ap.add_argument("--broker", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--devices", type=int, default=2)
    ap.add_argument("--state-file", default=str(default_state),
                    help="where flashed-firmware state is remembered between runs")
    ap.add_argument("--fault-rate", type=float, default=0.0,
                    help="per-second chance of a sensor fault, e.g. 0.01")
    ap.add_argument("--seed", type=int, default=None, help="reproducible readings")
    FakeAtom(ap.parse_args()).run()


if __name__ == "__main__":
    main()
