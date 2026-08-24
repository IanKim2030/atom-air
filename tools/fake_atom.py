"""A fake Atom Lite device, speaking the real MQTT contract.

This is the executable specification of what the ESP32 firmware must do, and it
lets the whole pipeline be tested without hardware:

    publishes  atom/{store}/sensor        12-byte SensorPacket, 1 Hz
    subscribes atom/{store}/ac/{dev_id}   8-byte AC control frame
    subscribes atom/{store}/ota/{dev_id}  JSON OTA command

On an AC frame it applies the setpoint, so the temperature it reports actually
drifts toward it. On an OTA command it downloads the firmware over HTTP from the
gateway and then raises FLAG_IR_READY -- which is what makes the gateway's SOTA
"verify" stage succeed for real rather than by simulation.

    python tools/fake_atom.py --store-id S001 --devices 2
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import threading
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

import protocol as p  # noqa: E402

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.exit("paho-mqtt is required: pip install paho-mqtt")


class FakeDevice:
    def __init__(self, dev_id: int) -> None:
        self.dev_id = dev_id
        self.seq = 0
        self.temp = 26.0 + random.uniform(-1.5, 1.5)
        self.hum = 48.0 + random.uniform(-5, 5)
        self.light = 700 + random.randint(-150, 150)
        self.ir_ready = False        # set only after a successful OTA
        self.ac_on = False
        self.setpoint = 24.0

    def step(self) -> bytes:
        self.seq = (self.seq + 1) & 0xFFFF
        flags = 0
        if self.ac_on:
            flags |= p.FLAG_AC_ON
            self.temp += (self.setpoint - self.temp) * 0.06
            self.hum += (42.0 - self.hum) * 0.03
        else:
            self.temp += random.uniform(-0.08, 0.10)
            self.hum += random.uniform(-0.15, 0.15)
        if self.ir_ready:
            flags |= p.FLAG_IR_READY

        self.temp = max(14.0, min(38.0, self.temp + random.uniform(-0.04, 0.04)))
        self.hum = max(20.0, min(85.0, self.hum))
        self.light = max(0, min(2000, self.light + random.randint(-25, 25)))
        return p.encode_sensor_packet(self.dev_id, self.seq, self.temp, self.hum,
                                      self.light, flags)


class FakeAtom:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.devices = {i: FakeDevice(i) for i in range(1, args.devices + 1)}
        self.topic_sensor = f"atom/{args.store_id}/sensor"
        self.topic_ac = f"atom/{args.store_id}/ac"
        self.topic_ota = f"atom/{args.store_id}/ota"
        self.stop = threading.Event()

        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                      client_id=f"fake-atom-{args.store_id}")
        except AttributeError:  # paho-mqtt 1.x
            self.client = mqtt.Client(client_id=f"fake-atom-{args.store_id}")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, *_):
        client.subscribe([(f"{self.topic_ac}/+", 1), (f"{self.topic_ota}/+", 1)])
        print(f"[atom] connected to {self.args.broker}:{self.args.port}, "
              f"listening on {self.topic_ac}/+ and {self.topic_ota}/+")

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
        state = "IR 송신" if dev.ir_ready else "IR 미탑재 -- 무시했을 것"
        print(f"[atom] dev{dev.dev_id} AC  {payload.hex()}  -> power={cmd['power']} "
              f"mode={cmd['mode']} temp={cmd['temp']} fan={cmd['fan']}  [{state}]")

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
        print(f"[atom] dev{dev.dev_id} OTA OK: {len(blob)} bytes in {elapsed:.2f}s "
              f"({cmd.get('protocol')}) -- rebooted, FLAG_IR_READY now set")

    def run(self) -> None:
        self.client.connect(self.args.broker, self.args.port, 30)
        self.client.loop_start()
        print(f"[atom] publishing {len(self.devices)} device(s) at 1Hz "
              f"to {self.topic_sensor}")
        try:
            while not self.stop.is_set():
                for dev in self.devices.values():
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

    ap = argparse.ArgumentParser(description="Fake Atom Lite over MQTT")
    ap.add_argument("--store-id", default="S001")
    ap.add_argument("--broker", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--devices", type=int, default=2)
    FakeAtom(ap.parse_args()).run()


if __name__ == "__main__":
    main()
