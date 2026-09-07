#!/usr/bin/env python3
"""A four-button remote for one board, straight over MQTT.

The dashboard is the product; this is the bench version of it. It talks to the
same broker the store PC already runs, on the same topics the firmware already
subscribes to, so it needs no gateway, no cloud and no browser -- just a
Windows PC and a board on the same network.

    python tools/pc_remote.py                 # interactive: 4 keys
    python tools/pc_remote.py on              # one-shot, scriptable
    python tools/pc_remote.py up up up

Four functions, because that is what the remote being learned has: power on,
power off, temperature up, temperature down.

The board is what turns these into button presses. An AC frame carries full
state, and the firmware compares it against what it last applied and fires the
learned slots for the difference -- so "temp up" here is simply the same frame
with one more degree in it. That also means the PC must remember what it last
asked for, which is what --state-file is: without it, a fresh process would
think the setpoint is 24 again and send nothing.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import paho.mqtt.client as mqtt

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common import protocol as p   # noqa: E402

# The firmware's own starting assumption (appliedTemp = 24), so a first
# "up" from a cold start asks for 25 and moves one step.
DEFAULT_TEMP = 24
TEMP_MIN, TEMP_MAX = 18, 30


class Remote:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.topic_ac = f"atom/{args.store_id}/ac/{args.dev}"
        self.topic_log = f"atom/{args.store_id}/log/{args.dev}"
        self.topic_sensor = f"atom/{args.store_id}/sensor"
        self.state_path = Path(args.state_file)
        self.state = self._load_state()
        self.last_packet_at = 0.0

        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                      client_id=f"pc-remote-{args.store_id}")
        except AttributeError:              # paho-mqtt 1.x
            self.client = mqtt.Client(client_id=f"pc-remote-{args.store_id}")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    # -- state -------------------------------------------------------------

    def _load_state(self) -> dict:
        try:
            saved = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            saved = {}
        entry = saved.get(f"{self.args.store_id}/{self.args.dev}") or {}
        return {"power": bool(entry.get("power", False)),
                "temp": int(entry.get("temp", DEFAULT_TEMP))}

    def _save_state(self) -> None:
        try:
            saved = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            saved = {}
        saved[f"{self.args.store_id}/{self.args.dev}"] = self.state
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(json.dumps(saved, indent=2), encoding="utf-8")
        except OSError as exc:
            print(f"[remote] could not save state: {exc}")

    # -- mqtt --------------------------------------------------------------

    def _on_connect(self, client, *_):
        # The board mirrors its console onto the log topic, so subscribing is
        # how a keypress gets an answer: '[ir] raw send: temp_up ...' means it
        # actually transmitted, and silence means it did not.
        client.subscribe([(self.topic_log, 0), (self.topic_sensor, 0)])
        print(f"[remote] connected to {self.args.broker}:{self.args.port}, "
              f"commanding {self.topic_ac}")

    def _on_message(self, _client, _userdata, msg):
        if msg.topic == self.topic_sensor:
            self.last_packet_at = time.time()
            return
        try:
            evt = json.loads(msg.payload)
        except json.JSONDecodeError:
            return
        for line in evt.get("lines") or []:
            print(f"  board| {line}")

    # -- commands ----------------------------------------------------------

    def send(self) -> None:
        packet = p.encode_ac_packet(self.args.dev, self.state["power"],
                                    "cool", self.state["temp"], "auto")
        self.client.publish(self.topic_ac, packet, qos=1)
        power = "ON " if self.state["power"] else "OFF"
        print(f"[remote] power={power} temp={self.state['temp']}  "
              f"({packet.hex()})")
        self._save_state()

    def power(self, on: bool) -> None:
        self.state["power"] = on
        if not on:
            # The board forgets its setpoint when it powers off (haveApplied
            # goes false), so the next power-on re-syncs from scratch. Match
            # that here rather than carrying a stale number across.
            self.state["temp"] = DEFAULT_TEMP
        self.send()

    def nudge(self, delta: int) -> None:
        if not self.state["power"]:
            print("[remote] the unit is off -- press 'o' first")
            return
        target = self.state["temp"] + delta
        if not TEMP_MIN <= target <= TEMP_MAX:
            print(f"[remote] {target}C is outside {TEMP_MIN}-{TEMP_MAX}, ignored")
            return
        self.state["temp"] = target
        self.send()

    def run_once(self, words: list[str]) -> int:
        actions = {"on": lambda: self.power(True),
                   "off": lambda: self.power(False),
                   "up": lambda: self.nudge(+1),
                   "down": lambda: self.nudge(-1)}
        unknown = [w for w in words if w not in actions]
        if unknown:
            print(f"unknown command(s): {', '.join(unknown)}")
            print("use: on | off | up | down")
            return 2
        for w in words:
            actions[w]()
            time.sleep(0.4)          # let the board finish its IR burst
        time.sleep(1.5)              # and let its console reply arrive
        return 0

    def run_interactive(self) -> int:
        print("\n  o = 전원 ON    f = 전원 OFF    + = 온도 올림    - = 온도 내림")
        print("  s = 상태        q = 종료\n")
        while True:
            try:
                key = input("remote> ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                print()
                return 0
            if key in ("q", "quit", "exit"):
                return 0
            if key in ("o", "on"):
                self.power(True)
            elif key in ("f", "off"):
                self.power(False)
            elif key in ("+", "up", "u"):
                self.nudge(+1)
            elif key in ("-", "down", "d"):
                self.nudge(-1)
            elif key in ("s", "status"):
                age = time.time() - self.last_packet_at
                alive = "online" if self.last_packet_at and age < 5 else "no packets"
                print(f"[remote] power={'ON' if self.state['power'] else 'OFF'} "
                      f"temp={self.state['temp']}  board: {alive}")
            elif key:
                print("keys: o f + - s q")
            time.sleep(0.3)          # give the board's log lines a moment

    def start(self) -> None:
        self.client.connect(self.args.broker, self.args.port, 30)
        self.client.loop_start()

    def stop(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()


def main() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    default_state = Path(__file__).resolve().parent.parent / ".mockdata" / "pc_remote_state.json"

    ap = argparse.ArgumentParser(
        description="Four-button AC remote for one board, over MQTT")
    ap.add_argument("command", nargs="*",
                    help="on | off | up | down; omit for an interactive prompt")
    ap.add_argument("--store-id", default="S001")
    ap.add_argument("--dev", type=int, default=1, help="target device id")
    ap.add_argument("--broker", default="127.0.0.1",
                    help="the store PC's broker; 127.0.0.1 when run on it")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--state-file", default=str(default_state),
                    help="where the last commanded setpoint is remembered")
    args = ap.parse_args()

    remote = Remote(args)
    try:
        remote.start()
    except OSError as exc:
        print(f"[remote] cannot reach the broker at {args.broker}:{args.port}: {exc}")
        raise SystemExit(1)
    time.sleep(0.6)                  # let on_connect land before the first send
    try:
        code = remote.run_once(args.command) if args.command \
            else remote.run_interactive()
    finally:
        remote.stop()
    raise SystemExit(code)


if __name__ == "__main__":
    main()
