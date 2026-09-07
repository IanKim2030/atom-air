"""Bring up the whole Atom Air stack with no hardware, in one command.

    python tools/run_mock.py

Starts the cloud, the gateway and fake Atom Lite devices, waits until each is
actually healthy, seeds firmware images so SOTA works out of the box, and
prints the URL to open. Ctrl+C stops everything it started.

If Mosquitto is listening the devices are driven over the real broker, which
exercises MQTT, the topics and the OTA download for real. Otherwise it falls
back to the gateway's built-in simulator so the stack still comes up.

    --reset        start from empty state (fresh database, unflashed devices)
    --devices N    how many Atom Lite devices to fake
    --fault-rate   per-second chance of a sensor fault, e.g. 0.01
    --no-mqtt      force the built-in simulator even if a broker is up
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
MOCK_DIR = REPO / ".mockdata"
GATEWAY_DIR = REPO / "gateway"
EXE = GATEWAY_DIR / ("atomair-gateway.exe" if os.name == "nt" else "atomair-gateway")
CLOUD_URL = "http://127.0.0.1:8000"

# Terminal colours, so three interleaved log streams stay readable.
COLOURS = {"cloud": "\033[36m", "gateway": "\033[32m", "atom": "\033[35m",
           "mock": "\033[33m"}
RESET = "\033[0m"


def say(source: str, message: str) -> None:
    colour = COLOURS.get(source, "")
    print(f"{colour}[{source:7}]{RESET} {message}", flush=True)


def port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    with socket.socket() as s:
        s.settimeout(timeout)
        return s.connect_ex((host, port)) == 0


def make_kill_on_close_job():
    """A Windows job object whose children die when this process does.

    Without it, force-killing the launcher (or just closing the terminal)
    orphans the cloud and the gateway, leaving port 8000 bound and the next
    run confused about what it is talking to.
    """
    if os.name != "nt":
        return None
    import ctypes
    from ctypes import wintypes

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [(n, ctypes.c_ulonglong) for n in (
            "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
            "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]

    class BASIC_LIMIT(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
            ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.POINTER(ctypes.c_ulong)),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD)]

    class EXTENDED_LIMIT(ctypes.Structure):
        _fields_ = [("BasicLimitInformation", BASIC_LIMIT),
                    ("IoInfo", IO_COUNTERS),
                    ("ProcessMemoryLimit", ctypes.c_size_t),
                    ("JobMemoryLimit", ctypes.c_size_t),
                    ("PeakProcessMemoryUsed", ctypes.c_size_t),
                    ("PeakJobMemoryUsed", ctypes.c_size_t)]

    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
    JobObjectExtendedLimitInformation = 9

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    job = k32.CreateJobObjectW(None, None)
    if not job:
        return None
    info = EXTENDED_LIMIT()
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if not k32.SetInformationJobObject(job, JobObjectExtendedLimitInformation,
                                       ctypes.byref(info), ctypes.sizeof(info)):
        return None
    return job


class Stack:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.procs: list[tuple[str, subprocess.Popen]] = []
        self.stopping = threading.Event()
        # Children join this job, so they cannot outlive the launcher.
        self.job = make_kill_on_close_job()

    # -- process plumbing --------------------------------------------------

    def spawn(self, source: str, cmd: list[str], cwd: Path) -> subprocess.Popen:
        env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
        proc = subprocess.Popen(
            cmd, cwd=str(cwd), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", bufsize=1)
        if self.job is not None:
            import ctypes
            handle = int(proc._handle)  # type: ignore[attr-defined]
            if not ctypes.WinDLL("kernel32").AssignProcessToJobObject(self.job, handle):
                say("mock", f"warning: could not put {source} in the cleanup job")
        self.procs.append((source, proc))
        threading.Thread(target=self._pump, args=(source, proc), daemon=True).start()
        return proc

    def _pump(self, source: str, proc: subprocess.Popen) -> None:
        for line in proc.stdout:
            if self.stopping.is_set():
                return
            say(source, line.rstrip())
        if not self.stopping.is_set() and proc.poll() not in (None, 0):
            say("mock", f"{source} exited with code {proc.returncode}")

    def stop_all(self) -> None:
        if self.stopping.is_set():
            return
        self.stopping.set()
        print()
        for source, proc in reversed(self.procs):
            if proc.poll() is None:
                say("mock", f"stopping {source}")
                proc.terminate()
        deadline = time.time() + 10
        for _, proc in self.procs:
            if proc.poll() is None:
                try:
                    proc.wait(timeout=max(0.1, deadline - time.time()))
                except subprocess.TimeoutExpired:
                    pass
        for _, proc in self.procs:
            if proc.poll() is None:
                proc.kill()
        say("mock", "all stopped")

    # -- startup steps -----------------------------------------------------

    def ensure_binary(self) -> bool:
        if not shutil.which("go"):
            if EXE.exists():
                return True
            say("mock", "Go is not installed and the gateway binary is missing")
            return False
        say("mock", "building the gateway")
        build = subprocess.run(["go", "build", "-o", EXE.name, "."],
                               cwd=str(GATEWAY_DIR), capture_output=True, text=True)
        if build.returncode != 0:
            say("mock", "build failed:\n" + (build.stderr or build.stdout))
            return False
        return True

    def start_cloud(self) -> bool:
        if port_open("127.0.0.1", 8000):
            say("mock", "a server is already listening on :8000, reusing it")
            return True
        self.spawn("cloud", [sys.executable, "-m", "uvicorn", "cloud.cloud_server:app",
                             "--host", "127.0.0.1", "--port", "8000"], REPO)
        for _ in range(60):
            if self.stopping.is_set():
                return False
            try:
                with urllib.request.urlopen(CLOUD_URL + "/healthz", timeout=1):
                    return True
            except (urllib.error.URLError, OSError):
                time.sleep(0.5)
        say("mock", "the cloud never became healthy")
        return False

    def seed_firmware(self) -> None:
        """Put a .bin behind every model in the catalog so SOTA works at once."""
        firmware = MOCK_DIR / "firmware"
        firmware.mkdir(parents=True, exist_ok=True)
        try:
            with urllib.request.urlopen(CLOUD_URL + "/api/v1/ac/models", timeout=5) as r:
                catalog = json.load(r)["catalog"]
        except Exception as exc:
            say("mock", f"could not read the AC catalog ({exc}); skipping firmware seed")
            return

        protocols = {m["protocol"] for brand in catalog for m in brand["models"]}
        # The universal raw-replay image must exist even before the first raw
        # model is registered, or a bare-device IR-data deploy has nothing to
        # flash first.
        protocols.add("RAW")
        made = 0
        for proto in sorted(protocols):
            path = firmware / f"atom_ac_{proto.lower()}.bin"
            if path.exists():
                continue
            path.write_bytes(f"ATOMAIR-MOCK-FW-{proto}\x00".encode() * 4500)
            made += 1
        say("mock", f"firmware images ready for {len(protocols)} protocols "
                    f"({made} newly written) in {firmware}")

    def start_gateway(self, use_mqtt: bool) -> None:
        cmd = [str(EXE), "--store-id", self.args.store_id,
               "--data-dir", str(MOCK_DIR),
               "--license-interval", "3600"]
        if not use_mqtt:
            cmd += ["--simulate", "--no-mqtt", "--devices", str(self.args.devices)]
        self.spawn("gateway", cmd, GATEWAY_DIR)

    def start_devices(self) -> None:
        cmd = [sys.executable, str(REPO / "tools" / "fake_atom.py"),
               "--store-id", self.args.store_id,
               "--devices", str(self.args.devices),
               "--first-dev", str(self.args.first_dev),
               "--state-file", str(MOCK_DIR / "fake_atom_state.json")]
        if self.args.fault_rate > 0:
            cmd += ["--fault-rate", str(self.args.fault_rate)]
        self.spawn("atom", cmd, REPO)

    def wait_for_gateway(self) -> bool:
        url = f"{CLOUD_URL}/api/v1/stores/{self.args.store_id}/status"
        for _ in range(40):
            if self.stopping.is_set():
                return False
            try:
                with urllib.request.urlopen(url, timeout=2) as r:
                    if json.load(r).get("gateway_online"):
                        return True
            except (urllib.error.URLError, OSError):
                pass
            time.sleep(0.5)
        return False

    # -- run ---------------------------------------------------------------

    def run(self) -> int:
        if self.args.reset and MOCK_DIR.exists():
            shutil.rmtree(MOCK_DIR, ignore_errors=True)
            say("mock", f"reset: cleared {MOCK_DIR}")
        MOCK_DIR.mkdir(parents=True, exist_ok=True)

        if not self.ensure_binary():
            return 1

        use_mqtt = not self.args.no_mqtt and port_open(self.args.broker, self.args.port)
        if use_mqtt:
            say("mock", f"Mosquitto found at {self.args.broker}:{self.args.port} "
                        f"-- driving devices over the real broker")
        else:
            reason = "forced by --no-mqtt" if self.args.no_mqtt else \
                     f"no broker on {self.args.broker}:{self.args.port}"
            say("mock", f"{reason} -- using the gateway's built-in simulator "
                        f"(MQTT and OTA download are not exercised)")

        if not self.start_cloud():
            self.stop_all()
            return 1
        self.seed_firmware()
        self.start_gateway(use_mqtt)
        if use_mqtt:
            time.sleep(1.5)
            self.start_devices()

        if not self.wait_for_gateway():
            say("mock", "the gateway never registered with the cloud")
            self.stop_all()
            return 1

        url = f"{CLOUD_URL}/?store_id={self.args.store_id}"
        print()
        say("mock", "=" * 58)
        say("mock", f"  open  {url}")
        say("mock", f"  mode  {'real MQTT + fake devices' if use_mqtt else 'built-in simulator'}")
        say("mock", f"  data  {MOCK_DIR}")
        say("mock", "  the 1-second stream starts when you open the page")
        say("mock", "  Ctrl+C to stop everything")
        # --reload looks like the answer and is not: with the gateway holding a
        # WebSocket open, uvicorn announces the reload and then never finishes
        # it, so the old code keeps serving. Better a reminder than a lie.
        say("mock", "  edited cloud/*.py? Ctrl+C and rerun -- templates are")
        say("mock", "  re-read per request, Python code is not")
        say("mock", "=" * 58)
        print()

        try:
            while not self.stopping.is_set():
                for source, proc in self.procs:
                    if proc.poll() not in (None, 0):
                        say("mock", f"{source} died; shutting down")
                        self.stop_all()
                        return 1
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        self.stop_all()
        return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Run the whole stack without hardware")
    ap.add_argument("--store-id", default="S001")
    ap.add_argument("--devices", type=int, default=2)
    ap.add_argument("--first-dev", type=int, default=1,
                    help="first fake dev_id -- raise it (e.g. 11) when a real "
                         "Atom Lite shares the broker, so ids never collide")
    ap.add_argument("--broker", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--fault-rate", type=float, default=0.0)
    ap.add_argument("--no-mqtt", action="store_true")
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    stack = Stack(args)
    handler = lambda *_: stack.stop_all()
    signal.signal(signal.SIGINT, handler)
    # Ctrl+Break, and the console-close signal, arrive as SIGBREAK on Windows.
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, handler)
    return stack.run()


if __name__ == "__main__":
    raise SystemExit(main())
