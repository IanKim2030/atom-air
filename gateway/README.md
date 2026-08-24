# Atom Air — store gateway (Go)

A single static `.exe` that runs on the Store Management PC as a Windows service.

```
Atom Lite <--MQTT (Mosquitto)--> atomair-gateway <--WebSocket--> Cloud
                                        |
                                 SQLite WAL + HTTP OTA (:8080)
```

## Why Go here

The gateway is deployed to many uncontrolled Windows machines by technicians, so
deployment cost dominates. One static binary means no Python runtime, no pip, no
venv, no pywin32, and no dependence on which `python.exe` a service account
resolves. `golang.org/x/sys/windows/svc` gives first-class service control, and
`modernc.org/sqlite` is pure Go — no cgo, so it cross-compiles from anywhere.

The cloud stays Python/FastAPI: it is one deployment you control, and there is
no comparable win in rewriting it.

## Build

```bash
cd gateway
go build -ldflags "-s -w" -o atomair-gateway.exe .

# from a non-Windows build host
GOOS=windows GOARCH=amd64 go build -ldflags "-s -w" -o atomair-gateway.exe .
```

No C toolchain required.

## Run in the foreground (development)

```bash
./atomair-gateway.exe --store-id S001 --simulate --devices 2
```

`--simulate` synthesises Atom Lite traffic through the exact same ingest path
the MQTT bridge uses, so everything downstream is exercised for real without
hardware or a broker.

## Testing against a real broker, without hardware

`--simulate` bypasses MQTT entirely. To exercise the real path — Mosquitto, the
topics, the 8-byte control frames and a genuine OTA download — run the gateway
**without** `--simulate` and put a fake device on the broker:

```bash
# Mosquitto listening on 1883, then:
./atomair-gateway.exe --store-id S001            # no --simulate
python ../tools/fake_atom.py --store-id S001 --devices 2
```

Or let [`tools/run_mock.py`](../tools/README.md) start the cloud, the gateway and
the devices together in one command.

[`tools/fake_atom.py`](../tools/fake_atom.py) is the executable specification of
what the ESP32 firmware must do:

| | |
|---|---|
| publishes | `atom/{store}/sensor` — 12-byte `SensorPacket`, 1 Hz |
| subscribes | `atom/{store}/ac/{dev_id}` — 8-byte AC control frame |
| subscribes | `atom/{store}/ota/{dev_id}` — JSON OTA command |

It applies the AC setpoint (so the reported temperature really drifts toward it),
and on an OTA command it **downloads the firmware over HTTP and then raises
`FLAG_IR_READY`** — which is what makes the gateway's SOTA `verify` stage pass
for real rather than by simulation. Put a `.bin` in
`<data-dir>\firmware\atom_ac_<protocol>.bin` first; without `--simulate` the
gateway fails the deploy loudly rather than inventing a stub.

## Install as a Windows service

From an **Administrator** prompt:

```
atomair-gateway.exe install --store-id S001 ^
    --cloud-ws wss://cloud.example.com ^
    --cloud-http https://cloud.example.com ^
    --token <your-gateway-token> ^
    --mqtt-host 127.0.0.1

atomair-gateway.exe start
atomair-gateway.exe status        # works without elevation
```

Flags given to `install` are baked into the service command line. `uninstall`,
`stop` and `restart` do what they say.

The installer:

- registers `AtomAirGateway`, **automatic (delayed)** start, depending on `Tcpip`
  so the network stack is up first;
- sets recovery actions — restart after 5s, 15s, then every 60s, counter reset daily;
- adds an inbound firewall rule for the OTA port on the private and domain profiles;
- validates every flag *before* touching the service manager, so a typo cannot
  leave a service installed that will not start.

### Where the service keeps its state

A service runs as LocalSystem with its working directory in `system32`, and
Program Files is not writable, so nothing is stored next to the binary:

| | path |
|---|---|
| data dir | `%ProgramData%\AtomAir` (override with `--data-dir`) |
| database | `<data-dir>\store_data.db` (WAL) |
| licence cache | `<data-dir>\store_license_config.json` |
| firmware | `<data-dir>\firmware\` |
| logs | `<data-dir>\logs\gateway.log`, rotated at 20 MB, 5 kept, gzipped |

A service has no console, so **file logging is on by default**. In the
foreground the same lines also go to stderr. `--log-file -` disables the file.

## Operating modes

| | behaviour |
|---|---|
| normal | only 1-minute statistics are pushed to the cloud |
| live | entered on `START_LIVE_STREAM`: every 1-second `SensorPacket` is bypassed to the cloud raw, in real time |

`STOP_LIVE_STREAM`, or losing the socket, drops straight back to normal. Local
persistence, downsampling, retention and the licence check are separate
goroutines and never pause for either mode.

## Concurrency

| goroutine | job | gated on the stream? |
|---|---|---|
| ingest | decode frames → buffer, and bypass when live | no |
| persist | flush the 1-second buffer to SQLite each second | **never** |
| minute | downsample, queue for upload | **never** |
| retention | purge raw rows older than a year | **never** |
| license | daily `POST /api/v1/store/authorize` + grace check | **never** |
| cloud | WebSocket link, reconnect with backoff | — |
| heartbeat | log a status line each minute | — |

Undeliverable minute statistics stay in the SQLite outbox and are flushed on the
next successful connect, so a cloud outage loses nothing.

## Key flags

| flag | default | |
|---|---|---|
| `--store-id` | `S001` | |
| `--cloud-ws` / `--cloud-http` | `127.0.0.1:8000` | |
| `--token` | `dev-gateway-token` | **change before deploying** |
| `--mqtt-host` / `--mqtt-port` | `127.0.0.1:1883` | Mosquitto |
| `--data-dir` | `%ProgramData%\AtomAir` | |
| `--ota-host` | `auto` | detects the LAN IP the Atom devices must reach |
| `--ota-port` | `8080` | |
| `--license-interval` | `86400` | seconds between authorize calls |
| `--simulate` / `--devices` | off / 2 | run without hardware |

Each has an `ATOM_*` environment-variable equivalent; see `--help`.

## Protocol

`protocol/` implements the 12-byte sensor and 8-byte AC frames. The Python cloud
carries the same formats in `common/protocol.py`, and **both assert against the
shared vectors in `common/protocol_vectors.json`**, so the two implementations
cannot drift:

```bash
cd gateway && go test ./...                 # Go side
python -m unittest discover -s common       # Python side
```
