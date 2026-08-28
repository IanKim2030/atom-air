# Atom Air — ATOM Lite firmware (real hardware)

PlatformIO project for the M5Stack ATOM Lite. Implements the MQTT contract whose
executable specification is [`tools/fake_atom.py`](../tools/fake_atom.py):

| | topic | payload |
|---|---|---|
| publishes | `atom/{store}/sensor` | 12-byte `SensorPacket`, 1 Hz |
| publishes | `atom/{store}/ir/{dev_id}` | JSON IR events: learn capture, IRDATA ack |
| subscribes | `atom/{store}/ac/{dev_id}` | 8-byte AC control frame → IR burst |
| subscribes | `atom/{store}/ota/{dev_id}` | `{"cmd":"OTA","url","protocol","model","size"}` or `{"cmd":"IRDATA","url","model_id","size","slots"}` |
| subscribes | `atom/{store}/learn/{dev_id}` | `{"cmd":"LEARN","session_id","slot","timeout_s"}` / `{"cmd":"LEARN_CANCEL"}` |

Two build targets mirror the SOTA pipeline:

| env | contents | how it gets onto the device |
|---|---|---|
| `atom_base` | Wi-Fi + MQTT + sensor loop + HTTP OTA client | USB, once, at install time |
| `atom_ac` | base + IRremoteESP8266 (`IRac`) | pushed by the gateway over the store LAN when an AC model is picked in the web UI |

The `atom_ac` image is **universal**: `IRac` speaks every protocol in the cloud's
model catalog, and the device learns *which* one from the OTA command — the
protocol name is stamped into NVS **before** flashing, so the freshly booted AC
image reads it and raises `FLAG_IR_READY`. That flag in the sensor packet is
what makes the gateway's SOTA `verify` stage pass.

## Hardware

- **IR transmit**: built-in IR LED on GPIO 12 — no wiring needed. It is
  low-power; mount the unit close to (or facing) the AC's receiver.
- **Status LED** (built-in RGB): white=booting · red=no Wi-Fi ·
  yellow=no MQTT · green=running · blue=OTA flashing.
- **Temp/humidity sensor — auto-detected at boot** on the Grove port
  (pins: GND, 5V, G26, G32):

  | sensor | connection | detection |
  |---|---|---|
  | M5 ENV III unit (SHT30) | Grove cable, plug in | I2C 0x44, SHT3x command set |
  | M5 ENV IV unit (SHT40) | Grove cable, plug in | I2C 0x44, SHT4x command set |
  | DHT22 / DHT11 | data → **G26**, VCC → 5V, GND → GND | probed after I2C, DHT22 first |

  Detection runs once at boot — plug the sensor in, then reset the unit.
  The serial monitor prints `[boot] sensor: ...` with what was found. With
  nothing attached, packets carry `FLAG_SENSOR_FAULT` and zeroed readings
  (the dashboard shows "센서 점검"); AC control keeps working regardless.

## First flash (USB)

1. Edit [`include/config.h`](include/config.h): the store PC's LAN IP
   (`MQTT_HOST`), `STORE_ID`, and a unique `DEVICE_ID` per unit. For flashing
   several units, override per run instead:
   `PLATFORMIO_BUILD_FLAGS="-DDEVICE_ID=2" pio run -e atom_base -t upload`
2. Plug the ATOM Lite in and:

```bash
cd firmware
pio run -e atom_base -t upload
pio device monitor          # watch it join Wi-Fi and MQTT
```

3. Give it Wi-Fi over the serial monitor (see below). `WIFI_SSID`/`WIFI_PASS`
   ship blank on purpose, so a fresh unit prints
   `[wifi] not provisioned -- run: wifi <ssid> <password>` every 5 s until you do.

The device shows up on the dashboard as soon as its first packet reaches the
gateway (auto-registration).

## Wi-Fi provisioning (serial)

Credentials live in NVS, so re-pointing a unit at a different network needs a
USB cable — not a rebuild or a reflash. Type these into `pio device monitor`:

| command | effect |
|---|---|
| `wifi <ssid> <password>` | save to NVS and reboot. Use `"quotes"` for values with spaces |
| `wifi?` | print SSID, where it came from (NVS / config.h), link status and IP |
| `wifi reset` | drop the NVS credentials and reboot onto the `config.h` defaults |

NVS wins over `config.h` whenever it holds an SSID. The connect loop keeps
polling serial, so a unit stuck on a wrong password still accepts a corrected
`wifi ...` command instead of needing a reflash.

Never commit real credentials to `config.h`. For a batch flash, pass them as
build flags instead:
`PLATFORMIO_BUILD_FLAGS='-DWIFI_SSID=\"StoreNet\" -DWIFI_PASS=\"secret\"' pio run -e atom_base -t upload`

## Stage the AC image for SOTA

The gateway serves `<data-dir>\firmware\atom_ac_<protocol>.bin` over HTTP :8080.
Build once, stage under every protocol name:

```bash
pio run -e atom_ac
python stage_firmware.py                       # -> %ProgramData%\AtomAir\firmware
python stage_firmware.py --data-dir <dir>      # if the gateway runs with --data-dir
```

Then pick the AC brand/model in the web UI (store dashboard or admin console →
장비 업그레이드). The pipeline is: gateway MQTT OTA command → device saves the
protocol to NVS → downloads the image over the LAN → flashes → reboots →
reports `FLAG_IR_READY` → AC control frames start producing real IR bursts.

Re-running SOTA with a different model re-stamps the protocol and re-flashes —
that is how a unit is repointed at a different AC.

## Raw IR: 학습 리모컨 (RAW protocol)

For ACs `IRac` does not cover, the admin console can register a **raw** model
and learn its remote's signals. The same universal `atom_ac` image handles it:

- The NVS protocol sentinel `RAW` selects the replay path. The learned code
  bundle arrives as `{"cmd":"IRDATA"}` on the OTA topic: the device downloads
  `ir_<model_id>.json` from the gateway's :8080 server, streams it into SPIFFS
  (`/irdata.json`), validates the header, then stamps NVS and acks — no reboot.
- An AC frame is mapped to a slot key (`off`, `cool_18..30`, `heat_18..30`;
  fan is fixed to whatever the remote sent when learned) and that slot's
  mark/space array is replayed with `sendRaw()`. Unlearned combos are skipped.
- **Learning** needs an IR receiver (VS1838B/TSOP38238: OUT→**G33**, VCC→3V3,
  GND→GND) on the unit used for capture — transmit-only units need nothing.
  A `LEARN` command arms the receiver (LED purple); the next decoded frame is
  published up as raw timings and the receiver disarms.
- `stage_firmware.py` also stages `atom_ac_raw.bin` so a bare device gets the
  universal image flashed automatically before its first IRDATA push.

## Notes

- `CARRIER_AC` in the catalog is not currently supported by `IRac`'s common
  interface; the device logs and drops commands for it rather than sending a
  wrong burst. The other catalog protocols (Samsung, LG/LG2, Daikin/Daikin216,
  Mitsubishi) all work. Unsupported units (Carrier included) can instead be
  registered as a **raw** model and learned from the real remote (see above).
- The AC frame's checksum, header (`0x55`) and tail (`0xEE`) are verified
  on-device; malformed frames are rejected, matching `common/protocol.py`.
- MQTT keepalive and Wi-Fi reconnect are automatic; the status LED tells a
  technician what is wrong from across the room.
