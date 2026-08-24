# Running Atom Air without hardware

```bash
python tools/run_mock.py
```

That is the whole thing. It builds the gateway, starts the cloud, the gateway and
fake Atom Lite devices, waits until each is genuinely healthy, seeds firmware
images so SOTA works immediately, and prints the URL. Ctrl+C stops everything it
started.

```
[mock   ] Mosquitto found at 127.0.0.1:1883 -- driving devices over the real broker
[mock   ] firmware images ready for 7 protocols in C:\dev\atom-air\.mockdata\firmware
[gateway] MQTT connected  broker=127.0.0.1:1883
[mock   ] ==========================================================
[mock   ]   open  http://127.0.0.1:8000/?store_id=S001
[mock   ]   mode  real MQTT + fake devices
[mock   ]   the 1-second stream starts when you open the page
[mock   ]   Ctrl+C to stop everything
[mock   ] ==========================================================
```

State lives in `.mockdata/` at the repo root — separate from the real service's
`%ProgramData%\AtomAir`, so mock runs never touch a deployed gateway.

## Two levels of mock

| | what it exercises |
|---|---|
| **Mosquitto running** (default) | the real thing: MQTT topics, 8-byte control frames on the wire, and a genuine firmware download over HTTP |
| **no broker**, or `--no-mqtt` | the gateway's built-in simulator. Faster to start, but MQTT and the OTA download are skipped |

The launcher picks automatically by checking whether port 1883 is open. With
`--no-mqtt` the gateway contacts no broker at all, so a machine that happens to
have Mosquitto up will not mix synthetic frames with real device traffic.

## Options

| | |
|---|---|
| `--reset` | start from empty state: fresh database, unflashed devices |
| `--devices N` | how many Atom Lite devices to fake (default 2) |
| `--fault-rate 0.01` | per-second chance of a sensor fault, to exercise `FLAG_SENSOR_FAULT` |
| `--no-mqtt` | force the built-in simulator even if a broker is up |
| `--store-id` | which store to run as (default `S001`) |

## The fake device

[`fake_atom.py`](fake_atom.py) is the executable specification of the firmware
contract, and can also be run on its own against an already-running gateway:

```bash
python tools/fake_atom.py --store-id S001 --devices 2
```

| | |
|---|---|
| publishes | `atom/{store}/sensor` — 12-byte `SensorPacket`, 1 Hz |
| subscribes | `atom/{store}/ac/{dev_id}` — 8-byte AC control frame |
| subscribes | `atom/{store}/ota/{dev_id}` — JSON OTA command |

It behaves like real hardware in the ways that matter:

- an AC frame changes the setpoint, so the reported temperature really drifts
  toward it — you can watch the chart bend after pressing a button;
- an OTA command makes it **download the firmware over HTTP** and only then raise
  `FLAG_IR_READY`, which is what lets the gateway's SOTA `verify` stage succeed
  for real rather than by simulation;
- **flashed firmware survives a restart**, recorded in
  `.mockdata/fake_atom_state.json`, so a demo does not have to re-run SOTA every
  time. `--reset` clears it;
- illuminance follows the clock (dim overnight, bright at midday) and the sensor
  can fault on demand.

## Trying the whole flow

1. `python tools/run_mock.py --reset`
2. Open the printed URL. The 1-second stream starts **because you opened it** —
   close the tab and the gateway drops back to sending 1-minute statistics only.
3. Press the AC power button and change the setpoint. Watch the temperature line
   bend toward it, and the device log the decoded 8-byte frame.
4. Open **에어컨 모델 선택 · 펌웨어 배포**, pick a model, deploy. The device
   downloads the image from the gateway's OTA server and comes back IR-capable;
   the gateway stops warning that the device has no IR firmware.
5. Ctrl+C.
