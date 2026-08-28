// Atom Air device configuration — edit before the first USB flash.
//
// Every value can also be overridden from platformio.ini build_flags
// (e.g. -DDEVICE_ID=2) without touching this file, which is how a technician
// flashes several units in a row.
#pragma once

// ── network ─────────────────────────────────────────────────────────────
// Left blank on purpose: real credentials must never land in git. Provision a
// unit over USB serial instead — `wifi <ssid> <password>` saves them to NVS —
// or pass -DWIFI_SSID=... -DWIFI_PASS=... when flashing a batch. A unit with
// no credentials anywhere parks on the serial console asking for them.
#ifndef WIFI_SSID
#define WIFI_SSID ""
#endif
#ifndef WIFI_PASS
#define WIFI_PASS ""
#endif

// The store management PC running Mosquitto + the gateway. Blank for the same
// reason as the Wi-Fi block — a LAN address is per-site, not per-repo. Set it
// over serial with `mqtt <host> [port]`, or with -DMQTT_HOST=... at build time.
#ifndef MQTT_HOST
#define MQTT_HOST ""
#endif
#ifndef MQTT_PORT
#define MQTT_PORT 1883
#endif

// ── identity ────────────────────────────────────────────────────────────
// Must match the gateway's --store-id and the cloud's store registration.
#ifndef STORE_ID
#define STORE_ID "S001"
#endif
// Unique per Atom Lite within the store: 1, 2, 3, ...
#ifndef DEVICE_ID
#define DEVICE_ID 1
#endif

// ── pins (M5Stack ATOM Lite) ────────────────────────────────────────────
#ifndef IR_TX_PIN
#define IR_TX_PIN 12        // built-in IR LED
#endif
// Optional IR receiver (VS1838B/TSOP38238: OUT->G25, VCC->3V3, GND->GND) for
// learning raw remote codes. Only devices used for 학습 need one attached;
// G25 is a free bottom-header pin (Grove 26/32 is taken by the sensor).
#ifndef IR_RX_PIN
#define IR_RX_PIN 25
#endif
#ifndef STATUS_LED_PIN
#define STATUS_LED_PIN 27   // built-in SK6812 RGB LED
#endif

// Grove port (the 4-pin connector): GND, 5V, G26, G32.
// I2C units (ENV III/IV) use SDA=26 / SCL=32; a bare DHT data line goes to G26.
#ifndef I2C_SDA_PIN
#define I2C_SDA_PIN 26
#endif
#ifndef I2C_SCL_PIN
#define I2C_SCL_PIN 32
#endif
#ifndef DHT_DATA_PIN
#define DHT_DATA_PIN 26
#endif
