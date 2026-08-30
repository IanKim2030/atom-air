// Atom Air — M5Stack ATOM Lite firmware.
//
// Speaks the store MQTT contract whose executable specification is
// tools/fake_atom.py, against the wire formats in common/protocol.py:
//
//   publishes  atom/{store}/sensor          12-byte SensorPacket, 1 Hz
//   publishes  atom/{store}/ir/{dev_id}     JSON IR events (capture, IRDATA ack)
//   publishes  atom/{store}/log/{dev_id}    JSON console mirror (web 디버깅 panel)
//   subscribes atom/{store}/ac/{dev_id}     8-byte AC control frame -> IR
//   subscribes atom/{store}/ota/{dev_id}    JSON {"cmd":"OTA"|"IRDATA","url",...}
//   subscribes atom/{store}/learn/{dev_id}  JSON {"cmd":"LEARN","slot",...}
//
// Built two ways (see platformio.ini): atom_base has no IR library and never
// raises FLAG_IR_READY; atom_ac adds IRremoteESP8266's IRsend and IRrecv, and
// reports ready once a learned bundle is actually sitting in SPIFFS.
//
// IR control is manufacturer-agnostic: everything comes from the customer's own
// remote. The learned bundle arrives as a JSON data file over the same OTA HTTP
// server (cmd IRDATA), lives in SPIFFS, and maps each button slot ("power_on",
// "temp_up", ...) to what was captured for it. Learning: a LEARN command arms
// the receiver on IR_RX_PIN; the next frame goes up atom/{store}/ir/{dev}.
//
// Two ways to send one slot, tried in that order. If the decoder recognised the
// remote, the frame is regenerated from protocol + value (or state bytes), so a
// slightly noisy capture still transmits to spec. Otherwise the recorded
// mark/space timings are replayed, which works for remotes no decoder knows --
// most air conditioners -- at the cost of reproducing the capture as-is. There
// is still no brand database: the protocol, when there is one, was learned from
// the remote rather than looked up.
//
// The temperature/humidity sensor on the Grove port is auto-detected at boot:
// M5 ENV III (SHT30) and ENV IV (SHT40) over I2C, or a bare DHT22/DHT11 on
// G26. With nothing attached, every packet carries FLAG_SENSOR_FAULT and
// zeroed readings — the honest wire representation of "this unit cannot read
// its environment". Plug a sensor in and reset; detection runs once at boot.

#include <Arduino.h>
#include <WiFi.h>
#include <Wire.h>
#include <HTTPClient.h>
#include <Update.h>
#include <Preferences.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <FastLED.h>
#include <Adafruit_SHT31.h>
#include <Adafruit_SHT4x.h>
#include <DHT.h>

#include "config.h"

#ifdef HAS_IR
#include <SPIFFS.h>
#include <IRremoteESP8266.h>   // decode_type_t, kStateSizeMax
#include <IRrecv.h>
#include <IRsend.h>
#include <IRutils.h>
#endif

// ── wire protocol (mirrors common/protocol.py) ──────────────────────────
static const uint8_t SENSOR_HEADER = 0xAA;
static const uint8_t AC_HEADER = 0x55;
static const uint8_t AC_TAIL = 0xEE;
static const size_t SENSOR_SIZE = 12;
static const size_t AC_SIZE = 8;

static const uint8_t FLAG_AC_ON = 0x01;
static const uint8_t FLAG_IR_READY = 0x02;
static const uint8_t FLAG_SENSOR_FAULT = 0x04;

// ── state ───────────────────────────────────────────────────────────────
static WiFiClient wifiClient;
static PubSubClient mqtt(wifiClient);
static Preferences prefs;
static CRGB statusLed[1];

static char topicSensor[64];
static char topicAC[64];
static char topicOTA[64];
static char topicLearn[64];
static char topicIR[64];
static char topicLog[64];

// -- serial tee: board output mirrored to the cloud ---------------------
// Every line the firmware prints goes two places: the USB console, as always,
// and a small ring the loop drains onto atom/{store}/log/{dev}. That is what
// lets the dashboard's 디버깅 panel show what a developer sees on a monitor,
// without anyone walking to the unit with a cable. The ring is what makes the
// boot banner survive -- those lines are printed long before MQTT is up.
static const uint8_t LOG_RING = 64;
static const uint8_t LOG_LINE_MAX = 160;

class SerialTee : public Print {
 public:
  size_t write(uint8_t c) override { Serial.write(c); capture(c); return 1; }
  size_t write(const uint8_t *buf, size_t n) override {
    Serial.write(buf, n);
    for (size_t i = 0; i < n; i++) capture(buf[i]);
    return n;
  }
  bool pending() const { return head != tail; }
  String pop() {
    if (head == tail) return String();
    String out = ring[tail];
    tail = (tail + 1) % LOG_RING;
    return out;
  }

 private:
  void capture(uint8_t c) {
    if (c == '\r') return;
    if (c == '\n') { commit(); return; }
    if (partial.length() < LOG_LINE_MAX) partial += (char)c;
  }
  void commit() {
    if (partial.isEmpty()) return;
    ring[head] = partial;
    partial = "";
    head = (head + 1) % LOG_RING;
    if (head == tail) tail = (tail + 1) % LOG_RING;   // oldest line falls off
  }
  String ring[LOG_RING];
  String partial;
  uint8_t head = 0, tail = 0;
};
static SerialTee LOG;

static uint16_t seq = 0;
static bool acOn = false;

// Filled from NVS at boot; written by the OTA handler just before flashing.
static String acModel;
static String acModelId;
static bool irReady = false;

// WiFi credentials: NVS wins when set (serial-provisioned), else config.h.
static String wifiSsid;
static String wifiPass;
static bool wifiFromNvs = false;

// Gateway (MQTT broker) address, same rule. mqttHost is handed to PubSubClient
// as a bare pointer, so it must not be reassigned after setup() — every
// provisioning command reboots instead of editing it in place.
static String mqttHost;
static uint16_t mqttPort = MQTT_PORT;
static bool mqttFromNvs = false;

// The MQTT callback only records the OTA request; the download and flash run
// from loop(), never from inside the network stack's callback.
static volatile bool otaPending = false;
static String otaUrl, otaModel;
static long otaSize = -1;

#ifdef HAS_IR
static IRsend irsend(IR_TX_PIN);            // learned-code replay (TX)
static IRrecv irrecv(IR_RX_PIN, 1024, 50, true);  // learn capture (AC frames are long)

// IRDATA download request, recorded by the callback, run from loop().
static volatile bool irdataPending = false;
static String irdataUrl, irdataModelId, irdataModel;
static long irdataSize = -1;

// Learn session, armed by a LEARN command.
static bool learnActive = false;
static uint32_t learnDeadline = 0;
// IR monitor: the receiver armed with nowhere to store what it hears. Every
// frame is printed and thrown away, which is what you want when the question is
// "does this remote reach the unit, and what does it actually send?"
static bool monitorActive = false;
static uint32_t monitorDeadline = 0;
static uint16_t monitorFrames = 0;
static String learnSessionId, learnSlot;

static const char IRDATA_PATH[] = "/irdata.json";
#endif

// ── status LED ──────────────────────────────────────────────────────────
// white=booting  red=no wifi  yellow=no mqtt  green=ok  blue=OTA flashing
static void led(const CRGB &c) {
  statusLed[0] = c;
  FastLED.show();
}

// Remote-command feedback: one clear blink, then settle back to green-ok.
// Brightness is raised for the blink so the hues are easy to tell apart on
// the tiny SK6812. Runs inside the MQTT callback; ~400ms of blocking is fine
// at a 1 Hz publish.
static void blinkAck(const CRGB &c) {
  FastLED.setBrightness(90);
  led(c);
  delay(320);
  led(CRGB::Black);
  delay(80);
  FastLED.setBrightness(24);
  led(CRGB::Green);
}

// ── sensors ─────────────────────────────────────────────────────────────
// One Grove port, four supported sensors, detected once at boot:
//   SHT3x (M5 ENV III) and SHT4x (M5 ENV IV) share I2C address 0x44 — probe
//   the address, then try each command set. No I2C device -> try DHT22, then
//   DHT11 on the bare data pin (same wire protocol, different encoding, so a
//   plausibility check on the decoded value tells them apart).
enum SensorKind { SENSOR_NONE, SENSOR_SHT3X, SENSOR_SHT4X,
                  SENSOR_DHT22, SENSOR_DHT11 };
static SensorKind sensorKind = SENSOR_NONE;
static Adafruit_SHT31 sht3x;
static Adafruit_SHT4x sht4x;
static DHT dht22(DHT_DATA_PIN, DHT22);
static DHT dht11(DHT_DATA_PIN, DHT11);

static const char *sensorName() {
  switch (sensorKind) {
    case SENSOR_SHT3X: return "SHT3x (ENV III)";
    case SENSOR_SHT4X: return "SHT4x (ENV IV)";
    case SENSOR_DHT22: return "DHT22";
    case SENSOR_DHT11: return "DHT11";
    default: return "none";
  }
}

static bool plausible(float t) { return !isnan(t) && t > -40.0f && t < 85.0f; }

static void detectSensor() {
  Wire.begin(I2C_SDA_PIN, I2C_SCL_PIN);
  Wire.beginTransmission(0x44);
  if (Wire.endTransmission() == 0) {
    if (sht3x.begin(0x44) && plausible(sht3x.readTemperature())) {
      sensorKind = SENSOR_SHT3X;
      return;
    }
    if (sht4x.begin(&Wire)) {
      sensors_event_t h, t;
      if (sht4x.getEvent(&h, &t) && plausible(t.temperature)) {
        sensorKind = SENSOR_SHT4X;
        return;
      }
    }
  }

  // DHT sensors need a beat after power-up before the first read sticks.
  dht22.begin();
  delay(1500);
  for (int i = 0; i < 3; i++) {
    if (plausible(dht22.readTemperature())) { sensorKind = SENSOR_DHT22; return; }
    delay(600);
  }
  dht11.begin();
  for (int i = 0; i < 3; i++) {
    if (plausible(dht11.readTemperature())) { sensorKind = SENSOR_DHT11; return; }
    delay(600);
  }
  sensorKind = SENSOR_NONE;
}

// Fills temp/hum in engineering units; false -> FLAG_SENSOR_FAULT goes out.
// No supported unit measures illuminance, so light stays 0.
static bool readSensors(float &temp, float &hum, uint16_t &light) {
  light = 0;
  switch (sensorKind) {
    case SENSOR_SHT3X: {
      float t = sht3x.readTemperature(), h = sht3x.readHumidity();
      if (!plausible(t) || isnan(h)) return false;
      temp = t; hum = h;
      return true;
    }
    case SENSOR_SHT4X: {
      sensors_event_t h, t;
      if (!sht4x.getEvent(&h, &t) || !plausible(t.temperature)) return false;
      temp = t.temperature; hum = h.relative_humidity;
      return true;
    }
    case SENSOR_DHT22:
    case SENSOR_DHT11: {
      // The DHT library rate-limits internally (cached value within 2s), so
      // calling at 1 Hz is safe — readings refresh every other packet.
      DHT &dht = (sensorKind == SENSOR_DHT22) ? dht22 : dht11;
      float t = dht.readTemperature(), h = dht.readHumidity();
      if (!plausible(t) || isnan(h)) return false;
      temp = t; hum = h;
      return true;
    }
    default:
      return false;
  }
}

// ── packet building / parsing ───────────────────────────────────────────
static uint8_t xorChecksum(const uint8_t *data, size_t len) {
  uint8_t chk = 0;
  for (size_t i = 0; i < len; i++) chk ^= data[i];
  return chk;
}

static void buildSensorPacket(uint8_t out[SENSOR_SIZE]) {
  float temp = 0.0f, hum = 0.0f;
  uint16_t light = 0;
  uint8_t flags = 0;

  if (!readSensors(temp, hum, light)) {
    flags |= FLAG_SENSOR_FAULT;
    temp = 0.0f; hum = 0.0f; light = 0;
  }
  if (acOn) flags |= FLAG_AC_ON;
  if (irReady) flags |= FLAG_IR_READY;

  seq++;
  int16_t t100 = (int16_t)constrain(lroundf(temp * 100), -32768L, 32767L);
  uint16_t h100 = (uint16_t)constrain(lroundf(hum * 100), 0L, 65535L);

  out[0] = SENSOR_HEADER;
  out[1] = DEVICE_ID;
  out[2] = seq & 0xFF;         out[3] = seq >> 8;          // little endian
  out[4] = t100 & 0xFF;        out[5] = (uint16_t)t100 >> 8;
  out[6] = h100 & 0xFF;        out[7] = h100 >> 8;
  out[8] = light & 0xFF;       out[9] = light >> 8;
  out[10] = flags;
  out[11] = xorChecksum(out, 11);
}

// ── AC control ──────────────────────────────────────────────────────────
// There is no brand/protocol path any more: every device replays the timings
// captured from the customer's own remote. One code path, and a remote nobody
// has written a protocol driver for works exactly as well as a common one.
#ifdef HAS_IR
// ── raw replay (학습 리모컨) ────────────────────────────────────────────
// One learned code per remote *button*. A command carries the state the user
// wants, and this walks there from the state last applied: power, then mode,
// then the temperature and fan steps needed to close the gap.
//
// That only holds for remotes that send a discrete command per button. A
// remote that transmits its whole state on every press will re-send whatever
// was captured instead of stepping -- visible the first time it is tested.
static uint8_t appliedMode = 0, appliedTemp = 24, appliedFan = 0;
static bool haveApplied = false;

static int hexNibble(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

// "0x1A2B" or "1A2B" -> bytes. Returns how many were written.
static uint16_t hexToBytes(const char *s, uint8_t *out, uint16_t maxBytes) {
  if (s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) s += 2;
  uint16_t n = 0;
  while (s[0] && s[1] && n < maxBytes) {
    int hi = hexNibble(s[0]), lo = hexNibble(s[1]);
    if (hi < 0 || lo < 0) break;
    out[n++] = (uint8_t)((hi << 4) | lo);
    s += 2;
  }
  return n;
}

// Replays one slot from SPIFFS. The ArduinoJson filter keeps memory at ~one
// code even though the file holds all nine.
//
// Two ways to send, in order of preference. If the decoder recognised the
// remote at learn time, regenerate the frame from protocol + value/state: the
// library builds it to spec, so a capture with a little noise in it still
// transmits cleanly. Otherwise replay the timings we recorded, which works for
// remotes no decoder knows — most air conditioners — at the cost of echoing
// whatever imperfections the capture had.
//
// v1 bundles stored each slot as a bare timing array; those still load, and
// take the raw path because there is nothing else in them.
static bool sendSlot(const char *slot) {
  File f = SPIFFS.open(IRDATA_PATH, FILE_READ);
  if (!f) {
    LOG.println("[ir] no /irdata.json in SPIFFS — command ignored");
    return false;
  }
  JsonDocument filter;
  filter["freq_khz"] = true;
  filter["slots"][slot] = true;
  JsonDocument doc;
  DeserializationError err =
      deserializeJson(doc, f, DeserializationOption::Filter(filter));
  f.close();
  if (err != DeserializationError::Ok) {
    LOG.printf("[ir] bundle parse failed: %s\n", err.c_str());
    return false;
  }
  JsonVariant entry = doc["slots"][slot];
  if (entry.isNull()) {
    LOG.printf("[ir] %s not learned — skipped\n", slot);
    return false;
  }
  const bool v1 = entry.is<JsonArray>();
  JsonArray arr = v1 ? entry.as<JsonArray>() : entry["raw"].as<JsonArray>();
  const char *pname = v1 ? "" : (entry["p"] | "");

  decode_type_t type = pname[0] ? strToDecodeType(pname) : UNKNOWN;
  if (type != UNKNOWN) {
    bool sent = false;
    const char *hex = entry["v"] | "";
    if (hasACState(type)) {
      // Long frames live in a byte array; "v" holds them as hex.
      static uint8_t state[kStateSizeMax];
      uint16_t nbytes = hexToBytes(hex, state, sizeof(state));
      if (nbytes) sent = irsend.send(type, state, nbytes);
    } else {
      uint16_t bits = entry["b"] | 0;
      if (bits && hex[0])
        sent = irsend.send(type, (uint64_t)strtoull(hex, nullptr, 16), bits);
    }
    if (sent) {
      LOG.printf("[ir] decoded send: %s as %s\n", slot, pname);
      delay(250);   // remotes need a gap or the AC drops the second frame
      return true;
    }
    LOG.printf("[ir] %s: %s not sendable — falling back to raw\n", slot, pname);
  }

  if (arr.isNull() || arr.size() < 20 || arr.size() > 1024) {
    LOG.printf("[ir] %s: no usable raw timings — skipped\n", slot);
    return false;
  }
  static uint16_t rawBuf[1024];
  uint16_t n = 0;
  for (JsonVariant v : arr) rawBuf[n++] = v.as<uint16_t>();
  uint16_t freq = doc["freq_khz"] | 38;
  irsend.sendRaw(rawBuf, n, freq);
  LOG.printf("[ir] raw send: %s (%u entries @ %ukHz)\n", slot, n, freq);
  delay(250);
  return true;
}

static const char *modeSlot(uint8_t mode) {
  switch (mode) {                       // AC_MODES in common/protocol.py
    case 0: return "mode_cool";
    case 1: return "mode_heat";
    case 2: return "mode_dry";
    default: return nullptr;            // fan/auto have no learned button
  }
}

// Steps one axis with its up/down button. Capped so a wrong remembered state
// cannot turn one command into a hundred IR bursts.
static int stepTo(int from, int to, const char *up, const char *down) {
  int steps = to - from;
  const char *slot = steps > 0 ? up : down;
  int n = steps > 0 ? steps : -steps;
  if (n > 12) n = 12;
  for (int i = 0; i < n; i++)
    if (!sendSlot(slot)) return i;      // unlearned button: stop, do not spin
  return n;
}

static bool sendRawSlot(uint8_t power, uint8_t mode, uint8_t temp, uint8_t fan) {
  if (!power) {
    if (!sendSlot("power_off")) return false;
    haveApplied = false;                // state is unknown again once it is off
    return true;
  }
  bool sent = false;
  if (!haveApplied) {
    sent |= sendSlot("power_on");
    // Nothing is known about where the AC actually sits, so drive both axes
    // from one end: bottom out, then step up to the target.
    appliedTemp = 18;
    appliedFan = 0;
    stepTo(30, 18, "temp_up", "temp_down");
    stepTo(3, 0, "fan_up", "fan_down");
    appliedMode = 255;                  // force the mode button below
  }
  const char *ms = modeSlot(mode);
  if (ms != nullptr && mode != appliedMode) {
    sent |= sendSlot(ms);
    appliedMode = mode;
  }
  int t = constrain((int)temp, 16, 30);
  if (stepTo(appliedTemp, t, "temp_up", "temp_down") > 0) sent = true;
  appliedTemp = t;
  int fs = constrain((int)fan, 0, 3);
  if (stepTo(appliedFan, fs, "fan_up", "fan_down") > 0) sent = true;
  appliedFan = fs;
  haveApplied = true;
  return sent;
}

// Prints a captured frame as the numbers themselves, not just a count. Twelve
// per line because a log line is capped at 160 chars and because mark/space
// pairs read best in even columns -- header burst first, then the bit stream.
static void dumpRawTimings(const char *tag, const uint16_t *raw, uint16_t len) {
  uint32_t total = 0;
  for (uint16_t i = 0; i < len; i++) total += raw[i];
  LOG.printf("[%s] %u timings, %lu us total:\n", tag, len, (unsigned long)total);
  char line[160];
  size_t at = 0;
  for (uint16_t i = 0; i < len; i++) {
    at += snprintf(line + at, sizeof(line) - at, "%u%s", raw[i],
                   (i + 1 == len) ? "" : ",");
    if ((i + 1) % 12 == 0 || i + 1 == len) {
      LOG.printf("[%s] %4u: %s\n", tag, (unsigned)(i - (i % 12)), line);
      at = 0;
      line[0] = '\0';
    }
  }
}

// Adds whatever the decoder recognised to a capture payload. value/address/
// command and state[] share one union, so which half is real depends entirely
// on hasACState() — reading the wrong one reads neighbouring bytes.
//
// The 64-bit value goes out as a hex string: JSON integers are only safe to
// 2^53, and this payload passes through a browser on its way to being shown.
static void addDecodeFields(JsonDocument &doc, const decode_results *r) {
  doc["protocol"] = typeToString(r->decode_type);
  doc["bits"] = r->bits;
  if (r->decode_type == UNKNOWN) return;   // nothing else is meaningful
  if (hasACState(r->decode_type)) {
    doc["state"] = resultToHexidecimal(r);
  } else {
    char hex[24];
    snprintf(hex, sizeof(hex), "0x%llX", (unsigned long long)r->value);
    doc["value"] = hex;
  }
}

// ── IR learn (리모컨 캡처) ──────────────────────────────────────────────
static bool publishIrJson(JsonDocument &doc) {
  String out;
  serializeJson(doc, out);
  if (out.length() > 8000) return false;   // must fit the MQTT buffer
  return mqtt.publish(topicIR, (const uint8_t *)out.c_str(), out.length(),
                      false);
}

static void publishCaptureError(const char *error) {
  JsonDocument doc;
  doc["type"] = "capture";
  doc["session_id"] = learnSessionId;
  doc["slot"] = learnSlot;
  doc["ok"] = false;
  doc["error"] = error;
  publishIrJson(doc);
}

static void stopLearn() {
  irrecv.disableIRIn();
  learnActive = false;
  FastLED.setBrightness(24);
  led(CRGB::Green);
}

static void stopMonitor() {
  irrecv.disableIRIn();
  monitorActive = false;
  FastLED.setBrightness(24);
  led(CRGB::Green);
  LOG.printf("[monitor] stopped -- %u frame(s) seen\n", monitorFrames);
}

static void handleLearnCommand(const uint8_t *payload, size_t len) {
  JsonDocument doc;
  if (deserializeJson(doc, payload, len) != DeserializationError::Ok) {
    LOG.println("[learn] bad JSON, ignoring");
    return;
  }
  const char *cmd = doc["cmd"] | "";
  if (strcmp(cmd, "LEARN_CANCEL") == 0) {
    if (learnActive) {
      LOG.println("[learn] canceled");
      stopLearn();
    }
    return;
  }
  if (strcmp(cmd, "MONITOR") == 0) {
    if (learnActive) {
      LOG.println("[monitor] a learn session is running -- try again after it ends");
      return;
    }
    if (monitorActive) stopMonitor();   // restart the window rather than stack
    long timeoutS = doc["timeout_s"] | 20L;
    monitorDeadline = millis() + (uint32_t)constrain(timeoutS, 5L, 120L) * 1000;
    monitorFrames = 0;
    irrecv.enableIRIn();
    monitorActive = true;
    FastLED.setBrightness(60);
    led(CRGB::Cyan);   // distinct from learn's purple: nothing is being stored
    LOG.printf("[monitor] IR 수신 대기 %lds -- 리모컨을 장비에 향하게 하고 누르세요\n",
               timeoutS);
    return;
  }
  if (strcmp(cmd, "MONITOR_CANCEL") == 0) {
    if (monitorActive) stopMonitor();
    return;
  }
  if (strcmp(cmd, "LEARN") != 0) return;
  if (monitorActive) stopMonitor();   // a real capture outranks a look
  learnSessionId = (const char *)(doc["session_id"] | "");
  learnSlot = (const char *)(doc["slot"] | "");
  long timeoutS = doc["timeout_s"] | 30L;
  learnDeadline = millis() + (uint32_t)constrain(timeoutS, 5L, 120L) * 1000;
  irrecv.enableIRIn();
  learnActive = true;
  FastLED.setBrightness(60);
  led(CRGB::Purple);   // "point the remote at me"
  LOG.printf("[learn] armed for slot %s (session %s, %lds)\n",
                learnSlot.c_str(), learnSessionId.c_str(), timeoutS);
}

// Runs from loop() while the monitor window is open. Everything it hears is
// printed and dropped -- the console is the only destination.
static void pollMonitor() {
  if (!monitorActive) return;
  if ((int32_t)(millis() - monitorDeadline) >= 0) {
    if (monitorFrames == 0)
      LOG.printf("[monitor] 아무것도 수신되지 않았습니다 -- 수신기 연결(G%d), "
                  "리모컨 방향, 건전지를 확인하세요\n", IR_RX_PIN);
    stopMonitor();
    return;
  }
  decode_results results;
  if (!irrecv.decode(&results)) return;
  uint16_t rawLen = getCorrectedRawLength(&results);
  if (rawLen < 12) {   // stray flicker from daylight or a fluorescent tube
    irrecv.resume();
    return;
  }
  monitorFrames++;
  uint16_t *raw = resultToRawArray(&results);
  LOG.printf("[monitor] frame #%u  protocol=%s\n", monitorFrames,
             typeToString(results.decode_type).c_str());
  dumpRawTimings("monitor", raw, rawLen);
  delete[] raw;
  irrecv.resume();
}

// Runs from loop() while a learn session is active.
static void pollLearn() {
  if (!learnActive) return;
  if ((int32_t)(millis() - learnDeadline) >= 0) {
    LOG.println("[learn] timeout — no signal seen");
    publishCaptureError("timeout");
    stopLearn();
    return;
  }
  decode_results results;
  if (!irrecv.decode(&results)) return;
  uint16_t rawLen = getCorrectedRawLength(&results);
  if (rawLen < 20) {   // stray flicker, not an AC frame — keep listening
    irrecv.resume();
    return;
  }
  uint16_t *raw = resultToRawArray(&results);
  JsonDocument doc;
  doc["type"] = "capture";
  doc["session_id"] = learnSessionId;
  doc["slot"] = learnSlot;
  doc["ok"] = true;
  doc["freq_khz"] = 38;   // demodulating receivers hide the true carrier
  doc["len"] = rawLen;
  // What the decoder made of it, when it made anything. Replaying a recognised
  // protocol regenerates the frame to spec instead of echoing our capture, so
  // it survives noise that raw replay would faithfully reproduce.
  addDecodeFields(doc, &results);
  JsonArray arr = doc["raw"].to<JsonArray>();
  for (uint16_t i = 0; i < rawLen; i++) arr.add(raw[i]);
  if (!publishIrJson(doc)) {
    LOG.printf("[learn] frame too long to publish (%u entries)\n", rawLen);
    publishCaptureError("too_long");
  } else {
    LOG.printf("[learn] captured %s: %u entries — sent\n",
                  learnSlot.c_str(), rawLen);
  }
  stopLearn();
}
#endif

static void handleAcFrame(const uint8_t *d, size_t len) {
  if (len != AC_SIZE || d[0] != AC_HEADER || d[7] != AC_TAIL ||
      d[6] != xorChecksum(d, 6)) {
    LOG.println("[ac] rejected malformed frame");
    return;
  }
  if (d[1] != DEVICE_ID) return;   // topic already targets us, but be strict

  uint8_t power = d[2], mode = d[3], temp = d[4], fan = d[5];

  // The frame carries full state, so what *changed* since the last command
  // decides the feedback colour: 전원 ON=빨강, OFF=주황, 온도+=노랑,
  // 온도-=옅은 노랑, 바람=하늘색. Power wins over temp, temp over fan.
  static uint8_t prevTemp = 0xFF, prevFan = 0xFF;
  static bool havePrev = false;
  const bool powerChanged = ((power != 0) != acOn);
  CRGB feedback;
  if (!havePrev || powerChanged) feedback = power ? CRGB(255, 0, 0)       // ON: 빨강
                                                  : CRGB(255, 170, 0);   // OFF: 주황
  else if (temp > prevTemp)      feedback = CRGB(255, 255, 0);           // 온도 +: 노랑
  else if (temp < prevTemp)      feedback = CRGB(110, 110, 20);          // 온도 -: 옅은 노랑
  else if (fan != prevFan)       feedback = CRGB(0, 200, 255);           // 바람: 하늘색
  else                           feedback = power ? CRGB(255, 0, 0)
                                                  : CRGB(255, 170, 0);
  prevTemp = temp;
  prevFan = fan;
  havePrev = true;

#ifdef HAS_IR
  if (irReady) {
    if (!sendRawSlot(power, mode, temp, fan)) return;   // nothing learned: skip
    acOn = power != 0;
    blinkAck(feedback);
    return;
  }
#endif
  (void)mode; (void)temp; (void)fan;
  LOG.println("[ac] no learned remote yet — command ignored");
}

// ── OTA / IRDATA ────────────────────────────────────────────────────────
static void handleOtaCommand(const uint8_t *payload, size_t len) {
  JsonDocument doc;
  if (deserializeJson(doc, payload, len) != DeserializationError::Ok) {
    LOG.println("[ota] bad JSON, ignoring");
    return;
  }
  const char *cmd = doc["cmd"] | "OTA";
  if (strcmp(cmd, "IRDATA") == 0) {
#ifdef HAS_IR
    irdataUrl = (const char *)(doc["url"] | "");
    irdataModelId = (const char *)(doc["model_id"] | "");
    irdataModel = (const char *)(doc["model"] | "");
    irdataSize = doc["size"] | -1L;
    if (irdataUrl.isEmpty()) {
      LOG.println("[irdata] command without url, ignoring");
      return;
    }
    irdataPending = true;   // picked up by loop()
#else
    LOG.println("[irdata] base image cannot store IR data — run SOTA first");
#endif
    return;
  }
  otaUrl = doc["url"] | "";
  otaModel = doc["model"] | "";
  otaSize = doc["size"] | -1L;
  if (otaUrl.isEmpty()) {
    LOG.println("[ota] command without url, ignoring");
    return;
  }
  otaPending = true;   // picked up by loop()
}

#ifdef HAS_IR
// Counts the keys of the top-level "slots" object without parsing the bundle
// into a document -- it holds every code at once and would not fit in RAM.
//
// A character walk rather than a substring search. Searching for a literal like
// "\":[" makes two assumptions the producer never promised: that it spaces its
// JSON one particular way, and that slots are one particular shape. Both have
// been wrong. This tracks string state and nesting depth instead, so it counts
// the same whether the writer packs or spaces its output, and whether a slot is
// an object (v2) or a bare timing array (v1).
static int countSlots(const String &text) {
  int at = text.indexOf("\"slots\"");
  if (at < 0) return 0;
  at = text.indexOf('{', at);
  if (at < 0) return 0;

  int depth = 0, n = 0;
  bool inStr = false, esc = false, keyPending = false;
  for (int i = at; i < (int)text.length(); i++) {
    char c = text[i];
    if (esc) { esc = false; continue; }
    if (inStr && c == '\\') { esc = true; continue; }
    if (c == '"') {
      inStr = !inStr;
      if (inStr && depth == 1) keyPending = true;   // a slot name
      continue;
    }
    if (inStr) continue;
    if (c == '{' || c == '[') { depth++; continue; }
    if (c == '}' || c == ']') { if (--depth == 0) break; continue; }
    if (c == ':' && depth == 1 && keyPending) { n++; keyPending = false; }
  }
  return n;
}

static void ackIrdata(bool ok, int slots, size_t bytes) {
  JsonDocument doc;
  doc["type"] = "irdata_ack";
  doc["model_id"] = irdataModelId;
  doc["ok"] = ok;
  if (ok) {
    doc["slots"] = slots;
    doc["bytes"] = (uint32_t)bytes;
  }
  publishIrJson(doc);
}

static void performIrdata() {
  irdataPending = false;
  LOG.printf("[irdata] start <- %s (model_id=%s)\n", irdataUrl.c_str(),
                irdataModelId.c_str());
  led(CRGB::Blue);

  HTTPClient http;
  http.begin(irdataUrl);
  http.setTimeout(20000);
  int code = http.GET();
  if (code != HTTP_CODE_OK) {
    LOG.printf("[irdata] fetch failed: HTTP %d\n", code);
    http.end();
    ackIrdata(false, 0, 0);
    led(CRGB::Green);
    return;
  }
  // Stream to SPIFFS in chunks; a full parse of a 100KB bundle would not fit
  // in RAM, and it never needs to — sendRawSlot() filter-reads one slot.
  File f = SPIFFS.open(IRDATA_PATH, FILE_WRITE);
  if (!f) {
    LOG.println("[irdata] cannot open SPIFFS file for writing");
    http.end();
    ackIrdata(false, 0, 0);
    led(CRGB::Green);
    return;
  }
  WiFiClient *stream = http.getStreamPtr();
  int contentLen = http.getSize();
  size_t written = 0;
  uint8_t buf[1024];
  uint32_t lastData = millis();
  while (http.connected() && (contentLen < 0 || written < (size_t)contentLen)) {
    size_t avail = stream->available();
    if (avail) {
      size_t n = stream->readBytes(buf, min(avail, sizeof(buf)));
      f.write(buf, n);
      written += n;
      lastData = millis();
    } else if (millis() - lastData > 10000) {
      break;   // stalled
    } else {
      delay(5);
    }
  }
  f.close();
  http.end();

  if (irdataSize > 0 && written != (size_t)irdataSize) {
    LOG.printf("[irdata] size mismatch: got %u, announced %ld\n",
                  (unsigned)written, irdataSize);
    SPIFFS.remove(IRDATA_PATH);
    ackIrdata(false, 0, 0);
    led(CRGB::Green);
    return;
  }

  // Sanity-check the header before committing anything to NVS. Unlike the
  // firmware OTA (which stamps NVS *before* rebooting into the new image),
  // there is no reboot here, so a bad download must leave the previous
  // working configuration untouched.
  int slots = 0;
  {
    File check = SPIFFS.open(IRDATA_PATH, FILE_READ);
    JsonDocument filter;
    filter["v"] = true;
    filter["model_id"] = true;
    filter["freq_khz"] = true;
    JsonDocument doc;
    DeserializationError err =
        deserializeJson(doc, check, DeserializationOption::Filter(filter));
    check.close();
    int version = doc["v"] | 0;
    if (err != DeserializationError::Ok || (version != 1 && version != 2) ||
        irdataModelId != (const char *)(doc["model_id"] | "")) {
      LOG.println("[irdata] bundle failed validation — keeping old config");
      SPIFFS.remove(IRDATA_PATH);
      ackIrdata(false, 0, 0);
      led(CRGB::Green);
      return;
    }
    check = SPIFFS.open(IRDATA_PATH, FILE_READ);
    String text = check.readString();
    check.close();
    slots = countSlots(text);
  }

  prefs.begin("atomair", false);
  prefs.putString("model", irdataModel);
  prefs.putString("model_id", irdataModelId);
  prefs.end();
  acModel = irdataModel;
  acModelId = irdataModelId;
  irReady = true;

  ackIrdata(true, slots, written);
  LOG.printf("[irdata] OK: %u bytes, %d combos (%s) — raw replay armed\n",
                (unsigned)written, slots, irdataModelId.c_str());
  led(CRGB::Green);
}
#endif

static void performOta() {
  otaPending = false;
  LOG.printf("[ota] start <- %s (%s)\n", otaUrl.c_str(),
                otaModel.isEmpty() ? "IR image" : otaModel.c_str());
  led(CRGB::Blue);

  // The model name is only a label for the UI; readiness comes from the
  // learned bundle in SPIFFS, which this flash does not touch.
  prefs.begin("atomair", false);
  prefs.putString("model", otaModel);
  prefs.end();

  HTTPClient http;
  http.begin(otaUrl);
  http.setTimeout(20000);
  int code = http.GET();
  if (code != HTTP_CODE_OK) {
    LOG.printf("[ota] fetch failed: HTTP %d\n", code);
    http.end();
    return;
  }
  int contentLen = http.getSize();
  if (otaSize > 0 && contentLen > 0 && contentLen != otaSize) {
    LOG.printf("[ota] size mismatch: got %d, announced %ld\n",
                  contentLen, otaSize);
    http.end();
    return;
  }
  if (!Update.begin(contentLen > 0 ? (size_t)contentLen : UPDATE_SIZE_UNKNOWN)) {
    LOG.printf("[ota] no space: %s\n", Update.errorString());
    http.end();
    return;
  }
  size_t written = Update.writeStream(http.getStream());
  http.end();
  if (!Update.end(true) || !Update.isFinished()) {
    LOG.printf("[ota] flash failed after %u bytes: %s\n",
                  (unsigned)written, Update.errorString());
    return;
  }
  LOG.printf("[ota] OK, %u bytes — rebooting into new firmware\n",
                (unsigned)written);
  delay(300);
  ESP.restart();
}

// ── serial provisioning ─────────────────────────────────────────────────
// Network settings live in NVS so a technician can re-point a unit at a new
// network or a new store PC over USB, without rebuilding or reflashing:
//   wifi <ssid> <password>   save to NVS and reboot ("quotes" for spaces)
//   wifi?                    show ssid / source / status (password masked)
//   wifi reset               drop the NVS credentials, back to config.h
//   mqtt <host> [port]       save the gateway address to NVS and reboot
//   mqtt?                    show host / port / source / link state
//   mqtt reset               drop the NVS address, back to config.h
//   scan                     list the 2.4GHz APs this radio can actually see
//   ir?                      is a receiver wired to IR_RX_PIN? (atom_ac only)
static String takeToken(String &line) {
  line.trim();
  if (line.startsWith("\"")) {
    int end = line.indexOf('"', 1);
    if (end > 0) {
      String tok = line.substring(1, end);
      line = line.substring(end + 1);
      return tok;
    }
  }
  int sp = line.indexOf(' ');
  if (sp < 0) {
    String tok = line;
    line = "";
    return tok;
  }
  String tok = line.substring(0, sp);
  line = line.substring(sp + 1);
  return tok;
}

static const char *authName(wifi_auth_mode_t mode) {
  switch (mode) {
    case WIFI_AUTH_OPEN:            return "open";
    case WIFI_AUTH_WEP:             return "WEP";
    case WIFI_AUTH_WPA_PSK:         return "WPA";
    case WIFI_AUTH_WPA2_PSK:        return "WPA2";
    case WIFI_AUTH_WPA_WPA2_PSK:    return "WPA/WPA2";
    case WIFI_AUTH_WPA2_ENTERPRISE: return "WPA2-ent";
    case WIFI_AUTH_WPA3_PSK:        return "WPA3";
    case WIFI_AUTH_WPA2_WPA3_PSK:   return "WPA2/WPA3";
    default:                        return "?";
  }
}

// scan lists what this radio can actually see, which is the only answer that
// settles "the SSID is right but it will not join". An ESP32 has no 5 GHz
// radio, so a 5 GHz-only AP is invisible here however strong it looks on a
// phone -- that difference is exactly what this command is for.
static void handleScan() {
  LOG.println("[scan] scanning 2.4GHz...");
  WiFi.mode(WIFI_STA);
  int found = WiFi.scanNetworks();
  if (found <= 0) {
    LOG.println("[scan] no networks found");
  } else {
    LOG.printf("[scan] %d networks\n", found);
    for (int i = 0; i < found; i++) {
      LOG.printf("[scan]  %-24s ch%-3d %4d dBm  %s\n", WiFi.SSID(i).c_str(),
                    WiFi.channel(i), WiFi.RSSI(i),
                    authName(WiFi.encryptionType(i)));
    }
  }
  WiFi.scanDelete();
  LOG.println("[scan] done -- this radio is 2.4GHz only; a 5GHz-only AP "
                 "never appears here");
  // Scanning drops a pending association, so re-arm it or ensureWifi's loop
  // would spin forever waiting on a request that no longer exists.
  if (WiFi.status() != WL_CONNECTED && !wifiSsid.isEmpty())
    WiFi.begin(wifiSsid.c_str(), wifiPass.c_str());
}

#ifdef HAS_IR
// ir? answers "is the receiver actually wired to IR_RX_PIN?", which is
// otherwise invisible: a VS1838B/TSOP38238 drives its output HIGH while idle,
// so it beats an internal pulldown. A floating pin does not. Then it listens
// so a remote press proves the whole path, not just the DC level.
static void handleIrProbe() {
  if (learnActive) {
    LOG.println("[ir] a learn session is running -- try again once it ends");
    return;
  }
  LOG.printf("[ir] tx=G%d rx=G%d\n", IR_TX_PIN, IR_RX_PIN);

  // No disableIRIn() here: the receiver is only armed during a learn, and
  // calling it without a prior enableIRIn() dereferences a null timer handle
  // and panics the chip. pinMode alone is enough to read the idle level.
  pinMode(IR_RX_PIN, INPUT_PULLDOWN);
  delay(20);
  int highs = 0;
  for (int i = 0; i < 200; i++) {
    if (digitalRead(IR_RX_PIN)) highs++;
    delayMicroseconds(200);
  }
  if (highs >= 195)
    LOG.println("[ir] receiver detected: idle HIGH against a pulldown");
  else if (highs <= 5)
    LOG.printf("[ir] nothing on G%d: reads LOW, so the pin is floating "
                  "(check OUT->G%d, VCC->3V3, GND)\n", IR_RX_PIN, IR_RX_PIN);
  else
    LOG.printf("[ir] G%d is unsteady (%d/200 high) -- either IR is hitting "
                  "it right now, or the wiring is loose\n", IR_RX_PIN, highs);

  LOG.println("[ir] listening 5s -- point a remote at the unit and press a button");
  irrecv.enableIRIn();
  decode_results results;
  uint32_t until = millis() + 5000;
  int frames = 0;
  while ((int32_t)(millis() - until) < 0) {
    if (irrecv.decode(&results)) {
      frames++;
      LOG.printf("[ir] captured a frame: %u timings\n",
                    (unsigned)results.rawlen);
      irrecv.resume();
    }
    delay(5);
  }
  irrecv.disableIRIn();   // safe: this call site enabled it a moment ago
  if (frames == 0)
    LOG.println("[ir] nothing captured -- no receiver, wrong pin, or no "
                   "button was pressed");
  else
    LOG.printf("[ir] %d frame(s) captured -- receive path works\n", frames);
}
#endif

static void handleSerialLine(String line) {
  line.trim();
  if (line.isEmpty()) return;

  if (line == "wifi?") {
    LOG.printf("[wifi] ssid=%s source=%s status=%s ip=%s\n",
                  wifiSsid.c_str(), wifiFromNvs ? "NVS" : "config.h",
                  WiFi.status() == WL_CONNECTED ? "connected" : "disconnected",
                  WiFi.localIP().toString().c_str());
    return;
  }
  if (line == "wifi reset") {
    prefs.begin("atomair", false);
    prefs.remove("wifi_ssid");
    prefs.remove("wifi_pass");
    prefs.end();
    LOG.println("[wifi] NVS credentials cleared -- rebooting to config.h defaults");
    delay(200);
    ESP.restart();
  }
  if (line.startsWith("wifi ")) {
    String rest = line.substring(5);
    String ssid = takeToken(rest);
    // No password = an open network, which is what guest Wi-Fi usually is.
    String pass = takeToken(rest);
    if (ssid.isEmpty()) {
      LOG.println("[wifi] usage: wifi <ssid> [password]   (\"quotes\" for spaces)");
      return;
    }
    prefs.begin("atomair", false);
    prefs.putString("wifi_ssid", ssid);
    prefs.putString("wifi_pass", pass);
    prefs.end();
    LOG.printf("[wifi] saved ssid=%s (%s) to NVS -- rebooting\n", ssid.c_str(),
               pass.isEmpty() ? "open" : "WPA");
    delay(200);
    ESP.restart();
  }

  if (line == "mqtt?") {
    LOG.printf("[mqtt] host=%s port=%u source=%s status=%s\n",
                  mqttHost.isEmpty() ? "(unset)" : mqttHost.c_str(), mqttPort,
                  mqttFromNvs ? "NVS" : "config.h",
                  mqtt.connected() ? "connected" : "disconnected");
    return;
  }
  if (line == "mqtt reset") {
    prefs.begin("atomair", false);
    prefs.remove("mqtt_host");
    prefs.remove("mqtt_port");
    prefs.end();
    LOG.println("[mqtt] NVS address cleared -- rebooting to config.h defaults");
    delay(200);
    ESP.restart();
  }
  if (line.startsWith("mqtt ")) {
    String rest = line.substring(5);
    String host = takeToken(rest);
    String portTok = takeToken(rest);
    uint16_t port = MQTT_PORT;   // the port is optional; brokers rarely move
    if (!portTok.isEmpty()) {
      long parsed = portTok.toInt();
      if (parsed < 1 || parsed > 65535) {
        LOG.printf("[mqtt] bad port: %s\n", portTok.c_str());
        return;
      }
      port = (uint16_t)parsed;
    }
    if (host.isEmpty()) {
      LOG.println("[mqtt] usage: mqtt <host> [port]   (host = store PC's LAN IP)");
      return;
    }
    prefs.begin("atomair", false);
    prefs.putString("mqtt_host", host);
    prefs.putUShort("mqtt_port", port);
    prefs.end();
    LOG.printf("[mqtt] saved %s:%u to NVS -- rebooting\n", host.c_str(), port);
    delay(200);
    ESP.restart();
  }

  if (line == "scan") {
    handleScan();
    return;
  }

#ifdef HAS_IR
  if (line == "ir?") {
    handleIrProbe();
    return;
  }
#endif

  LOG.printf("[serial] unknown command: %s\n", line.c_str());
}

static void pollSerial() {
  static String pending;
  while (Serial.available()) {
    char c = (char)Serial.read();
    if (c == '\n' || c == '\r') {
      if (!pending.isEmpty()) handleSerialLine(pending);
      pending = "";
    } else if (pending.length() < 160) {
      pending += c;
    }
  }
}

// ── MQTT ────────────────────────────────────────────────────────────────
static void onMqttMessage(char *topic, uint8_t *payload, unsigned int len) {
  if (strcmp(topic, topicAC) == 0) {
    handleAcFrame(payload, len);
  } else if (strcmp(topic, topicOTA) == 0) {
    handleOtaCommand(payload, len);
#ifdef HAS_IR
  } else if (strcmp(topic, topicLearn) == 0) {
    handleLearnCommand(payload, len);
#endif
  }
}

static bool ensureMqtt() {
  if (mqtt.connected()) return true;
  led(CRGB::Yellow);
  if (mqttHost.isEmpty()) {
    // Same deal as Wi-Fi: config.h ships blank so no LAN address reaches git.
    // loop() re-enters every 2 s and pollSerial() runs ahead of us, so nagging
    // at a slower cadence is enough to keep the console usable.
    static uint32_t lastPrompt = 0;
    uint32_t now = millis();
    if (lastPrompt == 0 || now - lastPrompt >= 5000) {
      lastPrompt = now;
      LOG.println("[mqtt] gateway address not set -- run: mqtt <host> [port]");
    }
    return false;
  }
  char clientId[48];
  snprintf(clientId, sizeof(clientId), "atom-%s-%d-%04X", STORE_ID, DEVICE_ID,
           (uint16_t)(ESP.getEfuseMac() & 0xFFFF));
  if (!mqtt.connect(clientId)) return false;
  mqtt.subscribe(topicAC, 1);
  mqtt.subscribe(topicOTA, 1);
#ifdef HAS_IR
  mqtt.subscribe(topicLearn, 1);
#endif
  LOG.printf("[mqtt] connected to %s:%u (%s) as %s\n", mqttHost.c_str(),
                mqttPort, mqttFromNvs ? "NVS" : "config.h", clientId);
  return true;
}

static void ensureWifi() {
  if (WiFi.status() == WL_CONNECTED) return;
  led(CRGB::Red);
  if (wifiSsid.isEmpty()) {
    // config.h ships blank so real credentials never reach git. The only way
    // out of here is a `wifi <ssid> <password>` over serial, which saves to
    // NVS and reboots — so park, prompt, and keep the console responsive.
    for (;;) {
      LOG.println("[wifi] not provisioned -- run: wifi <ssid> [password]");
      for (int i = 0; i < 50; i++) {   // ~5 s between prompts
        pollSerial();
        delay(100);
      }
    }
  }
  LOG.printf("[wifi] connecting to %s (%s)", wifiSsid.c_str(),
                wifiFromNvs ? "NVS" : "config.h");
  WiFi.mode(WIFI_STA);
  WiFi.begin(wifiSsid.c_str(), wifiPass.c_str());
  while (WiFi.status() != WL_CONNECTED) {
    // Wrong saved credentials park us here forever, so the serial console
    // must stay responsive to accept a corrected `wifi ...` command.
    pollSerial();
    delay(500);
    LOG.print(".");
  }
  LOG.printf("\n[wifi] connected, ip=%s\n", WiFi.localIP().toString().c_str());
}

// ── lifecycle ───────────────────────────────────────────────────────────
void setup() {
  Serial.begin(115200);
  FastLED.addLeds<WS2812, STATUS_LED_PIN, GRB>(statusLed, 1);
  FastLED.setBrightness(24);
  led(CRGB::White);

  snprintf(topicSensor, sizeof(topicSensor), "atom/%s/sensor", STORE_ID);
  snprintf(topicAC, sizeof(topicAC), "atom/%s/ac/%d", STORE_ID, DEVICE_ID);
  snprintf(topicOTA, sizeof(topicOTA), "atom/%s/ota/%d", STORE_ID, DEVICE_ID);
  snprintf(topicLearn, sizeof(topicLearn), "atom/%s/learn/%d", STORE_ID, DEVICE_ID);
  snprintf(topicIR, sizeof(topicIR), "atom/%s/ir/%d", STORE_ID, DEVICE_ID);
  snprintf(topicLog, sizeof(topicLog), "atom/%s/log/%d", STORE_ID, DEVICE_ID);

  detectSensor();
  LOG.printf("[boot] sensor: %s%s\n", sensorName(),
                sensorKind == SENSOR_NONE
                    ? " (packets will carry FLAG_SENSOR_FAULT; "
                      "plug a sensor into the Grove port and reset)"
                    : "");

  prefs.begin("atomair", true);
  acModel = prefs.getString("model", "");
  acModelId = prefs.getString("model_id", "");
  wifiSsid = prefs.getString("wifi_ssid", "");
  wifiPass = prefs.getString("wifi_pass", "");
  mqttHost = prefs.getString("mqtt_host", "");
  mqttPort = prefs.getUShort("mqtt_port", MQTT_PORT);
  prefs.end();
  wifiFromNvs = !wifiSsid.isEmpty();
  if (!wifiFromNvs) {
    wifiSsid = WIFI_SSID;
    wifiPass = WIFI_PASS;
  }
  if (wifiSsid.isEmpty())
    LOG.println("[boot] wifi: not provisioned -- run: wifi <ssid> <password>");
  else
    LOG.printf("[boot] wifi ssid=%s (%s)\n", wifiSsid.c_str(),
                  wifiFromNvs ? "NVS" : "config.h default");

  mqttFromNvs = !mqttHost.isEmpty();
  if (!mqttFromNvs) {
    mqttHost = MQTT_HOST;
    mqttPort = MQTT_PORT;
  }
  if (mqttHost.isEmpty())
    LOG.println("[boot] mqtt: gateway address not set -- run: mqtt <host> [port]");
  else
    LOG.printf("[boot] mqtt %s:%u (%s)\n", mqttHost.c_str(), mqttPort,
                  mqttFromNvs ? "NVS" : "config.h default");

#ifdef HAS_IR
  irsend.begin();
  if (!SPIFFS.begin(true))   // format on first mount so IRDATA can land later
    LOG.println("[boot] SPIFFS mount failed — learned IR replay unavailable");
  // Readiness is one question now: did a learned bundle survive the reboot?
  irReady = SPIFFS.exists(IRDATA_PATH);
  // The pins are in the banner because a learn that never captures is almost
  // always a receiver on the wrong pin, and this is the cheapest way to rule
  // that out over the serial console.
  LOG.printf("[boot] atom_ac firmware, remote=%s ir_ready=%d tx=G%d rx=G%d\n",
                acModel.isEmpty() ? "(none learned)" : acModel.c_str(), irReady,
                IR_TX_PIN, IR_RX_PIN);
#else
  irReady = false;   // base image cannot transmit IR regardless of NVS
  LOG.println("[boot] atom_base firmware (sensor + OTA only)");
#endif
  LOG.printf("[boot] store=%s dev=%d -> %s\n", STORE_ID, DEVICE_ID,
                topicSensor);

  // Safe to hand over the String's buffer: mqttHost is final by now, and a
  // later `mqtt ...` command reboots rather than reassigning it.
  if (!mqttHost.isEmpty()) mqtt.setServer(mqttHost.c_str(), mqttPort);
  mqtt.setCallback(onMqttMessage);
#ifdef HAS_IR
  mqtt.setBufferSize(8192);   // a learned AC frame is ~600 timings of JSON
#else
  mqtt.setBufferSize(1024);   // OTA JSON + headroom
#endif
}

// Ships buffered console lines to the gateway. Batched and rate-limited on
// purpose: a burst (boot, an OTA, an `ir?` probe) becomes one message instead
// of twenty, and the 600-byte budget keeps a batch inside the base build's
// 1 KB MQTT buffer.
static void flushLog() {
  static uint32_t lastFlush = 0;
  static bool inFlush = false;   // a failed publish logs, which would recurse
  if (inFlush || !LOG.pending()) return;
  uint32_t now = millis();
  if (now - lastFlush < 400) return;
  lastFlush = now;
  inFlush = true;

  JsonDocument doc;
  doc["type"] = "log";
  doc["dev_id"] = DEVICE_ID;
  doc["uptime_ms"] = now;
  JsonArray lines = doc["lines"].to<JsonArray>();
  size_t budget = 0;
  while (LOG.pending() && budget < 600) {
    String line = LOG.pop();
    budget += line.length() + 8;
    lines.add(line);
  }
  String out;
  serializeJson(doc, out);
  mqtt.publish(topicLog, (const uint8_t *)out.c_str(), out.length(), false);
  inFlush = false;
}

void loop() {
  pollSerial();
  ensureWifi();
  if (!ensureMqtt()) {
    delay(2000);
    return;
  }
  led(CRGB::Green);
  mqtt.loop();
  flushLog();

  if (otaPending) performOta();   // may not return (reboots on success)
#ifdef HAS_IR
  if (irdataPending) performIrdata();
  pollLearn();
  pollMonitor();
#endif

  static uint32_t lastPublish = 0;
  uint32_t now = millis();
  if (now - lastPublish >= 1000) {
    lastPublish = now;
    uint8_t packet[SENSOR_SIZE];
    buildSensorPacket(packet);
    mqtt.publish(topicSensor, packet, SENSOR_SIZE, false);
  }
  delay(10);
}
