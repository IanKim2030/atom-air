// Atom Air — M5Stack ATOM Lite firmware.
//
// Speaks the store MQTT contract whose executable specification is
// tools/fake_atom.py, against the wire formats in common/protocol.py:
//
//   publishes  atom/{store}/sensor          12-byte SensorPacket, 1 Hz
//   publishes  atom/{store}/ir/{dev_id}     JSON IR events (capture, IRDATA ack)
//   subscribes atom/{store}/ac/{dev_id}     8-byte AC control frame -> IR
//   subscribes atom/{store}/ota/{dev_id}    JSON {"cmd":"OTA"|"IRDATA","url",...}
//   subscribes atom/{store}/learn/{dev_id}  JSON {"cmd":"LEARN","slot",...}
//
// Built two ways (see platformio.ini): atom_base has no IR library and never
// raises FLAG_IR_READY; atom_ac adds IRremoteESP8266's IRsend and IRrecv, and
// reports ready once a learned bundle is actually sitting in SPIFFS.
//
// One IR control path, and it is manufacturer-agnostic: replay the timings
// captured from the customer's own remote. The learned bundle arrives as a
// JSON data file over the same OTA HTTP server (cmd IRDATA), lives in SPIFFS,
// and maps each full AC state ("off", "cool_24", ...) to a raw mark/space
// timing array. There is no brand or protocol database on the device -- a
// remote nobody has written a driver for works the same as a common one.
// Learning: a LEARN command arms the IR receiver on IR_RX_PIN; the next
// decoded frame goes up atom/{store}/ir/{dev} as raw timings.
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
static IRsend irsendRaw(IR_TX_PIN);            // learned-code replay (TX)
static IRrecv irrecv(IR_RX_PIN, 1024, 50, true);  // learn capture (AC frames are long)

// IRDATA download request, recorded by the callback, run from loop().
static volatile bool irdataPending = false;
static String irdataUrl, irdataModelId, irdataModel;
static long irdataSize = -1;

// Learn session, armed by a LEARN command.
static bool learnActive = false;
static uint32_t learnDeadline = 0;
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
// An AC remote transmits its full state per keypress, so each learned code is
// one state combination, keyed "off" / "cool_18".."cool_30" / "heat_18"...
static bool slotForState(uint8_t power, uint8_t mode, uint8_t temp,
                         char out[16]) {
  if (!power) {
    strlcpy(out, "off", 16);
    return true;
  }
  const char *m = (mode == 0) ? "cool" : (mode == 1) ? "heat" : nullptr;
  if (m == nullptr) return false;   // dry/fan/auto are not learnable combos
  snprintf(out, 16, "%s_%d", m, constrain(temp, 18, 30));
  return true;
}

// Loads one slot's timings from SPIFFS and transmits them. The ArduinoJson
// filter keeps memory at ~one code even though the file holds dozens.
static bool sendRawSlot(uint8_t power, uint8_t mode, uint8_t temp) {
  char slot[16];
  if (!slotForState(power, mode, temp, slot)) {
    Serial.printf("[ir] raw: mode %d has no learned combos — command ignored\n",
                  mode);
    return false;
  }
  File f = SPIFFS.open(IRDATA_PATH, FILE_READ);
  if (!f) {
    Serial.println("[ir] raw: no /irdata.json in SPIFFS — command ignored");
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
    Serial.printf("[ir] raw: bundle parse failed: %s\n", err.c_str());
    return false;
  }
  JsonArray arr = doc["slots"][slot];
  if (arr.isNull() || arr.size() < 20 || arr.size() > 1024) {
    Serial.printf("[ir] raw: slot %s not learned — command ignored\n", slot);
    return false;
  }
  static uint16_t rawBuf[1024];
  uint16_t n = 0;
  for (JsonVariant v : arr) rawBuf[n++] = v.as<uint16_t>();
  uint16_t freq = doc["freq_khz"] | 38;
  irsendRaw.sendRaw(rawBuf, n, freq);
  Serial.printf("[ir] raw: replayed %s (%u entries @ %ukHz)\n", slot, n, freq);
  return true;
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

static void handleLearnCommand(const uint8_t *payload, size_t len) {
  JsonDocument doc;
  if (deserializeJson(doc, payload, len) != DeserializationError::Ok) {
    Serial.println("[learn] bad JSON, ignoring");
    return;
  }
  const char *cmd = doc["cmd"] | "";
  if (strcmp(cmd, "LEARN_CANCEL") == 0) {
    if (learnActive) {
      Serial.println("[learn] canceled");
      stopLearn();
    }
    return;
  }
  if (strcmp(cmd, "LEARN") != 0) return;
  learnSessionId = (const char *)(doc["session_id"] | "");
  learnSlot = (const char *)(doc["slot"] | "");
  long timeoutS = doc["timeout_s"] | 30L;
  learnDeadline = millis() + (uint32_t)constrain(timeoutS, 5L, 120L) * 1000;
  irrecv.enableIRIn();
  learnActive = true;
  FastLED.setBrightness(60);
  led(CRGB::Purple);   // "point the remote at me"
  Serial.printf("[learn] armed for slot %s (session %s, %lds)\n",
                learnSlot.c_str(), learnSessionId.c_str(), timeoutS);
}

// Runs from loop() while a learn session is active.
static void pollLearn() {
  if (!learnActive) return;
  if ((int32_t)(millis() - learnDeadline) >= 0) {
    Serial.println("[learn] timeout — no signal seen");
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
  JsonArray arr = doc["raw"].to<JsonArray>();
  for (uint16_t i = 0; i < rawLen; i++) arr.add(raw[i]);
  delete[] raw;
  if (!publishIrJson(doc)) {
    Serial.printf("[learn] frame too long to publish (%u entries)\n", rawLen);
    publishCaptureError("too_long");
  } else {
    Serial.printf("[learn] captured %s: %u entries — sent\n",
                  learnSlot.c_str(), rawLen);
  }
  stopLearn();
}
#endif

static void handleAcFrame(const uint8_t *d, size_t len) {
  if (len != AC_SIZE || d[0] != AC_HEADER || d[7] != AC_TAIL ||
      d[6] != xorChecksum(d, 6)) {
    Serial.println("[ac] rejected malformed frame");
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
    if (!sendRawSlot(power, mode, temp)) return;   // unlearned combo: skip
    acOn = power != 0;
    blinkAck(feedback);
    return;
  }
#endif
  (void)mode; (void)temp; (void)fan;
  Serial.println("[ac] no learned remote yet — command ignored");
}

// ── OTA / IRDATA ────────────────────────────────────────────────────────
static void handleOtaCommand(const uint8_t *payload, size_t len) {
  JsonDocument doc;
  if (deserializeJson(doc, payload, len) != DeserializationError::Ok) {
    Serial.println("[ota] bad JSON, ignoring");
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
      Serial.println("[irdata] command without url, ignoring");
      return;
    }
    irdataPending = true;   // picked up by loop()
#else
    Serial.println("[irdata] base image cannot store IR data — run SOTA first");
#endif
    return;
  }
  otaUrl = doc["url"] | "";
  otaModel = doc["model"] | "";
  otaSize = doc["size"] | -1L;
  if (otaUrl.isEmpty()) {
    Serial.println("[ota] command without url, ignoring");
    return;
  }
  otaPending = true;   // picked up by loop()
}

#ifdef HAS_IR
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
  Serial.printf("[irdata] start <- %s (model_id=%s)\n", irdataUrl.c_str(),
                irdataModelId.c_str());
  led(CRGB::Blue);

  HTTPClient http;
  http.begin(irdataUrl);
  http.setTimeout(20000);
  int code = http.GET();
  if (code != HTTP_CODE_OK) {
    Serial.printf("[irdata] fetch failed: HTTP %d\n", code);
    http.end();
    ackIrdata(false, 0, 0);
    led(CRGB::Green);
    return;
  }
  // Stream to SPIFFS in chunks; a full parse of a 100KB bundle would not fit
  // in RAM, and it never needs to — sendRawSlot() filter-reads one slot.
  File f = SPIFFS.open(IRDATA_PATH, FILE_WRITE);
  if (!f) {
    Serial.println("[irdata] cannot open SPIFFS file for writing");
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
    Serial.printf("[irdata] size mismatch: got %u, announced %ld\n",
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
    if (err != DeserializationError::Ok || (doc["v"] | 0) != 1 ||
        irdataModelId != (const char *)(doc["model_id"] | "")) {
      Serial.println("[irdata] bundle failed validation — keeping old config");
      SPIFFS.remove(IRDATA_PATH);
      ackIrdata(false, 0, 0);
      led(CRGB::Green);
      return;
    }
    // Count the learned combos for the ack without a full parse: inside the
    // slots object every key is a quoted string directly followed by ":[",
    // and the timing arrays themselves contain no quotes.
    check = SPIFFS.open(IRDATA_PATH, FILE_READ);
    String text = check.readString();
    check.close();
    for (int i = text.indexOf("\"slots\"");
         (i = text.indexOf("\":[", i + 1)) >= 0;)
      slots++;
  }

  prefs.begin("atomair", false);
  prefs.putString("model", irdataModel);
  prefs.putString("model_id", irdataModelId);
  prefs.end();
  acModel = irdataModel;
  acModelId = irdataModelId;
  irReady = true;

  ackIrdata(true, slots, written);
  Serial.printf("[irdata] OK: %u bytes, %d combos (%s) — raw replay armed\n",
                (unsigned)written, slots, irdataModelId.c_str());
  led(CRGB::Green);
}
#endif

static void performOta() {
  otaPending = false;
  Serial.printf("[ota] start <- %s (%s)\n", otaUrl.c_str(),
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
    Serial.printf("[ota] fetch failed: HTTP %d\n", code);
    http.end();
    return;
  }
  int contentLen = http.getSize();
  if (otaSize > 0 && contentLen > 0 && contentLen != otaSize) {
    Serial.printf("[ota] size mismatch: got %d, announced %ld\n",
                  contentLen, otaSize);
    http.end();
    return;
  }
  if (!Update.begin(contentLen > 0 ? (size_t)contentLen : UPDATE_SIZE_UNKNOWN)) {
    Serial.printf("[ota] no space: %s\n", Update.errorString());
    http.end();
    return;
  }
  size_t written = Update.writeStream(http.getStream());
  http.end();
  if (!Update.end(true) || !Update.isFinished()) {
    Serial.printf("[ota] flash failed after %u bytes: %s\n",
                  (unsigned)written, Update.errorString());
    return;
  }
  Serial.printf("[ota] OK, %u bytes — rebooting into new firmware\n",
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
  Serial.println("[scan] scanning 2.4GHz...");
  WiFi.mode(WIFI_STA);
  int found = WiFi.scanNetworks();
  if (found <= 0) {
    Serial.println("[scan] no networks found");
  } else {
    Serial.printf("[scan] %d networks\n", found);
    for (int i = 0; i < found; i++) {
      Serial.printf("[scan]  %-24s ch%-3d %4d dBm  %s\n", WiFi.SSID(i).c_str(),
                    WiFi.channel(i), WiFi.RSSI(i),
                    authName(WiFi.encryptionType(i)));
    }
  }
  WiFi.scanDelete();
  Serial.println("[scan] done -- this radio is 2.4GHz only; a 5GHz-only AP "
                 "never appears here");
  // Scanning drops a pending association, so re-arm it or ensureWifi's loop
  // would spin forever waiting on a request that no longer exists.
  if (WiFi.status() != WL_CONNECTED && !wifiSsid.isEmpty())
    WiFi.begin(wifiSsid.c_str(), wifiPass.c_str());
}

static void handleSerialLine(String line) {
  line.trim();
  if (line.isEmpty()) return;

  if (line == "wifi?") {
    Serial.printf("[wifi] ssid=%s source=%s status=%s ip=%s\n",
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
    Serial.println("[wifi] NVS credentials cleared -- rebooting to config.h defaults");
    delay(200);
    ESP.restart();
  }
  if (line.startsWith("wifi ")) {
    String rest = line.substring(5);
    String ssid = takeToken(rest);
    String pass = takeToken(rest);
    if (ssid.isEmpty() || pass.isEmpty()) {
      Serial.println("[wifi] usage: wifi <ssid> <password>   (\"quotes\" for spaces)");
      return;
    }
    prefs.begin("atomair", false);
    prefs.putString("wifi_ssid", ssid);
    prefs.putString("wifi_pass", pass);
    prefs.end();
    Serial.printf("[wifi] saved ssid=%s to NVS -- rebooting\n", ssid.c_str());
    delay(200);
    ESP.restart();
  }

  if (line == "mqtt?") {
    Serial.printf("[mqtt] host=%s port=%u source=%s status=%s\n",
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
    Serial.println("[mqtt] NVS address cleared -- rebooting to config.h defaults");
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
        Serial.printf("[mqtt] bad port: %s\n", portTok.c_str());
        return;
      }
      port = (uint16_t)parsed;
    }
    if (host.isEmpty()) {
      Serial.println("[mqtt] usage: mqtt <host> [port]   (host = store PC's LAN IP)");
      return;
    }
    prefs.begin("atomair", false);
    prefs.putString("mqtt_host", host);
    prefs.putUShort("mqtt_port", port);
    prefs.end();
    Serial.printf("[mqtt] saved %s:%u to NVS -- rebooting\n", host.c_str(), port);
    delay(200);
    ESP.restart();
  }

  if (line == "scan") {
    handleScan();
    return;
  }

  Serial.printf("[serial] unknown command: %s\n", line.c_str());
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
      Serial.println("[mqtt] gateway address not set -- run: mqtt <host> [port]");
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
  Serial.printf("[mqtt] connected to %s:%u (%s) as %s\n", mqttHost.c_str(),
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
      Serial.println("[wifi] not provisioned -- run: wifi <ssid> <password>");
      for (int i = 0; i < 50; i++) {   // ~5 s between prompts
        pollSerial();
        delay(100);
      }
    }
  }
  Serial.printf("[wifi] connecting to %s (%s)", wifiSsid.c_str(),
                wifiFromNvs ? "NVS" : "config.h");
  WiFi.mode(WIFI_STA);
  WiFi.begin(wifiSsid.c_str(), wifiPass.c_str());
  while (WiFi.status() != WL_CONNECTED) {
    // Wrong saved credentials park us here forever, so the serial console
    // must stay responsive to accept a corrected `wifi ...` command.
    pollSerial();
    delay(500);
    Serial.print(".");
  }
  Serial.printf("\n[wifi] connected, ip=%s\n", WiFi.localIP().toString().c_str());
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

  detectSensor();
  Serial.printf("[boot] sensor: %s%s\n", sensorName(),
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
    Serial.println("[boot] wifi: not provisioned -- run: wifi <ssid> <password>");
  else
    Serial.printf("[boot] wifi ssid=%s (%s)\n", wifiSsid.c_str(),
                  wifiFromNvs ? "NVS" : "config.h default");

  mqttFromNvs = !mqttHost.isEmpty();
  if (!mqttFromNvs) {
    mqttHost = MQTT_HOST;
    mqttPort = MQTT_PORT;
  }
  if (mqttHost.isEmpty())
    Serial.println("[boot] mqtt: gateway address not set -- run: mqtt <host> [port]");
  else
    Serial.printf("[boot] mqtt %s:%u (%s)\n", mqttHost.c_str(), mqttPort,
                  mqttFromNvs ? "NVS" : "config.h default");

#ifdef HAS_IR
  irsendRaw.begin();
  if (!SPIFFS.begin(true))   // format on first mount so IRDATA can land later
    Serial.println("[boot] SPIFFS mount failed — learned IR replay unavailable");
  // Readiness is one question now: did a learned bundle survive the reboot?
  irReady = SPIFFS.exists(IRDATA_PATH);
  Serial.printf("[boot] atom_ac firmware, remote=%s ir_ready=%d\n",
                acModel.isEmpty() ? "(none learned)" : acModel.c_str(), irReady);
#else
  irReady = false;   // base image cannot transmit IR regardless of NVS
  Serial.println("[boot] atom_base firmware (sensor + OTA only)");
#endif
  Serial.printf("[boot] store=%s dev=%d -> %s\n", STORE_ID, DEVICE_ID,
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

void loop() {
  pollSerial();
  ensureWifi();
  if (!ensureMqtt()) {
    delay(2000);
    return;
  }
  led(CRGB::Green);
  mqtt.loop();

  if (otaPending) performOta();   // may not return (reboots on success)
#ifdef HAS_IR
  if (irdataPending) performIrdata();
  pollLearn();
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
