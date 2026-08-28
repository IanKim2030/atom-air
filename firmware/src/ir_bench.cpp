// Atom Air — IR 학습·송신 벤치 (standalone).
//
// The other half of this project's src/: main.cpp is the store firmware, this
// is a bench tool. It joins nothing, publishes nothing and stores nothing —
// point a remote at G25, read the decode on USB serial, press the ATOM's front
// button to fire it back. That is the whole program, which is the point: when
// the store firmware's receive path looks wrong, this tells you whether the
// hardware or the plumbing is at fault.
//
//   pio run -e atom_irbench -t upload
//   pio device monitor
//
// The resend is a loopback check, not a control feature: firing a decoded code
// back proves the transmit path and the LED both work, on a remote simple
// enough to decode (NEC-style — TV, fan, light). Air conditioner frames decode
// as UNKNOWN and are not resendable this way, which is expected here and is why
// the store firmware replays raw timings instead.
//
// The RGB LED is driven with neopixelWrite() rather than FastLED so nothing
// contends with IRremoteESP8266 for an RMT timer.

#include <Arduino.h>
#include <IRremoteESP8266.h>
#include <IRrecv.h>
#include <IRsend.h>
#include <IRutils.h>

// 핀 설정
const uint16_t RECV_PIN = 25; // 외장 IR 수신 핀 (G25)
const uint16_t SEND_PIN = 12; // ATOM Lite 내장 IR 송신 핀 (G12)
const uint8_t  BTN_PIN  = 39; // ATOM Lite 전면 버튼 (G39)
const uint8_t  LED_PIN  = 27; // ATOM Lite 내장 RGB LED (G27)

// 1024 timings, not the library's default 100: an air conditioner frame runs
// 200 and up, and a frame that overflows the buffer is truncated, fails to
// decode, and arrives as UNKNOWN. 50ms (default 15) closes the gap between an
// AC frame's two halves so they arrive as one capture.
IRrecv irrecv(RECV_PIN, 1024, 50, true);
IRsend irsend(SEND_PIN);
decode_results results;

// 저장 변수
decode_type_t lastProtocol = UNKNOWN;
uint64_t lastValue = 0;
uint16_t lastBits = 0;

// LED 색상 제어 함수 (FastLED 충돌 방지용 순수 ESP32 함수)
void setLedColor(uint8_t r, uint8_t g, uint8_t b) {
  neopixelWrite(LED_PIN, r, g, b);
}

// Prints everything the decoder produced. value/address/command and state[]
// share one union — they are mutually exclusive — so which half is real depends
// on the protocol, and printing the wrong one prints neighbouring bytes.
void dumpResult(const decode_results *r) {
  Serial.println("\n[신호 수신]");
  Serial.print(" - 프로토콜 : "); Serial.println(typeToString(r->decode_type, r->repeat));
  Serial.print(" - 비트 수  : "); Serial.println(r->bits);
  if (r->overflow)
    Serial.println(" - !! 버퍼 넘침: 프레임이 잘렸습니다 (버퍼를 더 키우세요)");
  if (r->repeat)
    Serial.println(" - 반복 코드 (버튼을 누르고 있는 중)");

  if (hasACState(r->decode_type)) {
    // 에어컨류: 데이터가 state[] 바이트 배열에 담깁니다. 이때 address/command는
    // 같은 메모리라 의미가 없습니다.
    Serial.print(" - STATE    : 0x"); Serial.println(resultToHexidecimal(r));
    Serial.println(" - (에어컨 프레임: 채널/커맨드 필드 없음, 상태 전체가 바이트열)");
  } else {
    Serial.print(" - HEX 코드 : 0x"); serialPrintUint64(r->value, HEX); Serial.println();
    Serial.print(" - 채널(주소): 0x"); Serial.println(r->address, HEX);
    Serial.print(" - 커맨드   : 0x"); Serial.println(r->command, HEX);
  }

  // The timings themselves, twelve per line so mark/space pairs stay in
  // columns: header burst first, then the bit stream.
  uint16_t len = getCorrectedRawLength(r);
  uint16_t *raw = resultToRawArray(r);
  uint32_t total = 0;
  for (uint16_t i = 0; i < len; i++) total += raw[i];
  Serial.printf(" - RAW      : %u개, %lu us\n", len, (unsigned long)total);
  for (uint16_t i = 0; i < len; i++) {
    if (i % 12 == 0) Serial.printf("   %4u: ", i);
    Serial.printf("%u%s", raw[i], (i + 1 == len) ? "\n" : ",");
    if ((i + 1) % 12 == 0 && i + 1 != len) Serial.println();
  }
  delete[] raw;
}

void setup() {
  Serial.begin(115200);
  pinMode(BTN_PIN, INPUT_PULLUP);

  // 대기 상태 (파란색)
  setLedColor(0, 0, 50);

  // IR 초기화 (각각 딱 1회만 호출)
  irsend.begin();
  irrecv.enableIRIn();

  Serial.println("\n==========================================");
  Serial.println(">> [준비 완료] 타이머 충돌 해결됨");
  Serial.println("1. 리모컨을 눌러 신호를 학습(수신)하세요.");
  Serial.println("2. ATOM 본체 버튼을 누르면 송신합니다.");
  Serial.println("==========================================");
}

void loop() {
  // 1. IR 신호 수신 — 무엇이 잡히든 전부 출력합니다. 예전에는 value가 0이면
  //    통째로 건너뛰었는데, 에어컨은 value가 항상 0이라 화면에 아무것도 남지
  //    않았습니다.
  if (irrecv.decode(&results)) {
    dumpResult(&results);
    setLedColor(0, 50, 0); // 수신 성공: 초록색

    // 재송신은 protocol+value 경로라 value가 있는 리모컨에만 해당합니다.
    if (results.value != 0xFFFFFFFFFFFFFFFF && results.value != 0 &&
        !hasACState(results.decode_type)) {
      lastProtocol = results.decode_type;
      lastValue = results.value;
      lastBits = results.bits;
      Serial.println(">> ATOM 버튼을 누르면 이 신호를 발사합니다.");
    } else {
      Serial.println(">> 재송신 불가 (protocol+value로 복원되지 않는 신호). "
                     "위 RAW 값이 이 리모컨의 실제 데이터입니다.");
    }

    delay(300);
    setLedColor(0, 0, 50); // 다시 파란색으로 복귀
    irrecv.resume(); // 다음 신호 수신 대기
  }

  // 2. ATOM Lite 전면 버튼 눌림 감지 -> IR 송신
  if (digitalRead(BTN_PIN) == LOW) {
    delay(50); // 디바운스
    if (digitalRead(BTN_PIN) == LOW) {
      if (lastValue == 0) {
        Serial.println("\n[송신할 신호가 없습니다 — 먼저 리모컨을 수신하세요]");
      } else {
        setLedColor(50, 0, 0); // 송신 중: 빨간색

        Serial.println("\n[🚀 IR 신호 송신 중...]");

        // 송신 중에는 수신기를 잠시 중단하여 충돌 방지
        irrecv.pause();

        // 저장된 프로토콜로 신호 발사
        bool sent = irsend.send(lastProtocol, lastValue, lastBits);

        delay(100);
        irrecv.resume(); // 송신 완료 후 수신 재개

        setLedColor(0, 0, 50); // 대기 (파란색)
        Serial.println(sent ? "[✔ 송신 완료!]"
                            : "[✖ 이 프로토콜은 send()가 지원하지 않습니다]");
      }
    }
    while (digitalRead(BTN_PIN) == LOW); // 버튼 뗄 때까지 대기
  }
}
