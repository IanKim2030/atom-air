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
// Two things it deliberately does not do. It resends by protocol+value, so it
// handles NEC-style remotes (TV, fan, light) and not air conditioners, whose
// full-state frames decode as UNKNOWN — that is exactly why the store firmware
// replays raw timings instead. And it drives the RGB LED with neopixelWrite()
// rather than FastLED, so it cannot fight IRremoteESP8266 over an RMT timer.

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

IRrecv irrecv(RECV_PIN);
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
  // 1. IR 신호 수신
  if (irrecv.decode(&results)) {
    // 유효한 신호인 경우만 저장 (0xFFFFFFFF 반복 신호 제외)
    if (results.value != 0xFFFFFFFFFFFFFFFF && results.value != 0) {
      lastProtocol = results.decode_type;
      lastValue = results.value;
      lastBits = results.bits;

      setLedColor(0, 50, 0); // 수신 성공: 초록색

      Serial.println("\n[📡 신호 학습 완료!]");
      Serial.print(" - 프로토콜: "); Serial.println(typeToString(lastProtocol));
      Serial.print(" - HEX 코드: 0x"); serialPrintUint64(lastValue, HEX); Serial.println();
      Serial.print(" - 비트 수 : "); Serial.println(lastBits);
      Serial.println(">> ATOM 버튼을 누르면 이 신호를 발사합니다.");

      delay(300);
      setLedColor(0, 0, 50); // 다시 파란색으로 복귀
    }

    irrecv.resume(); // 다음 신호 수신 대기
  }

  // 2. ATOM Lite 전면 버튼 눌림 감지 -> IR 송신
  if (digitalRead(BTN_PIN) == LOW) {
    delay(50); // 디바운스
    if (digitalRead(BTN_PIN) == LOW && lastValue != 0) {
      setLedColor(50, 0, 0); // 송신 중: 빨간색

      Serial.println("\n[🚀 IR 신호 송신 중...]");

      // 송신 중에는 수신기를 잠시 중단하여 충돌 방지
      irrecv.pause();

      // 저장된 프로토콜로 신호 발사
      irsend.send(lastProtocol, lastValue, lastBits);

      delay(100);
      irrecv.resume(); // 송신 완료 후 수신 재개

      setLedColor(0, 0, 50); // 대기 (파란색)
      Serial.println("[✔ 송신 완료!]");
    }
    while (digitalRead(BTN_PIN) == LOW); // 버튼 뗄 때까지 대기
  }
}
