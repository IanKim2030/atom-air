# PC방 PC 전원 감지 → 에어컨 자동 OFF

룸(room) 단위로 PC 여러 대의 전원 상태를 모아, 룸의 PC가 모두 꺼지면 그 룸의
에어컨을 자동으로 끈다. 클라우드 DB는 관여하지 않는다 — 룸/PC/에어컨 매핑과
자동 OFF 판단은 전부 매장 게이트웨이(`gateway/`) 안에서만 이뤄진다.

```
PC 1..5 (pcagent 서비스) --MQTT(로컬 Mosquitto)--> atomair-gateway --MQTT--> Atom Lite(에어컨)
```

**설계 원칙: PC Agent는 MQTT 서버 주소 외에는 아무것도 몰라도 된다.** 어느 룸에
속하는지, store_id가 무엇인지, 자기 이름이 무엇인지조차 알 필요가 없다 — 이
매핑은 전부 게이트웨이의 `room_config.json`에서만 관리한다. PC방은 대개 이미지
복제(Ghost 등)로 설치되어 컴퓨터 이름이 겹치는 경우가 흔하므로, 식별자로는
컴퓨터 이름 대신 **MAC 주소**(NIC 고유값)를 자동 감지해 사용한다.

## MQTT 토픽 & 페이로드

```
atom/pcagent/status/{key}   (retained, QoS 1)
{"status": "on" | "off", "ts": <unix seconds>}
```

- `{key}`는 `pcagent`가 자동 감지한 자신의 MAC 주소다(`--pc-key`로 수동 지정도
  가능 — 로컬 테스트처럼 실제 MAC이 여러 개 필요할 때 사용). **store_id도 room_id도
  토픽에 없다** — 로컬 Mosquitto 브로커 하나는 항상 매장 하나에 속하므로 store_id가
  필요 없고, 어느 룸인지는 게이트웨이가 `room_config.json`에서 찾는다.
- `pcagent`는 접속 시 이 토픽에 **Last Will**을 등록한다(retained `{"status":"off",...}`).
  전원이 꺼지거나 네트워크가 끊기는 등 비정상 종료 시 브로커가 대신 발행한다.
- 접속 성공 직후 `{"status":"on",...}`을 즉시 발행하고, 이후 `--heartbeat`
  간격(기본 30초)마다 재발행한다.
- 서비스가 정상적으로 멈출 때(SCM stop/Ctrl+C)는 종료 직전 `off`를 직접 발행한다.

## 게이트웨이 설정: `room_config.json`

`<data-dir>/room_config.json` (기본값, `store_license_config.json`과 같은 위치).
파일이 없으면 자동 OFF 기능은 비활성 상태로 남는다(경고 로그만 남기고 정상 동작).

```json
{
  "auto_off_defaults": { "debounce_minutes": 5, "retry_interval_minutes": 2, "max_retries": 3 },
  "rooms": [
    {
      "room_id": "room1",
      "ac_dev_ids": [1],
      "pcs": [
        { "pc_id": "pc1", "mac": "aa:bb:cc:dd:ee:01" },
        { "pc_id": "pc2", "mac": "aa:bb:cc:dd:ee:02" },
        { "pc_id": "pc3", "mac": "aa:bb:cc:dd:ee:03" },
        { "pc_id": "pc4", "mac": "aa:bb:cc:dd:ee:04" },
        { "pc_id": "pc5", "mac": "aa:bb:cc:dd:ee:05" }
      ]
    }
  ]
}
```

- `mac`은 대소문자/구분자(`:`, `-`, 공백)를 가리지 않고 비교한다 — `ipconfig /all`이
  보여주는 `AA-BB-CC-DD-EE-01` 형식을 그대로 붙여넣어도 된다.
- `pc_id`는 로그와 이 파일 안에서만 쓰이는 사람이 읽기 위한 이름이다. pcagent는
  이 값을 전혀 모른다.
- 한 번도 상태를 보고한 적 없는(또는 아직 `room_config.json`에 등록되지 않은) PC는
  **항상 켜진 것으로 간주**한다 — 설치가 덜 끝났거나 네트워크 장애로 룸이 비어
  보이는 오탐을 막기 위한 안전장치다.
- 룸별로 `"auto_off": {...}`를 추가하면 그 룸만 기본값을 덮어쓸 수 있다.

## PC 등록 흐름

1. 15대 PC 전부에 **동일한** 명령으로 `pcagent`를 설치한다(어느 룸인지 신경 쓸 필요 없음):
   ```
   atomair-pcagent install --mqtt-host=<게이트웨이 PC IP>
   atomair-pcagent start
   ```
2. 에이전트는 시작 시 자신의 MAC 주소(`pc_key`)를 로그에 남긴다:
   `pcagent starting pc_key=aa:bb:cc:dd:ee:01 ...`
3. 관리자가 각 PC의 물리적 위치(어느 룸인지)를 확인해 그 MAC을 게이트웨이의
   `room_config.json`에 채워 넣는다.
4. `room_config.json`에 없는 MAC은 게이트웨이가 "미등록 PC" 경고를 한 번만
   남기고 무시한다 — room_config.json에 추가되는 순간부터(게이트웨이 재시작
   없이, 다음 heartbeat 수신 시) 자동으로 인식된다.

## 자동 OFF 알고리즘 (`gateway/pcstatus.go`)

1. 룸에 등록된 PC(`pcs`)가 전부 "off"로 확인되면 `debounce_minutes` 타이머를 건다
   (윈도우 재부팅 등으로 짧게 꺼졌다 켜지는 상황에서 오탐을 막기 위함). 그사이
   PC 하나라도 "on"이 되면 타이머를 취소한다.
2. 타이머가 만료되면 다시 한 번 "모두 off"인지 확인한 뒤, 룸의 `ac_dev_ids` 전체에
   전원 OFF IR 명령을 보낸다(마지막으로 알려진 mode/temp/fan을 재사용, 없으면
   `cool/24/auto` 기본값).
3. `retry_interval_minutes` 후 재확인한다 — 기기가 스스로 보고하는 센서 플래그
   (`FlagACOn`, `protocol/protocol.go`)가 여전히 "on"이면 다시 전송하고, 그사이
   룸이 다시 켜졌으면 재시도를 중단한다. `max_retries` 소진 시 포기하고 로그만 남긴다.
4. 모든 이벤트는 클라우드로 `device_log` 메시지로 전달되어 기존 대시보드의 기기별
   로그 패널에 그대로 노출된다(클라우드 쪽 스키마/코드 변경 없음).
