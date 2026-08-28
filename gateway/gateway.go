package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IanKim2030/atom-air/gateway/protocol"
)

const (
	rawRetentionDays = 365
	ingestBuffer     = 4096
	outboxBatch      = 240
)

// minuteBucket accumulates one device-minute of samples.
type minuteBucket struct {
	temps, hums, lights []float64
}

func (b *minuteBucket) add(temp, hum, light float64) {
	b.temps = append(b.temps, temp)
	b.hums = append(b.hums, hum)
	b.lights = append(b.lights, light)
}

func (b *minuteBucket) finalize(devID uint8, ts int64) MinutePoint {
	tMin, tMax, tSum := b.temps[0], b.temps[0], 0.0
	for _, v := range b.temps {
		tSum += v
		tMin = min(tMin, v)
		tMax = max(tMax, v)
	}
	hSum, lSum := 0.0, 0.0
	for i := range b.hums {
		hSum += b.hums[i]
		lSum += b.lights[i]
	}
	n := float64(len(b.temps))
	return MinutePoint{
		DevID: devID, TS: ts,
		TempAvg: round3(tSum / n), TempMin: round3(tMin), TempMax: round3(tMax),
		HumAvg: round3(hSum / n), LightAvg: round1(lSum / n),
		SampleCount: len(b.temps),
	}
}

type bucketKey struct {
	devID uint8
	ts    int64
}

// Service wires the gateway together. Every loop below runs as its own
// goroutine, so toggling the live stream never interrupts local persistence or
// the licence checks.
type Service struct {
	cfg   Config
	store *Store
	lic   *LicenseManager
	ota   *OTAServer
	mqtt  *MQTTBridge
	cloud *CloudLink

	ingest chan []byte

	mu         sync.Mutex
	rawBuffer  []RawRow
	buckets    map[bucketKey]*minuteBucket
	acState    protocol.ACCommand
	irReady    map[uint8]bool
	irdataAcks map[uint8]irdataAck // last IRDATA ack per device

	liveStreaming atomic.Bool
	licensed      atomic.Bool

	packets  atomic.Int64
	bypassed atomic.Int64
	dropped  atomic.Int64
}

// irdataAck remembers a device's response to an IRDATA push, so the deploy
// goroutine can poll for it the same way SOTA polls for irReady.
type irdataAck struct {
	modelID string
	ok      bool
	when    time.Time
}

func NewService(cfg Config) *Service {
	return &Service{
		cfg:        cfg,
		ingest:     make(chan []byte, ingestBuffer),
		buckets:    make(map[bucketKey]*minuteBucket),
		irReady:    make(map[uint8]bool),
		irdataAcks: make(map[uint8]irdataAck),
		acState:    protocol.ACCommand{TargetID: 1, Power: 0, Mode: "cool", Temp: 24, Fan: "auto"},
	}
}

// Run blocks until ctx is cancelled, then shuts everything down cleanly.
func (s *Service) Run(ctx context.Context) error {
	store, err := OpenStore(s.cfg.DBPath)
	if err != nil {
		return fmt.Errorf("open local store: %w", err)
	}
	s.store = store
	defer s.store.Close()
	slog.Info("local sqlite (WAL) ready", "path", s.cfg.DBPath)

	s.lic = NewLicenseManager(s.cfg.StoreID, s.cfg.LicenseConfig,
		s.cfg.CloudHTTP, s.cfg.GraceDays)
	s.licensed.Store(true) // provisional until the first check lands

	s.ota = NewOTAServer(s.cfg.FirmwareDir, s.cfg.OTAPort)
	if err := s.ota.Start(); err != nil {
		// Not fatal: ingest and statistics must survive an OTA port clash.
		slog.Error("OTA HTTP server unavailable; SOTA deploys will fail",
			"port", s.cfg.OTAPort, "err", err)
	}
	defer s.ota.Stop()

	if s.cfg.NoMQTT {
		// Deliberately no broker. Without this, a simulator run on a machine
		// that happens to have Mosquitto up would mix synthetic frames with
		// whatever real devices are publishing.
		slog.Info("MQTT disabled (--no-mqtt); no broker will be contacted")
	} else {
		s.mqtt = NewMQTTBridge(s.cfg.MQTTHost, s.cfg.MQTTPort, s.cfg.StoreID,
			s.onFrame, s.onIREvent)
		s.mqtt.Start()
		defer s.mqtt.Stop()
	}
	if s.cfg.Simulate {
		slog.Info("simulator enabled: synthesising Atom traffic locally",
			"devices", s.cfg.Devices)
	}

	wsURL := fmt.Sprintf("%s/ws/gateway/%s?token=%s",
		strings.TrimRight(s.cfg.CloudWS, "/"), s.cfg.StoreID, s.cfg.Token)
	s.cloud = NewCloudLink(wsURL, s.handleCommand, s.onCloudUp)

	loops := []struct {
		name string
		fn   func(context.Context)
	}{
		{"ingest", s.loopIngest},
		{"persist", s.loopPersist},
		{"minute", s.loopMinute},
		{"retention", s.loopRetention},
		{"license", s.loopLicense},
		{"cloud", s.cloud.Run},
		{"heartbeat", s.loopHeartbeat},
	}
	if s.cfg.Simulate {
		loops = append(loops, struct {
			name string
			fn   func(context.Context)
		}{"simulate", s.loopSimulate})
	}

	var wg sync.WaitGroup
	for _, l := range loops {
		wg.Add(1)
		go func(name string, fn func(context.Context)) {
			defer wg.Done()
			fn(ctx)
			slog.Debug("loop stopped", "loop", name)
		}(l.name, l.fn)
	}

	slog.Info("gateway running -- normal mode (1-minute stats); waiting for START_LIVE_STREAM",
		"store", s.cfg.StoreID, "version", AppVersion)

	<-ctx.Done()
	wg.Wait()
	s.flushOnShutdown()
	slog.Info("gateway stopped")
	return nil
}

// onFrame is called from the MQTT goroutine and from the simulator. It never
// blocks: a full queue drops the frame rather than stalling the broker.
func (s *Service) onFrame(frame []byte) {
	select {
	case s.ingest <- frame:
	default:
		if n := s.dropped.Add(1); n%100 == 1 {
			slog.Warn("ingest queue full; dropping frames", "dropped_total", n)
		}
	}
}

// loopIngest decodes frames, always persists them locally, and bypasses them
// to the cloud only while the live stream is on.
func (s *Service) loopIngest(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case payload := <-s.ingest:
			for off := 0; off+protocol.SensorSize <= len(payload); off += protocol.SensorSize {
				frame := payload[off : off+protocol.SensorSize]
				reading, err := protocol.DecodeSensorPacket(frame)
				if err != nil {
					slog.Warn("bad sensor packet", "err", err)
					continue
				}
				s.packets.Add(1)
				now := time.Now().Unix()

				// (1) Local persistence -- unconditional.
				s.mu.Lock()
				if reading.IRReady {
					s.irReady[reading.DevID] = true
				}
				s.rawBuffer = append(s.rawBuffer, RawRow{
					TS: now, DevID: reading.DevID, Seq: reading.Seq,
					Temp: reading.Temp, Hum: reading.Hum,
					Light: reading.Light, Flags: reading.Flags,
				})
				key := bucketKey{reading.DevID, now / 60 * 60}
				b := s.buckets[key]
				if b == nil {
					b = &minuteBucket{}
					s.buckets[key] = b
				}
				b.add(reading.Temp, reading.Hum, float64(reading.Light))
				s.mu.Unlock()

				// (2) On-demand bypass -- the raw frame goes up untouched.
				if s.liveStreaming.Load() {
					if s.cloud.SendBinary(frame) {
						s.bypassed.Add(1)
					}
				}
			}
		}
	}
}

// loopPersist flushes the 1-second buffer to SQLite once a second. Always on.
func (s *Service) loopPersist(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			batch := s.rawBuffer
			s.rawBuffer = nil
			s.mu.Unlock()
			if len(batch) == 0 {
				continue
			}
			if _, err := s.store.InsertRaw(batch); err != nil {
				slog.Error("local raw insert failed", "rows_lost", len(batch), "err", err)
			}
		}
	}
}

// loopMinute closes finished minute buckets and queues them for the cloud.
func (s *Service) loopMinute(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := time.Now().Unix() / 60 * 60

			s.mu.Lock()
			var pts []MinutePoint
			for key, b := range s.buckets {
				if key.ts < current {
					pts = append(pts, b.finalize(key.devID, key.ts))
					delete(s.buckets, key)
				}
			}
			s.mu.Unlock()

			if len(pts) == 0 {
				continue
			}
			sort.Slice(pts, func(i, j int) bool { return pts[i].TS < pts[j].TS })
			if _, err := s.store.UpsertMinute(pts); err != nil {
				slog.Error("minute stat write failed", "err", err)
				continue
			}
			slog.Info("downsampled minute buckets",
				"count", len(pts), "live", s.liveStreaming.Load())
			s.flushOutbox()
		}
	}
}

// flushOutbox ships pending minute rows. Anything undelivered stays in SQLite.
func (s *Service) flushOutbox() {
	if !s.cloud.Connected() || !s.licensed.Load() {
		return
	}
	pending, err := s.store.Outbox(outboxBatch)
	if err != nil {
		slog.Error("could not read the outbox", "err", err)
		return
	}
	if len(pending) == 0 {
		return
	}
	if !s.cloud.SendJSON(map[string]any{"type": "minute_stats", "points": pending}) {
		slog.Warn("minute stat upload failed; rows stay queued", "rows", len(pending))
		return
	}
	if err := s.store.MarkUploaded(pending); err != nil {
		slog.Error("could not clear the outbox", "err", err)
		return
	}
	slog.Info("uploaded minute stat rows", "rows", len(pending))
}

func (s *Service) onCloudUp() {
	s.cloud.SendJSON(map[string]any{
		"type": "gateway_status",
		"info": map[string]any{
			"app_version": AppVersion,
			"mqtt":        s.mqtt.Connected(),
			"simulated":   s.cfg.Simulate,
			"licensed":    s.licensed.Load(),
			"runtime":     "go",
		},
	})
	s.flushOutbox() // drain whatever accumulated while the link was down
}

// loopRetention keeps a rolling year of 1-second data.
func (s *Service) loopRetention(ctx context.Context) {
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()
	for {
		if deleted, err := s.store.PurgeRaw(rawRetentionDays); err != nil {
			slog.Error("retention purge failed", "err", err)
		} else if deleted > 0 {
			slog.Info("retention purge", "rows", deleted, "older_than_days", rawRetentionDays)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// loopLicense runs the daily authorize call, independent of the cloud socket
// and of live mode.
func (s *Service) loopLicense(ctx context.Context) {
	for {
		v := s.lic.Check()
		was := s.licensed.Swap(v.Operational)
		if v.Operational {
			slog.Info("licence ok", "status", v.Reason,
				"days_remaining", v.DaysRemaining, "msg", v.Message)
		} else {
			slog.Error("licence not operational", "status", v.Reason, "msg", v.Message)
		}
		if was && !v.Operational {
			slog.Error("licence lapsed -- AC control and cloud upload are now blocked; " +
				"local logging continues")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.cfg.LicenseInterval):
		}
	}
}

func (s *Service) loopHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			counts, err := s.store.Counts()
			if err != nil {
				slog.Error("could not read storage counts", "err", err)
				continue
			}
			mode := "normal"
			if s.liveStreaming.Load() {
				mode = "LIVE"
			}
			slog.Info("status", "mode", mode,
				"packets", s.packets.Load(), "bypassed", s.bypassed.Load(),
				"dropped", s.dropped.Load()+s.cloud.Dropped(),
				"raw", counts.Raw, "minute", counts.Minute, "pending", counts.Pending,
				"mqtt", s.mqtt.Connected(), "licensed", s.licensed.Load())
		}
	}
}

func (s *Service) flushOnShutdown() {
	s.mu.Lock()
	batch := s.rawBuffer
	s.rawBuffer = nil
	s.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	if _, err := s.store.InsertRaw(batch); err != nil {
		slog.Error("could not flush buffered readings on shutdown", "err", err)
		return
	}
	slog.Info("flushed buffered readings on shutdown", "rows", len(batch))
}

// ---------------------------------------------------------------------------
// cloud commands
// ---------------------------------------------------------------------------

func (s *Service) handleCommand(cmd Command) {
	switch cmd.Cmd {
	case "START_LIVE_STREAM":
		if !s.liveStreaming.Swap(true) {
			slog.Info("START_LIVE_STREAM -> bypassing 1s packets to cloud",
				"viewers", cmd.Viewers)
		}

	case "STOP_LIVE_STREAM":
		if s.liveStreaming.Swap(false) {
			slog.Info("STOP_LIVE_STREAM -> back to normal mode (1-minute statistics only)")
		}

	case "AC_CONTROL":
		s.handleACControl(cmd)

	case "DEPLOY_FIRMWARE":
		go s.runSOTA(cmd) // long-running; must not block the reader

	case "DEPLOY_IRDATA":
		go s.runIRDeploy(cmd)

	case "LEARN_IR":
		s.handleLearn(cmd, false)

	case "LEARN_CANCEL":
		s.handleLearn(cmd, true)

	default:
		slog.Debug("unhandled cloud command", "cmd", cmd.Cmd)
	}
}

// handleLearn relays a learn start/cancel to the device over MQTT. A publish
// that cannot happen fails fast with a synthetic capture-failure, so the admin
// is not left staring at a 30-second timeout.
func (s *Service) handleLearn(cmd Command, cancel bool) {
	target := uint8(1)
	if cmd.TargetID > 0 && cmd.TargetID < 256 {
		target = uint8(cmd.TargetID)
	}
	action := "LEARN"
	if cancel {
		action = "LEARN_CANCEL"
	}
	if s.cfg.Simulate {
		if !cancel {
			go s.simulateCapture(cmd, target)
		}
		return
	}
	sent := s.mqtt.PublishLearn(target, map[string]any{
		"cmd": action, "session_id": cmd.SessionID,
		"slot": cmd.Slot, "timeout_s": cmd.TimeoutS,
	})
	if cancel {
		return // best-effort; the cloud has already marked the session canceled
	}
	if !sent {
		s.cloud.SendJSON(map[string]any{
			"type": "ir_capture", "dev_id": target,
			"session_id": cmd.SessionID, "slot": cmd.Slot,
			"ok": false, "error": "mqtt_offline",
		})
		slog.Warn("learn command could not be published", "dev", target)
		return
	}
	slog.Info("LEARN_IR relayed", "dev", target, "slot", cmd.Slot,
		"session", cmd.SessionID)
}

// simulateCapture stands in for a device with an IR receiver: a deterministic
// pseudo-signal derived from the slot name, two seconds after the request.
func (s *Service) simulateCapture(cmd Command, target uint8) {
	time.Sleep(2 * time.Second)
	seed := uint32(2166136261)
	for _, c := range []byte(s.cfg.StoreID + cmd.Slot) {
		seed = (seed ^ uint32(c)) * 16777619
	}
	raw := make([]int, 0, 200)
	raw = append(raw, 3400, 1700) // header burst
	for i := 0; i < 99; i++ {
		seed = seed*1664525 + 1013904223
		mark := 400 + int(seed%3)*50
		space := 450
		if seed&0x8000 != 0 {
			space = 1300
		}
		raw = append(raw, mark, space)
	}
	s.cloud.SendJSON(map[string]any{
		"type": "ir_capture", "dev_id": target,
		"session_id": cmd.SessionID, "slot": cmd.Slot,
		"ok": true, "freq_khz": 38, "len": len(raw), "raw": raw,
	})
	slog.Info("[simulate] synthesized IR capture", "dev", target, "slot", cmd.Slot)
}

// onIREvent handles JSON messages from devices on atom/{store}/ir/{dev}:
// learn captures are relayed to the cloud, IRDATA acks are recorded for the
// deploy goroutine to poll.
func (s *Service) onIREvent(devID uint8, payload []byte) {
	var evt struct {
		Type      string `json:"type"`
		SessionID string `json:"session_id"`
		Slot      string `json:"slot"`
		OK        bool   `json:"ok"`
		Error     string `json:"error"`
		FreqKHz   int    `json:"freq_khz"`
		Len       int    `json:"len"`
		Raw       []int  `json:"raw"`
		ModelID   string `json:"model_id"`
		Slots     int    `json:"slots"`
		Bytes     int64  `json:"bytes"`
	}
	if err := json.Unmarshal(payload, &evt); err != nil {
		slog.Warn("unparseable IR event", "dev", devID, "err", err)
		return
	}
	switch evt.Type {
	case "capture":
		s.cloud.SendJSON(map[string]any{
			"type": "ir_capture", "dev_id": devID,
			"session_id": evt.SessionID, "slot": evt.Slot,
			"ok": evt.OK, "error": evt.Error,
			"freq_khz": evt.FreqKHz, "len": evt.Len, "raw": evt.Raw,
		})
		slog.Info("IR capture relayed", "dev", devID, "slot", evt.Slot,
			"ok", evt.OK, "len", evt.Len)
	case "irdata_ack":
		s.mu.Lock()
		s.irdataAcks[devID] = irdataAck{modelID: evt.ModelID, ok: evt.OK, when: time.Now()}
		if evt.OK {
			s.irReady[devID] = true
		}
		s.mu.Unlock()
		slog.Info("IRDATA ack", "dev", devID, "model", evt.ModelID, "ok", evt.OK)
	default:
		slog.Debug("unhandled IR event", "dev", devID, "type", evt.Type)
	}
}

func (s *Service) handleACControl(cmd Command) {
	packet, err := hex.DecodeString(cmd.PacketHex)
	if err != nil {
		s.ackAC(false, "제어 패킷을 해석할 수 없습니다: "+err.Error(), nil)
		return
	}
	decoded, err := protocol.DecodeACPacket(packet)
	if err != nil {
		slog.Warn("rejecting AC command", "err", err)
		s.ackAC(false, err.Error(), nil)
		return
	}
	if !s.licensed.Load() {
		s.ackAC(false, "라이선스 유예기간 만료로 제어가 차단되었습니다.", nil)
		return
	}

	slog.Info("AC_CONTROL", "dev", decoded.TargetID, "power", decoded.Power,
		"mode", decoded.Mode, "temp", decoded.Temp, "fan", decoded.Fan,
		"packet", cmd.PacketHex)

	s.mu.Lock()
	ready := s.irReady[decoded.TargetID]
	s.mu.Unlock()
	if !ready {
		slog.Warn("device has not reported IR-capable firmware; sending anyway "+
			"(run SOTA if it does not respond)", "dev", decoded.TargetID)
	}

	if !s.mqtt.PublishAC(decoded.TargetID, packet) && !s.cfg.Simulate {
		s.ackAC(false, "MQTT 브로커에 연결되어 있지 않습니다.", nil)
		return
	}

	s.mu.Lock()
	s.acState = decoded
	s.mu.Unlock()
	s.ackAC(true, "IR 명령을 전송했습니다.", &decoded)
}

func (s *Service) ackAC(ok bool, message string, state *protocol.ACCommand) {
	msg := map[string]any{"type": "ac_ack", "ok": ok, "message": message}
	if state != nil {
		msg["state"] = state
	}
	s.cloud.SendJSON(msg)
}

// flashFirmware stages the protocol-named image, tells the device to flash it
// and walks the prepare/serve/notify/flashing stages. Shared by the protocol
// SOTA and the bare-device leg of an IR-data deploy. Returns false once a
// failure has been reported.
func (s *Service) flashFirmware(target uint8, model, proto string,
	report func(stage string, percent int, message string, ok bool)) bool {

	report("prepare", 10, fmt.Sprintf("%s (%s) 펌웨어를 준비합니다.", model, proto), true)

	binary := filepath.Join(s.cfg.FirmwareDir,
		fmt.Sprintf("atom_ac_%s.bin", strings.ToLower(proto)))
	info, err := os.Stat(binary)
	if err != nil {
		if !s.cfg.Simulate {
			report("failed", 10, "펌웨어 파일이 없습니다: "+binary, false)
			return false
		}
		// Simulation only: stand in for the real IRremoteESP8266 build output.
		if err := os.MkdirAll(s.cfg.FirmwareDir, 0o755); err != nil {
			report("failed", 10, "펌웨어 디렉터리를 만들 수 없습니다: "+err.Error(), false)
			return false
		}
		stub := []byte(strings.Repeat("ATOMAIR-SIMULATED-FIRMWARE\x00", 4096))
		if err := os.WriteFile(binary, stub, 0o644); err != nil {
			report("failed", 10, "임시 펌웨어를 쓸 수 없습니다: "+err.Error(), false)
			return false
		}
		info, _ = os.Stat(binary)
		report("prepare", 20,
			"[시뮬레이션] 임시 펌웨어 이미지를 생성했습니다: "+filepath.Base(binary), true)
	}

	url := fmt.Sprintf("http://%s:%d/%s", s.cfg.OTAHost, s.cfg.OTAPort, filepath.Base(binary))
	report("serve", 35,
		fmt.Sprintf("로컬 OTA 서버에서 배포 중 (%dKB) - %s", info.Size()/1024, url), true)

	sent := s.mqtt.PublishOTA(target, map[string]any{
		"cmd": "OTA", "url": url, "protocol": proto, "model": model, "size": info.Size(),
	})
	if !sent && !s.cfg.Simulate {
		report("failed", 35, "MQTT 브로커에 연결되어 있지 않아 OTA를 지시할 수 없습니다.", false)
		return false
	}
	report("notify", 50, fmt.Sprintf("디바이스 %d에 OTA 명령을 전달했습니다.", target), true)

	// Rule 4 budgets ~5s for the LAN flash; poll for the device to come back.
	for _, pct := range []int{65, 80, 90} {
		time.Sleep(1600 * time.Millisecond)
		report("flashing", pct, "디바이스가 펌웨어를 내려받아 기록하는 중입니다...", true)
	}
	return true
}

// runSOTA stages the AC-capable firmware and drives the local fast HTTP OTA.
func (s *Service) runSOTA(cmd Command) {
	target := uint8(1)
	if cmd.TargetID > 0 && cmd.TargetID < 256 {
		target = uint8(cmd.TargetID)
	}
	model := firstNonEmpty(cmd.Model, cmd.ModelID, "unknown")
	proto := firstNonEmpty(cmd.Protocol, "GENERIC")

	report := func(stage string, percent int, message string, ok bool) {
		s.cloud.SendJSON(map[string]any{
			"type": "sota_progress", "stage": stage, "percent": percent,
			"message": message, "model": model, "ok": ok,
		})
		slog.Info("SOTA", "stage", stage, "percent", percent, "msg", message)
	}

	if !s.flashFirmware(target, model, proto, report) {
		return
	}

	if s.cfg.Simulate {
		s.mu.Lock()
		s.irReady[target] = true
		s.mu.Unlock()
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		ready := s.irReady[target]
		s.mu.Unlock()
		if ready {
			report("done", 100,
				fmt.Sprintf("완료. 디바이스 %d에서 AC IR 제어를 사용할 수 있습니다.", target), true)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	report("verify", 95,
		"플래싱은 지시했으나 디바이스가 아직 IR 준비 상태를 보고하지 않았습니다.", false)
}

// modelIDPattern matches the slugs the cloud generates; anything else must not
// end up in a served filename or URL.
var modelIDPattern = regexp.MustCompile(`^[a-z0-9_-]+$`)

// runIRDeploy ships a learned raw-IR code bundle to one device. A device still
// on the base firmware is flashed with the universal raw-capable image first,
// then the bundle rides the same OTA HTTP server as a data file.
func (s *Service) runIRDeploy(cmd Command) {
	target := uint8(1)
	if cmd.TargetID > 0 && cmd.TargetID < 256 {
		target = uint8(cmd.TargetID)
	}
	model := firstNonEmpty(cmd.Model, cmd.ModelID, "unknown")

	report := func(stage string, percent int, message string, ok bool) {
		s.cloud.SendJSON(map[string]any{
			"type": "sota_progress", "stage": stage, "percent": percent,
			"message": message, "model": model, "ok": ok,
		})
		slog.Info("IRDATA", "stage", stage, "percent", percent, "msg", message)
	}

	if !modelIDPattern.MatchString(cmd.ModelID) {
		report("failed", 5, "잘못된 model_id입니다: "+cmd.ModelID, false)
		return
	}
	var bundle struct {
		Slots map[string]json.RawMessage `json:"slots"`
	}
	if err := json.Unmarshal(cmd.Bundle, &bundle); err != nil || len(bundle.Slots) == 0 {
		report("failed", 5, "학습 데이터 번들이 비어 있습니다.", false)
		return
	}

	// A device that has never reported IR-ready is still on the base image:
	// give it the raw-capable universal firmware before the data file.
	s.mu.Lock()
	ready := s.irReady[target]
	s.mu.Unlock()
	if !ready {
		slog.Info("device not IR-ready; flashing raw-capable firmware first",
			"dev", target)
		if !s.flashFirmware(target, model, "RAW", report) {
			return
		}
		if s.cfg.Simulate {
			s.mu.Lock()
			s.irReady[target] = true
			s.mu.Unlock()
		}
	}

	report("prepare", 92, fmt.Sprintf("학습된 리모컨 데이터를 준비합니다 (%d개 조합).",
		len(bundle.Slots)), true)
	if err := os.MkdirAll(s.cfg.FirmwareDir, 0o755); err != nil {
		report("failed", 92, "펌웨어 디렉터리를 만들 수 없습니다: "+err.Error(), false)
		return
	}
	filename := "ir_" + cmd.ModelID + ".json"
	path := filepath.Join(s.cfg.FirmwareDir, filename)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, cmd.Bundle, 0o644); err != nil {
		report("failed", 92, "학습 데이터를 저장할 수 없습니다: "+err.Error(), false)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		report("failed", 92, "학습 데이터를 저장할 수 없습니다: "+err.Error(), false)
		return
	}
	info, _ := os.Stat(path)

	url := fmt.Sprintf("http://%s:%d/%s", s.cfg.OTAHost, s.cfg.OTAPort, filename)
	report("serve", 94,
		fmt.Sprintf("로컬 OTA 서버에서 데이터 배포 중 (%dKB) - %s", info.Size()/1024, url), true)

	s.mu.Lock()
	delete(s.irdataAcks, target) // a stale ack must not satisfy this deploy
	s.mu.Unlock()

	irdataCmd := map[string]any{
		"cmd": "IRDATA", "url": url, "model_id": cmd.ModelID, "model": model,
		"size": info.Size(), "slots": len(bundle.Slots),
	}
	if !s.mqtt.PublishOTA(target, irdataCmd) && !s.cfg.Simulate {
		report("failed", 94, "MQTT 브로커에 연결되어 있지 않아 전송할 수 없습니다.", false)
		return
	}
	report("notify", 96, fmt.Sprintf("디바이스 %d에 데이터 전송을 지시했습니다.", target), true)

	if s.cfg.Simulate {
		s.mu.Lock()
		s.irdataAcks[target] = irdataAck{modelID: cmd.ModelID, ok: true, when: time.Now()}
		s.irReady[target] = true
		s.mu.Unlock()
	}
	// A freshly flashed device may still be rebooting when the first IRDATA
	// message goes out (clean-session MQTT drops it), so re-publish every few
	// seconds until the ack lands.
	deadline := time.Now().Add(30 * time.Second)
	lastPublish := time.Now()
	for time.Now().Before(deadline) {
		s.mu.Lock()
		ack, has := s.irdataAcks[target]
		s.mu.Unlock()
		if has && ack.modelID == cmd.ModelID {
			if ack.ok {
				report("done", 100, fmt.Sprintf(
					"완료. 디바이스 %d에 리모컨 데이터가 탑재되었습니다 (%d개 조합).",
					target, len(bundle.Slots)), true)
			} else {
				report("failed", 96, "디바이스가 데이터 저장 실패를 보고했습니다.", false)
			}
			return
		}
		if time.Since(lastPublish) > 5*time.Second {
			lastPublish = time.Now()
			s.mqtt.PublishOTA(target, irdataCmd)
		}
		time.Sleep(500 * time.Millisecond)
	}
	report("verify", 95,
		"데이터 전송은 지시했으나 디바이스가 아직 수신을 확인하지 않았습니다.", false)
}
