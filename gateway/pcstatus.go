package main

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// RoomOffSender is the Service surface PCStatusTracker needs: publish an
// AC-off command, read back what the device last reported about its own AC
// state, and tell the cloud dashboard what happened. Kept as an interface so
// the debounce/retry state machine can be reasoned about (and tested)
// without a full Service.
type RoomOffSender interface {
	SendACOff(devID uint8) bool
	ACOnReported(devID uint8) (on bool, known bool)
	NotifyRoomEvent(repDevID uint8, line string)
}

// roomState is the live tracking for one configured room: which of its PCs
// have reported in and what they last said, plus any pending debounce/retry
// timer.
type roomState struct {
	pcOn  map[string]bool // only PCs that have reported at least once
	timer *time.Timer
}

// PCStatusTracker turns per-PC on/off heartbeats into room-level AC-off
// decisions. A PC that has never reported counts as "on" (fail safe): an
// agent that is not installed yet, or a network hiccup, must never look like
// an empty room.
type PCStatusTracker struct {
	cfg *RoomConfig
	out RoomOffSender

	mu     sync.Mutex
	rooms  map[string]*roomState
	warned map[string]bool
}

func NewPCStatusTracker(cfg *RoomConfig, out RoomOffSender) *PCStatusTracker {
	return &PCStatusTracker{
		cfg:    cfg,
		out:    out,
		rooms:  make(map[string]*roomState),
		warned: make(map[string]bool),
	}
}

// OnStatus records one PC's reported power state and re-evaluates its room.
func (t *PCStatusTracker) OnStatus(roomID, pcID string, on bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	room, found := t.cfg.find(roomID)
	if !found {
		if !t.warned[roomID] {
			t.warned[roomID] = true
			slog.Warn("pc status for unconfigured room; ignoring", "room", roomID, "pc", pcID)
		}
		return
	}

	rs := t.rooms[roomID]
	if rs == nil {
		rs = &roomState{pcOn: make(map[string]bool)}
		t.rooms[roomID] = rs
	}
	rs.pcOn[pcID] = on
	slog.Info("pc status", "room", roomID, "pc", pcID, "on", on)

	t.evaluateLocked(room, rs)
}

// evaluateLocked starts, leaves running, or cancels the room's debounce/retry
// timer based on its current all-off state. Callers must hold t.mu.
func (t *PCStatusTracker) evaluateLocked(room RoomEntry, rs *roomState) {
	if t.allOffLocked(room, rs) {
		if rs.timer == nil {
			ao := room.autoOff(t.cfg.Defaults)
			delay := time.Duration(ao.DebounceMinutes * float64(time.Minute))
			slog.Info("room fully powered off; arming auto-off", "room", room.RoomID,
				"debounce", delay)
			roomID := room.RoomID
			rs.timer = time.AfterFunc(delay, func() { t.fireDebounce(roomID) })
		}
		return
	}
	if rs.timer != nil {
		rs.timer.Stop()
		rs.timer = nil
		slog.Info("room no longer fully off; auto-off cancelled", "room", room.RoomID)
	}
}

// allOffLocked reports whether every PC configured for this room has
// reported in and is currently off. Callers must hold t.mu.
func (t *PCStatusTracker) allOffLocked(room RoomEntry, rs *roomState) bool {
	for _, pc := range room.PCs {
		on, seen := rs.pcOn[pc.PCID]
		if !seen || on {
			return false
		}
	}
	return len(room.PCs) > 0
}

// fireDebounce is the debounce timer callback: it re-checks the room (a PC
// may have come back on while waiting) and, if it is still empty, starts the
// off/retry sequence.
func (t *PCStatusTracker) fireDebounce(roomID string) {
	t.mu.Lock()
	room, found := t.cfg.find(roomID)
	rs := t.rooms[roomID]
	if !found || rs == nil {
		t.mu.Unlock()
		return
	}
	if !t.allOffLocked(room, rs) {
		rs.timer = nil
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()
	t.attemptOff(roomID, 0)
}

// attemptOff sends the off command for every AC device serving the room,
// reports the event, and -- unless this was the last allowed attempt --
// schedules a recheck one retry interval later.
func (t *PCStatusTracker) attemptOff(roomID string, attempt int) {
	t.mu.Lock()
	room, found := t.cfg.find(roomID)
	rs := t.rooms[roomID]
	if !found || rs == nil {
		t.mu.Unlock()
		return
	}
	ao := room.autoOff(t.cfg.Defaults)
	t.mu.Unlock()

	for _, dev := range room.ACDevIDs {
		t.out.SendACOff(dev)
	}
	rep := uint8(0)
	if len(room.ACDevIDs) > 0 {
		rep = room.ACDevIDs[0]
	}
	t.out.NotifyRoomEvent(rep, fmt.Sprintf(
		"룸 %s: PC %d대 모두 꺼짐 확인 -> 에어컨 OFF 전송 (시도 %d/%d)",
		roomID, len(room.PCs), attempt+1, ao.MaxRetries+1))

	if attempt >= ao.MaxRetries {
		t.mu.Lock()
		if rs2 := t.rooms[roomID]; rs2 != nil {
			rs2.timer = nil
		}
		t.mu.Unlock()
		return
	}

	interval := time.Duration(ao.RetryIntervalMinutes * float64(time.Minute))
	t.mu.Lock()
	if rs2 := t.rooms[roomID]; rs2 != nil {
		rs2.timer = time.AfterFunc(interval, func() { t.recheckAndRetry(roomID, attempt+1) })
	}
	t.mu.Unlock()
}

// recheckAndRetry runs one retry interval after an off attempt. It gives up
// quietly if the room is occupied again, or if every AC the room depends on
// has confirmed (via its own sensor flag) that it is off; otherwise it sends
// the off command again.
func (t *PCStatusTracker) recheckAndRetry(roomID string, attempt int) {
	t.mu.Lock()
	room, found := t.cfg.find(roomID)
	rs := t.rooms[roomID]
	if !found || rs == nil {
		t.mu.Unlock()
		return
	}
	if !t.allOffLocked(room, rs) {
		rs.timer = nil
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()

	allConfirmedOff := true
	for _, dev := range room.ACDevIDs {
		if on, known := t.out.ACOnReported(dev); known && on {
			allConfirmedOff = false
		}
	}
	if allConfirmedOff {
		t.mu.Lock()
		if rs2 := t.rooms[roomID]; rs2 != nil {
			rs2.timer = nil
		}
		t.mu.Unlock()
		slog.Info("room AC confirmed off; no further retries", "room", roomID)
		return
	}

	t.attemptOff(roomID, attempt)
}
