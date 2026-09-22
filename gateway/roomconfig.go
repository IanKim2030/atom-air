package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// AutoOffConfig tunes how long a room must sit fully powered-down before the
// gateway turns its AC off, and how hard it retries if the IR command does
// not seem to have landed.
type AutoOffConfig struct {
	DebounceMinutes      float64 `json:"debounce_minutes"`
	RetryIntervalMinutes float64 `json:"retry_interval_minutes"`
	MaxRetries           int     `json:"max_retries"`
}

// PCEntry names one PC expected in a room. Key is how the pcagent running on
// that PC identifies itself on MQTT -- normally its MAC address, since PC방
// machines are usually cloned from one image and cannot be told apart by
// hostname. PCID is only a human-readable label for logs and room_config.json
// itself; it never has to match anything the agent knows.
type PCEntry struct {
	PCID string `json:"pc_id"`
	Key  string `json:"mac"`
}

// RoomEntry maps one PC방 room to the PCs expected inside it and the AC
// device(s) that serve it. AutoOff is nil unless the room overrides the
// file-level defaults.
type RoomEntry struct {
	RoomID   string         `json:"room_id"`
	ACDevIDs []uint8        `json:"ac_dev_ids"`
	PCs      []PCEntry      `json:"pcs"`
	AutoOff  *AutoOffConfig `json:"auto_off,omitempty"`
}

// RoomConfig is the gateway-local room/PC/AC map, loaded from
// room_config.json. There is no cloud-side equivalent, and the pcagent itself
// carries none of this either: an agent only ever announces "I am key X", and
// this file is the only place that says what X means.
type RoomConfig struct {
	Defaults AutoOffConfig `json:"auto_off_defaults"`
	Rooms    []RoomEntry   `json:"rooms"`
}

var defaultAutoOff = AutoOffConfig{
	DebounceMinutes:      5,
	RetryIntervalMinutes: 2,
	MaxRetries:           3,
}

// normalizeKey makes PC identity comparisons agnostic to how a MAC address is
// punctuated or cased ("AA:BB:..", "aa-bb-..", "aabb..").
func normalizeKey(key string) string {
	key = strings.ToLower(key)
	key = strings.NewReplacer(":", "", "-", "", " ", "").Replace(key)
	return key
}

// LoadRoomConfig reads room_config.json. A missing file is not an error: it
// means the auto-off feature is simply not configured for this store, so an
// empty config (no rooms) is returned.
func LoadRoomConfig(path string) (*RoomConfig, error) {
	cfg := &RoomConfig{Defaults: defaultAutoOff}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return cfg, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read room config: %w", err)
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse room config %s: %w", path, err)
	}
	if cfg.Defaults.DebounceMinutes <= 0 {
		cfg.Defaults.DebounceMinutes = defaultAutoOff.DebounceMinutes
	}
	if cfg.Defaults.RetryIntervalMinutes <= 0 {
		cfg.Defaults.RetryIntervalMinutes = defaultAutoOff.RetryIntervalMinutes
	}
	if cfg.Defaults.MaxRetries <= 0 {
		cfg.Defaults.MaxRetries = defaultAutoOff.MaxRetries
	}
	return cfg, nil
}

// find returns the room entry for roomID, if any room in the config claims it.
func (c *RoomConfig) find(roomID string) (RoomEntry, bool) {
	if c == nil {
		return RoomEntry{}, false
	}
	for _, r := range c.Rooms {
		if r.RoomID == roomID {
			return r, true
		}
	}
	return RoomEntry{}, false
}

// findByKey resolves a pcagent's self-reported identity key (its MAC) to the
// room and pc_id an admin assigned it in room_config.json.
func (c *RoomConfig) findByKey(key string) (roomID, pcID string, ok bool) {
	if c == nil {
		return "", "", false
	}
	nk := normalizeKey(key)
	for _, r := range c.Rooms {
		for _, pc := range r.PCs {
			if normalizeKey(pc.Key) == nk {
				return r.RoomID, pc.PCID, true
			}
		}
	}
	return "", "", false
}

// autoOff resolves this room's debounce/retry settings, falling back to the
// file-level defaults for anything it does not override.
func (e RoomEntry) autoOff(defaults AutoOffConfig) AutoOffConfig {
	if e.AutoOff == nil {
		return defaults
	}
	ao := *e.AutoOff
	if ao.DebounceMinutes <= 0 {
		ao.DebounceMinutes = defaults.DebounceMinutes
	}
	if ao.RetryIntervalMinutes <= 0 {
		ao.RetryIntervalMinutes = defaults.RetryIntervalMinutes
	}
	if ao.MaxRetries <= 0 {
		ao.MaxRetries = defaults.MaxRetries
	}
	return ao
}
