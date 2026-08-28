package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// LicenseConfig is the locally cached licence state, persisted to
// store_license_config.json so it survives restarts and cloud outages.
type LicenseConfig struct {
	StoreID           string `json:"store_id"`
	GracePeriodDays   int    `json:"grace_period_days"`
	LastAuthorizedAt  string `json:"last_authorized_at"`
	LastStatus        string `json:"last_status"`
	LicenseExpiresAt  string `json:"license_expires_at"`
	DeviceFingerprint string `json:"device_fingerprint"`
	UpdatedAt         string `json:"updated_at"`
}

// Verdict is the outcome of a licence evaluation, whether it came from the
// cloud or from the cached config during an outage.
type Verdict struct {
	Operational   bool
	Online        bool
	Reason        string
	DaysRemaining int
	Message       string
}

// authorizeResponse mirrors POST /api/v1/store/authorize.
type authorizeResponse struct {
	Authorized       bool   `json:"authorized"`
	Status           string `json:"status"`
	GracePeriodDays  int    `json:"grace_period_days"`
	LicenseExpiresAt string `json:"license_expires_at"`
	GraceExpiresAt   string `json:"grace_expires_at"`
	DaysRemaining    int    `json:"days_remaining"`
	ServerTime       string `json:"server_time"`
	Message          string `json:"message"`
}

// LicenseManager keeps the store operating for GracePeriodDays after the last
// successful check. The grace window is dictated by the cloud and cached
// locally, so a cloud outage cannot immediately shut a store down.
type LicenseManager struct {
	storeID   string
	path      string
	cloudHTTP string
	client    *http.Client

	mu  sync.RWMutex
	cfg LicenseConfig
}

func NewLicenseManager(storeID, path, cloudHTTP string, defaultGraceDays int) *LicenseManager {
	m := &LicenseManager{
		storeID:   storeID,
		path:      path,
		cloudHTTP: strings.TrimRight(cloudHTTP, "/"),
		client:    &http.Client{Timeout: 10 * time.Second},
		cfg: LicenseConfig{
			StoreID:         storeID,
			GracePeriodDays: defaultGraceDays,
			LastStatus:      "unknown",
		},
	}
	m.load()
	if m.cfg.StoreID != storeID {
		// The config doubles as the cache resolveStoreID reads at boot, so the
		// id that actually won has to be the one written back.
		m.cfg.StoreID = storeID
		m.save()
	}
	if m.cfg.DeviceFingerprint == "" {
		m.cfg.DeviceFingerprint = fingerprint(storeID)
		m.save()
	}
	return m
}

// fingerprint derives a stable per-machine identifier from the first physical
// MAC address, falling back to the hostname.
func fingerprint(storeID string) string {
	if ifaces, err := net.Interfaces(); err == nil {
		for _, ifi := range ifaces {
			if ifi.Flags&net.FlagLoopback != 0 || len(ifi.HardwareAddr) == 0 {
				continue
			}
			return fmt.Sprintf("%s-%s", storeID,
				strings.ReplaceAll(ifi.HardwareAddr.String(), ":", ""))
		}
	}
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%s", storeID, host)
}

func (m *LicenseManager) Config() LicenseConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg
}

func (m *LicenseManager) load() {
	raw, err := os.ReadFile(m.path)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("could not read licence config; using defaults",
				"path", m.path, "err", err)
		}
		return
	}
	var cfg LicenseConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		slog.Warn("licence config is not valid JSON; using defaults",
			"path", m.path, "err", err)
		return
	}
	if cfg.GracePeriodDays <= 0 {
		cfg.GracePeriodDays = m.cfg.GracePeriodDays
	}
	m.cfg = cfg
	slog.Info("loaded licence config",
		"grace_days", cfg.GracePeriodDays, "last_ok", cfg.LastAuthorizedAt)
}

// save writes the config atomically, so a crash mid-write cannot leave the
// gateway with a truncated licence file and no way to start.
func (m *LicenseManager) save() {
	m.cfg.UpdatedAt = isoNow()
	raw, err := json.MarshalIndent(m.cfg, "", "  ")
	if err != nil {
		slog.Error("could not encode licence config", "err", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(m.path), 0o755); err != nil {
		slog.Error("could not create licence config dir", "err", err)
		return
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o644); err != nil {
		slog.Error("could not write licence config", "err", err)
		return
	}
	if err := os.Rename(tmp, m.path); err != nil {
		slog.Error("could not replace licence config", "err", err)
	}
}

// EvaluateOffline decides using only the cached config -- the cloud may be down.
func (m *LicenseManager) EvaluateOffline() Verdict {
	m.mu.RLock()
	cfg := m.cfg
	m.mu.RUnlock()

	lastOK, ok := parseISO(cfg.LastAuthorizedAt)
	if !ok {
		return Verdict{Reason: "no_successful_check",
			Message: "최초 인증이 완료되지 않았습니다."}
	}
	deadline := lastOK.Add(time.Duration(cfg.GracePeriodDays) * 24 * time.Hour)
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return Verdict{Reason: "grace_expired",
			Message: fmt.Sprintf("유예기간 %d일이 만료되었습니다. 클라우드 인증이 필요합니다.",
				cfg.GracePeriodDays)}
	}
	days := int(remaining.Hours() / 24)
	reason := cfg.LastStatus
	if reason == "" {
		reason = "cached"
	}
	return Verdict{Operational: true, Reason: reason, DaysRemaining: days,
		Message: fmt.Sprintf("오프라인 유예 %d일 남음", days)}
}

// Check calls POST /api/v1/store/authorize and caches the grace terms it
// returns. On any transport failure it falls back to the offline evaluation.
func (m *LicenseManager) Check() Verdict {
	m.mu.RLock()
	body, _ := json.Marshal(map[string]string{
		"store_id":           m.storeID,
		"device_fingerprint": m.cfg.DeviceFingerprint,
		"app_version":        AppVersion,
	})
	m.mu.RUnlock()

	resp, err := m.client.Post(m.cloudHTTP+"/api/v1/store/authorize",
		"application/json", bytes.NewReader(body))
	if err != nil {
		v := m.EvaluateOffline()
		slog.Warn("licence check failed; falling back to cached grace",
			"err", err, "verdict", v.Message)
		return v
	}
	defer resp.Body.Close()

	var out authorizeResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		v := m.EvaluateOffline()
		slog.Warn("licence response was unreadable; falling back to cached grace",
			"err", err, "verdict", v.Message)
		return v
	}

	// The cloud owns the grace terms; cache them for the next outage.
	m.mu.Lock()
	if out.GracePeriodDays > 0 {
		m.cfg.GracePeriodDays = out.GracePeriodDays
	}
	m.cfg.LastStatus = out.Status
	m.cfg.LicenseExpiresAt = out.LicenseExpiresAt
	if out.Authorized {
		if out.ServerTime != "" {
			m.cfg.LastAuthorizedAt = out.ServerTime
		} else {
			m.cfg.LastAuthorizedAt = isoNow()
		}
	}
	m.save()
	m.mu.Unlock()

	return Verdict{
		Operational:   out.Authorized,
		Online:        true,
		Reason:        out.Status,
		DaysRemaining: out.DaysRemaining,
		Message:       out.Message,
	}
}
