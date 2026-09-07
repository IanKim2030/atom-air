package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// identifyResponse mirrors GET /api/v1/gateway/identify.
type identifyResponse struct {
	StoreID string `json:"store_id"`
	Name    string `json:"name"`
}

// cachedStoreID returns the id a previous run resolved. The licence config is
// already the gateway's "what do I know about myself offline" file, so the
// store id belongs there too: a cloud that is down at boot must not stop the
// gateway from collecting, which is the whole point of the grace period.
func cachedStoreID(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var cfg LicenseConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return ""
	}
	return cfg.StoreID
}

// identifyOnce asks the cloud which store this gateway's token belongs to.
func identifyOnce(ctx context.Context, client *http.Client, endpoint string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		// 401 (unknown token) and 403 (IP not registered) are operator errors,
		// not transient ones, so the message has to survive into the log.
		var problem struct {
			Detail string `json:"detail"`
		}
		_ = json.Unmarshal(body, &problem)
		if problem.Detail == "" {
			problem.Detail = strings.TrimSpace(string(body))
		}
		return "", fmt.Errorf("cloud refused (%d): %s", resp.StatusCode, problem.Detail)
	}
	var out identifyResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("unreadable identify response: %w", err)
	}
	if out.StoreID == "" {
		return "", fmt.Errorf("cloud returned no store id")
	}
	slog.Info("cloud identified this gateway", "store", out.StoreID, "name", out.Name)
	return out.StoreID, nil
}

// resolveStoreID decides which store this gateway serves.
//
// The token is the identity -- the cloud maps it to exactly one store -- so a
// technician configures one value instead of two. Precedence: an explicit
// --store-id, then the id cached from a previous run, then the cloud.
func resolveStoreID(ctx context.Context, cfg Config) (string, error) {
	if cfg.StoreID != "" {
		return cfg.StoreID, nil
	}
	if id := cachedStoreID(cfg.LicenseConfig); id != "" {
		slog.Info("store id from the cached licence config", "store", id)
		return id, nil
	}
	endpoint := fmt.Sprintf("%s/api/v1/gateway/identify?token=%s",
		strings.TrimRight(cfg.CloudHTTP, "/"), url.QueryEscape(cfg.Token))
	client := &http.Client{Timeout: 10 * time.Second}
	backoff := 2 * time.Second
	for {
		id, err := identifyOnce(ctx, client, endpoint)
		if err == nil {
			return id, nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		slog.Warn("could not identify this gateway; retrying",
			"err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < time.Minute {
			backoff *= 2
		}
	}
}
