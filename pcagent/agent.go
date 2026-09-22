package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type statusPayload struct {
	Status string `json:"status"`
	TS     int64  `json:"ts"`
}

// topicFor matches the gateway's unscoped atom/pcagent/status/{key} topic
// (gateway/mqtt.go): unlike the Atom Lite topics, this one carries no store
// id, because a pcagent knows nothing about the store it is in -- only the
// broker address it was given.
func topicFor(key string) string {
	return fmt.Sprintf("atom/pcagent/status/%s", key)
}

func encodeStatus(status string) []byte {
	b, _ := json.Marshal(statusPayload{Status: status, TS: time.Now().Unix()})
	return b
}

// Agent announces this PC's power state to the store gateway. It reports
// "on" as soon as it connects and again every Heartbeat, and reports "off"
// itself on a clean shutdown. If the process instead disappears without
// warning -- the PC is switched off at the wall, Windows crashes, the
// network drops -- the MQTT broker publishes the same "off" message on its
// behalf via the connection's Last Will, so the gateway finds out either way.
type Agent struct {
	cfg    Config
	client mqtt.Client
	topic  string
}

func NewAgent(cfg Config) *Agent {
	a := &Agent{cfg: cfg, topic: topicFor(cfg.PCKey)}

	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s:%d", cfg.MQTTHost, cfg.MQTTPort)).
		SetClientID(fmt.Sprintf("pcagent-%s-%d", cfg.PCKey, os.Getpid())).
		SetCleanSession(true).
		SetKeepAlive(20*time.Second).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5*time.Second).
		SetConnectTimeout(5*time.Second).
		SetWill(a.topic, string(encodeStatus("off")), 1, true)

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		tok := c.Publish(a.topic, 1, true, encodeStatus("on"))
		if tok.WaitTimeout(3*time.Second) && tok.Error() != nil {
			slog.Warn("could not publish on-status", "err", tok.Error())
			return
		}
		slog.Info("MQTT connected; reported on",
			"broker", fmt.Sprintf("%s:%d", cfg.MQTTHost, cfg.MQTTPort), "topic", a.topic)
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		slog.Warn("MQTT connection lost; paho will retry", "err", err)
	})

	a.client = mqtt.NewClient(opts)
	return a
}

// Run connects, republishes "on" every cfg.Heartbeat, and publishes "off"
// itself when ctx is cancelled (a normal SCM stop or Ctrl+C).
func (a *Agent) Run(ctx context.Context) error {
	token := a.client.Connect()
	go func() {
		if token.WaitTimeout(6*time.Second) && token.Error() != nil {
			slog.Warn("initial MQTT connect failed; retrying in background", "err", token.Error())
		}
	}()

	ticker := time.NewTicker(a.cfg.Heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			a.publishOff()
			a.client.Disconnect(500)
			return nil
		case <-ticker.C:
			if a.client.IsConnectionOpen() {
				a.client.Publish(a.topic, 1, true, encodeStatus("on")).WaitTimeout(3 * time.Second)
			}
		}
	}
}

// publishOff is best-effort: on a clean shutdown there is only a moment
// before the process exits, and if the broker is unreachable the Last Will
// registered at connect time never applies (the socket was already closed
// gracefully), so this is the only chance to say "off".
func (a *Agent) publishOff() {
	if !a.client.IsConnectionOpen() {
		return
	}
	a.client.Publish(a.topic, 1, true, encodeStatus("off")).WaitTimeout(2 * time.Second)
	slog.Info("reported off before shutdown", "topic", a.topic)
}
