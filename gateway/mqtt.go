package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// MQTTBridge subscribes to the in-store Atom devices via Mosquitto and
// publishes AC control and OTA commands back to them.
//
// paho runs its own network goroutine; incoming frames are handed straight to
// a channel, so the ingest loop never touches paho's internals.
type MQTTBridge struct {
	client    mqtt.Client
	connected atomic.Bool

	topicSensor string
	topicAC     string
	topicOTA    string
	topicLearn  string
	topicIR     string
	topicLog    string
}

func NewMQTTBridge(host string, port int, storeID string, onFrame func([]byte),
	onIREvent func(uint8, []byte), onLog func(uint8, []byte)) *MQTTBridge {
	b := &MQTTBridge{
		topicSensor: fmt.Sprintf("atom/%s/sensor", storeID),
		topicAC:     fmt.Sprintf("atom/%s/ac", storeID),
		topicOTA:    fmt.Sprintf("atom/%s/ota", storeID),
		topicLearn:  fmt.Sprintf("atom/%s/learn", storeID),
		topicIR:     fmt.Sprintf("atom/%s/ir", storeID),
		topicLog:    fmt.Sprintf("atom/%s/log", storeID),
	}

	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s:%d", host, port)).
		SetClientID(fmt.Sprintf("gw-%s-%d", storeID, os.Getpid())).
		SetCleanSession(true).
		SetKeepAlive(30 * time.Second).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetConnectTimeout(5 * time.Second)

	opts.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
		// Copy: paho reuses the payload buffer after the handler returns.
		frame := append([]byte(nil), msg.Payload()...)
		onFrame(frame)
	})
	// IR events (learn captures, IRDATA acks) are JSON, so they get their own
	// handler: the default handler feeds the sensor ingest loop, which would
	// try to parse them as 12-byte binary frames.
	// Both carry a dev_id as the last topic segment, so they share a shape.
	perDevice := func(kind string, fn func(uint8, []byte)) mqtt.MessageHandler {
		return func(_ mqtt.Client, msg mqtt.Message) {
			parts := strings.Split(msg.Topic(), "/")
			id, err := strconv.Atoi(parts[len(parts)-1])
			if err != nil || id < 0 || id > 255 {
				slog.Warn(kind+" on unparseable topic", "topic", msg.Topic())
				return
			}
			if fn != nil {
				fn(uint8(id), append([]byte(nil), msg.Payload()...))
			}
		}
	}
	irHandler := perDevice("IR event", onIREvent)
	// Console mirrors are debug chatter: QoS 0 and no retry, because a line
	// lost while the link flaps is not worth delaying a learn capture for.
	logHandler := perDevice("device log", onLog)

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		b.connected.Store(true)
		filters := map[string]byte{b.topicSensor: 0, b.topicSensor + "/+": 0}
		if tok := c.SubscribeMultiple(filters, nil); tok.Wait() && tok.Error() != nil {
			slog.Error("MQTT subscribe failed", "err", tok.Error())
			return
		}
		if tok := c.Subscribe(b.topicIR+"/+", 1, irHandler); tok.Wait() && tok.Error() != nil {
			slog.Error("MQTT IR subscribe failed", "err", tok.Error())
			return
		}
		if tok := c.Subscribe(b.topicLog+"/+", 0, logHandler); tok.Wait() && tok.Error() != nil {
			slog.Error("MQTT device-log subscribe failed", "err", tok.Error())
			return
		}
		slog.Info("MQTT connected", "broker", fmt.Sprintf("%s:%d", host, port),
			"topic", b.topicSensor+"/#")
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		b.connected.Store(false)
		slog.Warn("MQTT connection lost; paho will retry", "err", err)
	})

	b.client = mqtt.NewClient(opts)
	return b
}

// Start begins connecting. With ConnectRetry set, a broker that is not up yet
// is not an error: paho keeps trying in the background.
func (b *MQTTBridge) Start() {
	token := b.client.Connect()
	go func() {
		if token.WaitTimeout(6*time.Second) && token.Error() != nil {
			slog.Warn("MQTT initial connect failed; retrying in background",
				"err", token.Error())
		}
	}()
}

func (b *MQTTBridge) Connected() bool { return b != nil && b.connected.Load() }

func (b *MQTTBridge) Stop() {
	if b != nil && b.client != nil {
		b.client.Disconnect(500)
	}
}

// PublishAC sends the raw 8-byte control frame to one device.
func (b *MQTTBridge) PublishAC(targetID uint8, packet []byte) bool {
	if !b.Connected() {
		return false
	}
	topic := fmt.Sprintf("%s/%d", b.topicAC, targetID)
	tok := b.client.Publish(topic, 1, false, packet)
	return tok.WaitTimeout(3*time.Second) && tok.Error() == nil
}

// PublishLearn puts one device into (or out of) IR learn mode.
func (b *MQTTBridge) PublishLearn(targetID uint8, cmd any) bool {
	if !b.Connected() {
		return false
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		slog.Error("could not encode learn command", "err", err)
		return false
	}
	topic := fmt.Sprintf("%s/%d", b.topicLearn, targetID)
	tok := b.client.Publish(topic, 1, false, payload)
	return tok.WaitTimeout(3*time.Second) && tok.Error() == nil
}

// PublishOTA tells one device where to fetch its new firmware.
func (b *MQTTBridge) PublishOTA(targetID uint8, cmd any) bool {
	if !b.Connected() {
		return false
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		slog.Error("could not encode OTA command", "err", err)
		return false
	}
	topic := fmt.Sprintf("%s/%d", b.topicOTA, targetID)
	tok := b.client.Publish(topic, 1, false, payload)
	return tok.WaitTimeout(3*time.Second) && tok.Error() == nil
}
