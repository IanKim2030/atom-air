package main

import (
	"context"
	"log/slog"
	"math/rand"
	"time"

	"github.com/IanKim2030/atom-air/gateway/protocol"
)

// simDevice is one synthetic Atom Lite.
type simDevice struct {
	id    uint8
	seq   uint16
	temp  float64
	hum   float64
	light float64
}

// loopSimulate synthesises Atom Lite traffic through the exact same ingest
// path the MQTT bridge uses, so everything downstream is exercised for real
// when no hardware or broker is available.
func (s *Service) loopSimulate(ctx context.Context) {
	devices := make([]*simDevice, 0, s.cfg.Devices)
	for i := 1; i <= s.cfg.Devices; i++ {
		devices = append(devices, &simDevice{
			id:    uint8(i),
			temp:  26 + (rand.Float64()*3 - 1.5),
			hum:   48 + (rand.Float64()*10 - 5),
			light: 700 + float64(rand.Intn(300)-150),
		})
	}
	slog.Info("simulator emitting devices at 1Hz", "devices", len(devices))

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			ac := s.acState
			ready := make(map[uint8]bool, len(s.irReady))
			for k, v := range s.irReady {
				ready[k] = v
			}
			s.mu.Unlock()

			for _, d := range devices {
				d.seq++
				var flags uint8
				if ac.Power != 0 && d.id == ac.TargetID {
					flags |= protocol.FlagACOn
					// Drift toward the setpoint so the remote visibly moves the chart.
					d.temp += (float64(ac.Temp) - d.temp) * 0.06
					d.hum += (42 - d.hum) * 0.03
				} else {
					d.temp += rand.Float64()*0.18 - 0.08
					d.hum += rand.Float64()*0.3 - 0.15
				}
				if ready[d.id] {
					flags |= protocol.FlagIRReady
				}

				d.temp = clampF(d.temp+rand.Float64()*0.08-0.04, 14, 38)
				d.hum = clampF(d.hum, 20, 85)
				d.light = clampF(d.light+float64(rand.Intn(51)-25), 0, 2000)

				s.onFrame(protocol.EncodeSensorPacket(
					d.id, d.seq, d.temp, d.hum, uint16(d.light), flags))
			}
		}
	}
}

func clampF(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
