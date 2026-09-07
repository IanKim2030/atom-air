// Package protocol implements the Atom Air wire formats: the 12-byte sensor
// packet an Atom Lite publishes and the 8-byte AC control frame it consumes.
//
// This is the Go half of a two-language protocol. The Python cloud carries the
// same formats in common/protocol.py, and both sides assert against the shared
// conformance vectors in common/protocol_vectors.json, so the two
// implementations cannot drift apart.
//
// Sensor packet (Atom -> PC), 12 bytes, little endian:
//
//	offset  size  field
//	0       1     header    0xAA
//	1       1     devID
//	2       2     seq       uint16, wraps at 65535
//	4       2     temp      int16,  celsius * 100 (signed: sub-zero readings)
//	6       2     hum       uint16, percent * 100
//	8       2     light     uint16, raw lux
//	10      1     flags     bitfield, see Flag*
//	11      1     checksum  XOR of bytes 0..10
//
// AC control packet (PC -> Atom), 8 bytes, little endian:
//
//	offset  size  field
//	0       1     header    0x55
//	1       1     targetID
//	2       1     power     0=off 1=on
//	3       1     mode      see modeToWire
//	4       1     temp      celsius, 16..30
//	5       1     fan       see fanToWire
//	6       1     checksum  XOR of bytes 0..5
//	7       1     tail      0xEE
package protocol

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

// Frame layout constants.
const (
	SensorHeader = 0xAA
	SensorSize   = 12
	ACHeader     = 0x55
	ACTail       = 0xEE
	ACSize       = 8

	TempMin = 16 // AC setpoint bounds, inclusive
	TempMax = 30
)

// Sensor flags bitfield.
const (
	FlagACOn        = 0x01 // device believes the AC is running
	FlagIRReady     = 0x02 // IR-capable firmware flashed (SOTA stage 2 complete)
	FlagSensorFault = 0x04 // last read from the DHT/lux sensor failed
)

var (
	modeToWire = map[string]uint8{"cool": 0, "heat": 1, "dry": 2, "fan": 3, "auto": 4}
	fanToWire  = map[string]uint8{"auto": 0, "low": 1, "mid": 2, "high": 3}
	wireToMode = invert(modeToWire)
	wireToFan  = invert(fanToWire)
)

func invert(m map[string]uint8) map[uint8]string {
	out := make(map[uint8]string, len(m))
	for k, v := range m {
		out[v] = k
	}
	return out
}

// SensorReading is a decoded sensor packet in engineering units. The JSON tags
// match the cloud's `live` message payload.
type SensorReading struct {
	DevID   uint8   `json:"dev_id"`
	Seq     uint16  `json:"seq"`
	Temp    float64 `json:"temp"`
	Hum     float64 `json:"hum"`
	Light   uint16  `json:"light"`
	Flags   uint8   `json:"flags"`
	ACOn    bool    `json:"ac_on"`
	IRReady bool    `json:"ir_ready"`
	Fault   bool    `json:"fault"`
}

// ACCommand is a decoded AC control frame.
type ACCommand struct {
	TargetID uint8  `json:"target_id"`
	Power    uint8  `json:"power"`
	Mode     string `json:"mode"`
	Temp     uint8  `json:"temp"`
	Fan      string `json:"fan"`
}

// XORChecksum is the checksum both frame types use: XOR of every byte given.
func XORChecksum(b []byte) byte {
	var chk byte
	for _, v := range b {
		chk ^= v
	}
	return chk
}

// NormalizeMode accepts the friendly name used by the web UI and returns the
// wire value.
func NormalizeMode(mode string) (uint8, error) {
	v, ok := modeToWire[strings.ToLower(strings.TrimSpace(mode))]
	if !ok {
		return 0, fmt.Errorf("unknown AC mode: %q", mode)
	}
	return v, nil
}

// NormalizeFan is NormalizeMode's counterpart for the fan level.
func NormalizeFan(fan string) (uint8, error) {
	v, ok := fanToWire[strings.ToLower(strings.TrimSpace(fan))]
	if !ok {
		return 0, fmt.Errorf("unknown fan level: %q", fan)
	}
	return v, nil
}

// ClampTargetTemp holds a setpoint inside the range the hardware accepts.
func ClampTargetTemp(t int) uint8 {
	if t < TempMin {
		t = TempMin
	}
	if t > TempMax {
		t = TempMax
	}
	return uint8(t)
}

// scale converts engineering units to the fixed-point wire representation.
// math.Round matches the Python side's int(round(x)) for every value the
// conformance vectors cover.
func scale(v float64, lo, hi float64) float64 {
	return math.Max(lo, math.Min(hi, math.Round(v*100)))
}

// EncodeSensorPacket builds a 12-byte sensor frame. Used by the device
// simulator and by tests; real frames come off the wire from the firmware.
func EncodeSensorPacket(devID uint8, seq uint16, temp, hum float64, light uint16, flags uint8) []byte {
	buf := make([]byte, SensorSize)
	buf[0] = SensorHeader
	buf[1] = devID
	binary.LittleEndian.PutUint16(buf[2:4], seq)
	binary.LittleEndian.PutUint16(buf[4:6], uint16(int16(scale(temp, -32768, 32767))))
	binary.LittleEndian.PutUint16(buf[6:8], uint16(scale(hum, 0, 65535)))
	binary.LittleEndian.PutUint16(buf[8:10], light)
	buf[10] = flags
	buf[11] = XORChecksum(buf[:11])
	return buf
}

// DecodeSensorPacket validates and decodes one 12-byte frame.
func DecodeSensorPacket(raw []byte) (SensorReading, error) {
	var r SensorReading
	if len(raw) != SensorSize {
		return r, fmt.Errorf("sensor packet must be %d bytes, got %d", SensorSize, len(raw))
	}
	if raw[0] != SensorHeader {
		return r, fmt.Errorf("bad sensor header 0x%02X", raw[0])
	}
	if got, want := raw[11], XORChecksum(raw[:11]); got != want {
		return r, fmt.Errorf("sensor checksum mismatch (got 0x%02X, want 0x%02X)", got, want)
	}
	flags := raw[10]
	return SensorReading{
		DevID:   raw[1],
		Seq:     binary.LittleEndian.Uint16(raw[2:4]),
		Temp:    float64(int16(binary.LittleEndian.Uint16(raw[4:6]))) / 100,
		Hum:     float64(binary.LittleEndian.Uint16(raw[6:8])) / 100,
		Light:   binary.LittleEndian.Uint16(raw[8:10]),
		Flags:   flags,
		ACOn:    flags&FlagACOn != 0,
		IRReady: flags&FlagIRReady != 0,
		Fault:   flags&FlagSensorFault != 0,
	}, nil
}

// EncodeACPacket builds the 8-byte AC control frame.
func EncodeACPacket(targetID, power uint8, mode string, temp int, fan string) ([]byte, error) {
	modeWire, err := NormalizeMode(mode)
	if err != nil {
		return nil, err
	}
	fanWire, err := NormalizeFan(fan)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, ACSize)
	buf[0] = ACHeader
	buf[1] = targetID
	if power != 0 {
		buf[2] = 1
	}
	buf[3] = modeWire
	buf[4] = ClampTargetTemp(temp)
	buf[5] = fanWire
	buf[6] = XORChecksum(buf[:6])
	buf[7] = ACTail
	return buf, nil
}

// DecodeACPacket validates and decodes an 8-byte AC control frame.
func DecodeACPacket(raw []byte) (ACCommand, error) {
	var c ACCommand
	if len(raw) != ACSize {
		return c, fmt.Errorf("AC packet must be %d bytes, got %d", ACSize, len(raw))
	}
	if raw[0] != ACHeader {
		return c, fmt.Errorf("bad AC header 0x%02X", raw[0])
	}
	if raw[7] != ACTail {
		return c, fmt.Errorf("bad AC tail 0x%02X", raw[7])
	}
	if got, want := raw[6], XORChecksum(raw[:6]); got != want {
		return c, fmt.Errorf("AC checksum mismatch (got 0x%02X, want 0x%02X)", got, want)
	}
	mode, ok := wireToMode[raw[3]]
	if !ok {
		return c, fmt.Errorf("unknown AC mode value %d", raw[3])
	}
	fan, ok := wireToFan[raw[5]]
	if !ok {
		return c, fmt.Errorf("unknown fan value %d", raw[5])
	}
	return ACCommand{TargetID: raw[1], Power: raw[2], Mode: mode, Temp: raw[4], Fan: fan}, nil
}
