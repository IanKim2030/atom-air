package protocol

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// vectors mirrors common/protocol_vectors.json, the cross-language contract
// between this package and the Python cloud's common/protocol.py.
type vectors struct {
	Sensor []struct {
		Hex     string  `json:"hex"`
		DevID   uint8   `json:"dev_id"`
		Seq     uint16  `json:"seq"`
		Temp    float64 `json:"temp"`
		Hum     float64 `json:"hum"`
		Light   uint16  `json:"light"`
		Flags   uint8   `json:"flags"`
		ACOn    bool    `json:"ac_on"`
		IRReady bool    `json:"ir_ready"`
		Fault   bool    `json:"fault"`
	} `json:"sensor"`
	AC []struct {
		Hex      string `json:"hex"`
		TargetID uint8  `json:"target_id"`
		Power    uint8  `json:"power"`
		Mode     string `json:"mode"`
		Temp     uint8  `json:"temp"`
		Fan      string `json:"fan"`
	} `json:"ac"`
}

func load(t *testing.T) vectors {
	t.Helper()
	path := filepath.Join("..", "..", "common", "protocol_vectors.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var v vectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if len(v.Sensor) == 0 || len(v.AC) == 0 {
		t.Fatalf("%s has no vectors", path)
	}
	return v
}

func TestSensorVectorsDecode(t *testing.T) {
	for _, want := range load(t).Sensor {
		raw, err := hex.DecodeString(want.Hex)
		if err != nil {
			t.Fatalf("bad hex %q: %v", want.Hex, err)
		}
		got, err := DecodeSensorPacket(raw)
		if err != nil {
			t.Fatalf("%s: %v", want.Hex, err)
		}
		if got.DevID != want.DevID || got.Seq != want.Seq || got.Temp != want.Temp ||
			got.Hum != want.Hum || got.Light != want.Light || got.Flags != want.Flags ||
			got.ACOn != want.ACOn || got.IRReady != want.IRReady || got.Fault != want.Fault {
			t.Errorf("%s\n got %+v\nwant %+v", want.Hex, got, want)
		}
	}
}

func TestSensorVectorsEncode(t *testing.T) {
	for _, want := range load(t).Sensor {
		got := hex.EncodeToString(
			EncodeSensorPacket(want.DevID, want.Seq, want.Temp, want.Hum, want.Light, want.Flags))
		if got != want.Hex {
			t.Errorf("encode(%+v)\n got %s\nwant %s", want, got, want.Hex)
		}
	}
}

func TestACVectors(t *testing.T) {
	for _, want := range load(t).AC {
		built, err := EncodeACPacket(want.TargetID, want.Power, want.Mode, int(want.Temp), want.Fan)
		if err != nil {
			t.Fatalf("encode %+v: %v", want, err)
		}
		if got := hex.EncodeToString(built); got != want.Hex {
			t.Errorf("encode(%+v)\n got %s\nwant %s", want, got, want.Hex)
		}
		got, err := DecodeACPacket(built)
		if err != nil {
			t.Fatalf("%s: %v", want.Hex, err)
		}
		if got.TargetID != want.TargetID || got.Power != want.Power || got.Mode != want.Mode ||
			got.Temp != want.Temp || got.Fan != want.Fan {
			t.Errorf("%s\n got %+v\nwant %+v", want.Hex, got, want)
		}
	}
}

func TestRejectsMalformed(t *testing.T) {
	good := EncodeSensorPacket(1, 1, 20, 50, 100, 0)

	short := good[:SensorSize-1]
	if _, err := DecodeSensorPacket(short); err == nil {
		t.Error("short sensor packet accepted")
	}

	badHeader := append([]byte(nil), good...)
	badHeader[0] = 0x00
	if _, err := DecodeSensorPacket(badHeader); err == nil {
		t.Error("bad sensor header accepted")
	}

	corrupt := append([]byte(nil), good...)
	corrupt[5] ^= 0xFF // flip a payload bit, leave the checksum stale
	if _, err := DecodeSensorPacket(corrupt); err == nil {
		t.Error("sensor checksum mismatch accepted")
	}

	ac, _ := EncodeACPacket(1, 1, "cool", 24, "auto")
	badTail := append([]byte(nil), ac...)
	badTail[7] = 0x00
	if _, err := DecodeACPacket(badTail); err == nil {
		t.Error("bad AC tail accepted")
	}

	if _, err := EncodeACPacket(1, 1, "turbo", 24, "auto"); err == nil {
		t.Error("unknown AC mode accepted")
	}
	if _, err := EncodeACPacket(1, 1, "cool", 24, "hurricane"); err == nil {
		t.Error("unknown fan level accepted")
	}
}

func TestClampTargetTemp(t *testing.T) {
	for _, c := range []struct{ in, want int }{
		{5, TempMin}, {16, 16}, {24, 24}, {30, 30}, {99, TempMax},
	} {
		if got := ClampTargetTemp(c.in); int(got) != c.want {
			t.Errorf("ClampTargetTemp(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}
