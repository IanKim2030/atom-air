"""Python half of the cross-language protocol contract.

The Go gateway asserts against the same file in gateway/protocol/protocol_test.go.
Run both and the two implementations cannot drift apart:

    python -m unittest discover -s common          # this side
    go test ./...                                  # from gateway/
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import protocol as p  # noqa: E402

VECTORS = json.loads(
    (Path(__file__).resolve().parent / "protocol_vectors.json").read_text(encoding="utf-8"))


class SensorVectors(unittest.TestCase):
    def test_decode(self):
        for v in VECTORS["sensor"]:
            with self.subTest(hex=v["hex"]):
                got = p.decode_sensor_packet(bytes.fromhex(v["hex"]))
                for field in ("dev_id", "seq", "temp", "hum", "light", "flags",
                              "ac_on", "ir_ready", "fault"):
                    self.assertEqual(got[field], v[field], field)

    def test_encode(self):
        for v in VECTORS["sensor"]:
            with self.subTest(hex=v["hex"]):
                built = p.encode_sensor_packet(v["dev_id"], v["seq"], v["temp"],
                                               v["hum"], v["light"], v["flags"])
                self.assertEqual(built.hex(), v["hex"])
                self.assertEqual(len(built), p.SENSOR_SIZE)


class ACVectors(unittest.TestCase):
    def test_round_trip(self):
        for v in VECTORS["ac"]:
            with self.subTest(hex=v["hex"]):
                built = p.encode_ac_packet(v["target_id"], v["power"], v["mode"],
                                           v["temp"], v["fan"])
                self.assertEqual(built.hex(), v["hex"])
                self.assertEqual(len(built), p.AC_SIZE)
                got = p.decode_ac_packet(built)
                for field in ("target_id", "power", "mode", "temp", "fan"):
                    self.assertEqual(got[field], v[field], field)


class Malformed(unittest.TestCase):
    def test_rejected(self):
        good = p.encode_sensor_packet(1, 1, 20.0, 50.0, 100, 0)

        with self.assertRaises(p.PacketError):
            p.decode_sensor_packet(good[:-1])

        bad_header = bytearray(good)
        bad_header[0] = 0x00
        with self.assertRaises(p.PacketError):
            p.decode_sensor_packet(bytes(bad_header))

        corrupt = bytearray(good)
        corrupt[5] ^= 0xFF  # flip a payload bit, leave the checksum stale
        with self.assertRaises(p.PacketError):
            p.decode_sensor_packet(bytes(corrupt))

        ac = bytearray(p.encode_ac_packet(1, 1, "cool", 24, "auto"))
        ac[7] = 0x00
        with self.assertRaises(p.PacketError):
            p.decode_ac_packet(bytes(ac))

        with self.assertRaises(p.PacketError):
            p.encode_ac_packet(1, 1, "turbo", 24, "auto")
        with self.assertRaises(p.PacketError):
            p.encode_ac_packet(1, 1, "cool", 24, "hurricane")

    def test_clamp(self):
        for given, want in ((5, 16), (16, 16), (24, 24), (30, 30), (99, 30)):
            self.assertEqual(p.clamp_target_temp(given), want)


if __name__ == "__main__":
    unittest.main()
