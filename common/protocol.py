"""Wire protocol shared by the Atom Lite firmware, the store gateway and the cloud.

Single source of truth for the two packed C-structs defined in the system rules.
Both `cloud/` and `gateway/` import this module so the formats can never drift.

Sensor packet (Atom -> PC), 12 bytes, little endian::

    offset  size  field
    0       1     header    0xAA
    1       1     dev_id
    2       2     seq       uint16, wraps at 65535
    4       2     temp      int16,  celsius * 100  (signed: sub-zero readings)
    6       2     hum       uint16, percent * 100
    8       2     light     uint16, raw lux
    10      1     flags     bitfield, see FLAG_*
    11      1     checksum  XOR of bytes 0..10

AC control packet (PC -> Atom), 8 bytes, little endian::

    offset  size  field
    0       1     header    0x55
    1       1     target_id
    2       1     power     0=off 1=on
    3       1     mode      see AC_MODES
    4       1     temp      celsius, 16..30
    5       1     fan       see AC_FANS
    6       1     checksum  XOR of bytes 0..5
    7       1     tail      0xEE
"""

from __future__ import annotations

import struct

__all__ = [
    "PacketError",
    "SENSOR_HEADER", "SENSOR_FMT", "SENSOR_SIZE",
    "AC_HEADER", "AC_TAIL", "AC_FMT", "AC_SIZE",
    "AC_MODES", "AC_MODE_NAMES", "AC_FANS", "AC_FAN_NAMES",
    "FLAG_AC_ON", "FLAG_IR_READY", "FLAG_SENSOR_FAULT",
    "xor_checksum", "decode_sensor_packet", "encode_sensor_packet",
    "encode_ac_packet", "decode_ac_packet",
    "normalize_mode", "normalize_fan", "clamp_target_temp",
]


class PacketError(ValueError):
    """Raised when a frame is malformed: bad length, header, tail or checksum."""


SENSOR_HEADER = 0xAA
SENSOR_FMT = "<BBHhHHBB"
SENSOR_SIZE = struct.calcsize(SENSOR_FMT)  # 12

AC_HEADER = 0x55
AC_TAIL = 0xEE
AC_FMT = "<BBBBBBBB"
AC_SIZE = struct.calcsize(AC_FMT)  # 8

# Sensor `flags` bitfield.
FLAG_AC_ON = 0x01        # device believes the AC is running
FLAG_IR_READY = 0x02     # IR-capable firmware flashed (SOTA stage 2 complete)
FLAG_SENSOR_FAULT = 0x04  # last read from the DHT/lux sensor failed

AC_MODES = {"cool": 0, "heat": 1, "dry": 2, "fan": 3, "auto": 4}
AC_MODE_NAMES = {v: k for k, v in AC_MODES.items()}

AC_FANS = {"auto": 0, "low": 1, "mid": 2, "high": 3}
AC_FAN_NAMES = {v: k for k, v in AC_FANS.items()}

TEMP_MIN, TEMP_MAX = 16, 30


def xor_checksum(data: bytes) -> int:
    """XOR of every byte -- the checksum used by both packet types."""
    chk = 0
    for byte in data:
        chk ^= byte
    return chk & 0xFF


def normalize_mode(mode) -> int:
    """Accept either the wire integer or the friendly name used by the web UI."""
    if isinstance(mode, str):
        try:
            return AC_MODES[mode.strip().lower()]
        except KeyError:
            raise PacketError(f"unknown AC mode: {mode!r}") from None
    if isinstance(mode, int) and mode in AC_MODE_NAMES:
        return mode
    raise PacketError(f"unknown AC mode: {mode!r}")


def normalize_fan(fan) -> int:
    if isinstance(fan, str):
        try:
            return AC_FANS[fan.strip().lower()]
        except KeyError:
            raise PacketError(f"unknown fan level: {fan!r}") from None
    if isinstance(fan, int) and fan in AC_FAN_NAMES:
        return fan
    raise PacketError(f"unknown fan level: {fan!r}")


def clamp_target_temp(temp) -> int:
    try:
        value = int(round(float(temp)))
    except (TypeError, ValueError):
        raise PacketError(f"invalid target temperature: {temp!r}") from None
    return max(TEMP_MIN, min(TEMP_MAX, value))


def decode_sensor_packet(raw: bytes) -> dict:
    """Decode one 12-byte sensor frame into engineering units."""
    if len(raw) != SENSOR_SIZE:
        raise PacketError(f"sensor packet must be {SENSOR_SIZE} bytes, got {len(raw)}")
    header, dev_id, seq, temp_raw, hum_raw, light, flags, chk = struct.unpack(SENSOR_FMT, raw)
    if header != SENSOR_HEADER:
        raise PacketError(f"bad sensor header 0x{header:02X}")
    if chk != xor_checksum(raw[:-1]):
        raise PacketError(f"sensor checksum mismatch (got 0x{chk:02X})")
    return {
        "dev_id": dev_id,
        "seq": seq,
        "temp": temp_raw / 100.0,
        "hum": hum_raw / 100.0,
        "light": light,
        "flags": flags,
        "ac_on": bool(flags & FLAG_AC_ON),
        "ir_ready": bool(flags & FLAG_IR_READY),
        "fault": bool(flags & FLAG_SENSOR_FAULT),
    }


def encode_sensor_packet(dev_id: int, seq: int, temp: float, hum: float,
                         light: int, flags: int = 0) -> bytes:
    """Build a 12-byte sensor frame (used by firmware and the device simulator)."""
    body = struct.pack(
        SENSOR_FMT[:-1],  # everything except the trailing checksum byte
        SENSOR_HEADER,
        dev_id & 0xFF,
        seq & 0xFFFF,
        max(-32768, min(32767, int(round(temp * 100)))),
        max(0, min(65535, int(round(hum * 100)))),
        max(0, min(65535, int(round(light)))),
        flags & 0xFF,
    )
    return body + bytes([xor_checksum(body)])


def encode_ac_packet(target_id: int, power, mode, temp, fan) -> bytes:
    """Build the 8-byte AC control frame."""
    body = struct.pack(
        AC_FMT[:-2],  # header .. fan, i.e. bytes 0..5
        AC_HEADER,
        int(target_id) & 0xFF,
        1 if int(power) else 0,
        normalize_mode(mode),
        clamp_target_temp(temp),
        normalize_fan(fan),
    )
    return body + bytes([xor_checksum(body), AC_TAIL])


def decode_ac_packet(raw: bytes) -> dict:
    """Decode an 8-byte AC control frame back into a friendly dict."""
    if len(raw) != AC_SIZE:
        raise PacketError(f"AC packet must be {AC_SIZE} bytes, got {len(raw)}")
    header, target_id, power, mode, temp, fan, chk, tail = struct.unpack(AC_FMT, raw)
    if header != AC_HEADER:
        raise PacketError(f"bad AC header 0x{header:02X}")
    if tail != AC_TAIL:
        raise PacketError(f"bad AC tail 0x{tail:02X}")
    if chk != xor_checksum(raw[:6]):
        raise PacketError(f"AC checksum mismatch (got 0x{chk:02X})")
    return {
        "target_id": target_id,
        "power": power,
        "mode": AC_MODE_NAMES.get(mode, mode),
        "temp": temp,
        "fan": AC_FAN_NAMES.get(fan, fan),
    }
