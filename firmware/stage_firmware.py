"""Stage the atom_ac OTA image where the gateway serves it.

The gateway serves exactly one image, ``<data-dir>\\firmware\\atom_ac.bin``.
There is nothing brand-specific to stage: the device replays timings learned
from the customer's own remote, so the same binary drives every air
conditioner.

    pio run -e atom_ac              # build first (or pass --build)
    python stage_firmware.py        # stage into %ProgramData%\\AtomAir\\firmware
    python stage_firmware.py --data-dir D:\\atomair-data   # match --data-dir
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
IMAGE = HERE / ".pio" / "build" / "atom_ac" / "firmware.bin"


def default_data_dir() -> Path:
    program_data = os.environ.get("ProgramData", r"C:\ProgramData")
    return Path(program_data) / "AtomAir"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=default_data_dir(),
                    help="gateway data dir (default: %%ProgramData%%\\AtomAir)")
    ap.add_argument("--build", action="store_true",
                    help="run 'pio run -e atom_ac' first")
    args = ap.parse_args()

    if args.build:
        subprocess.run(["pio", "run", "-e", "atom_ac"], cwd=HERE, check=True)

    if not IMAGE.exists():
        sys.exit(f"no atom_ac image at {IMAGE} — run: pio run -e atom_ac")

    firmware_dir = args.data_dir / "firmware"
    firmware_dir.mkdir(parents=True, exist_ok=True)

    # Protocol-named copies from before learned-remote-only control. Leaving
    # them behind wastes ~10MB and invites a deploy of a stale image.
    stale = sorted(firmware_dir.glob("atom_ac_*.bin"))
    for old in stale:
        old.unlink()
    if stale:
        print(f"removed {len(stale)} obsolete protocol-named image(s)")

    dest = firmware_dir / "atom_ac.bin"
    shutil.copy2(IMAGE, dest)
    print(f"staged {dest} ({IMAGE.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
