"""Stage the atom_ac OTA image where the gateway serves it.

The gateway resolves a SOTA deploy to ``<data-dir>\\firmware\\atom_ac_<protocol>.bin``
(protocol lower-cased). The atom_ac build is universal — IRac speaks every
protocol in the cloud's model catalog and the device learns *which* one from
the OTA command it stores in NVS — so one image is copied under every
protocol-specific name the gateway may look for.

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

# Keep in sync with the seeded ac_models registry in cloud/cloud_server.py.
# "RAW" is the universal image for learned-remote (raw IR replay) deploys.
PROTOCOLS = ["SAMSUNG_AC", "LG2", "LG", "DAIKIN", "DAIKIN216",
             "MITSUBISHI_AC", "CARRIER_AC", "RAW"]

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
    for proto in PROTOCOLS:
        dest = firmware_dir / f"atom_ac_{proto.lower()}.bin"
        shutil.copy2(IMAGE, dest)
        print(f"staged {dest}")
    print(f"\n{len(PROTOCOLS)} images staged from {IMAGE.name} "
          f"({IMAGE.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
