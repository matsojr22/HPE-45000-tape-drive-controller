"""
Discover SCSI tape devices on Linux (/dev/nst*, optionally with lsscsi labels).
"""
import glob
import os
import stat
import subprocess
from dataclasses import dataclass
from typing import Optional


@dataclass
class TapeDevice:
    """A tape device path with optional friendly label from lsscsi."""

    path: str
    label: Optional[str] = None

    def display_name(self) -> str:
        if self.label:
            return f"{self.path} — {self.label}"
        return self.path


def _is_tape_char_device(path: str) -> bool:
    try:
        mode = os.stat(path).st_mode
        return stat.S_ISCHR(mode)
    except OSError:
        return False


def _get_lsscsi_labels() -> dict[str, str]:
    """Run lsscsi and return mapping from /dev/nst* path to model string (e.g. HP Ultrium 2-SCSI)."""
    result: dict[str, str] = {}
    try:
        out = subprocess.run(
            ["lsscsi"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if out.returncode != 0:
            return result
        # Lines like: [2:0:0:0]    tape    HP       Ultrium 2-SCSI   F6CH    /dev/st0
        for line in out.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 4 and "tape" in parts:
                dev = parts[-1]
                if dev.startswith("/dev/st") and dev[7:].isdigit():
                    nst = f"/dev/nst{dev[7:]}"
                    idx_tape = next((i for i, p in enumerate(parts) if p == "tape"), None)
                    if idx_tape is not None and idx_tape + 1 < len(parts):
                        # Everything between "tape" and /dev/ is vendor/model
                        rest = parts[idx_tape + 1 : -1]
                        if rest:
                            result[nst] = " ".join(rest)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return result


def list_tape_devices() -> list[TapeDevice]:
    """
    List available tape devices (non-rewind /dev/nst*).
    Optionally enriches with model name from lsscsi if available.
    """
    labels = _get_lsscsi_labels()
    devices: list[TapeDevice] = []
    for path in sorted(glob.glob("/dev/nst*")):
        if _is_tape_char_device(path):
            devices.append(TapeDevice(path=path, label=labels.get(path)))
    return devices
