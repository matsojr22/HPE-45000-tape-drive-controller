"""
Query tape capacity via sg3-utils (sg_read_attr or sg_logs).
Uses maximum (total) capacity only, since the app rewinds and overwrites the whole tape.
Optional: requires system package sg3-utils.
"""
import os
import re
import subprocess
from typing import Optional

# sg_read_attr: "Maximum capacity in partition [MiB]: 18874368"
_SG_READ_ATTR_MAXIMUM_RE = re.compile(
    r"Maximum capacity in partition\s*\[MiB\]\s*:\s*(\d+)",
    re.IGNORECASE,
)
# sg_logs: "Main partition maximum capacity (in MiB): ..." or similar
_SG_LOGS_MAXIMUM_RE = re.compile(
    r"(?:Main partition )?maximum capacity\s*\(?\s*in MiB\)?\s*:?\s*(\d+)",
    re.IGNORECASE,
)

# Some LTO-9 drives report maximum as ~1/161 of actual (e.g. 120 GB instead of ~18 TB).
# When reported capacity is in this range, scale by LTO9_MISREPORT_FACTOR to correct.
LTO9_MISREPORT_MIN_GB = 50
LTO9_MISREPORT_MAX_GB = 500
LTO9_MISREPORT_FACTOR = 161


def _nst_to_sg(device: str) -> Optional[str]:
    """Resolve /dev/nst0 to /dev/sgN using sysfs. Returns None if not found."""
    if not device.startswith("/dev/nst"):
        return None
    name = os.path.basename(device)  # e.g. nst0
    generic = f"/sys/class/scsi_tape/{name}/device/generic"
    try:
        target = os.readlink(generic)
        sg_name = os.path.basename(target)  # sg0
        return f"/dev/{sg_name}"
    except (OSError, ValueError):
        return None


def nst_to_sg(device: str) -> Optional[str]:
    """
    Resolve tape device (e.g. /dev/nst0) to the corresponding SCSI generic device (e.g. /dev/sg0).
    Required for LTFS (mkltfs, mount.ltfs) which use the sg device. Returns None if not found.
    """
    return _nst_to_sg(device)


def _query_sg_read_attr(device: str) -> Optional[int]:
    """Run sg_read_attr and parse maximum capacity in partition [MiB]. Returns bytes or None."""
    try:
        r = subprocess.run(
            ["sg_read_attr", device],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if r.returncode != 0:
            return None
        match = _SG_READ_ATTR_MAXIMUM_RE.search(r.stdout or "")
        if match:
            mib = int(match.group(1))
            return mib * 1024 * 1024
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        return None


def _query_sg_logs(device: str) -> Optional[int]:
    """Run sg_logs -a and parse maximum capacity in partition [MiB]. Returns bytes or None."""
    try:
        r = subprocess.run(
            ["sg_logs", "-a", device],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if r.returncode != 0:
            return None
        text = (r.stdout or "") + (r.stderr or "")
        match = _SG_LOGS_MAXIMUM_RE.search(text)
        if match:
            mib = int(match.group(1))
            return mib * 1024 * 1024
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        return None


def _apply_lto9_misreport_correction(bytes_val: int) -> int:
    """If value is in known LTO-9 misreport range (50-500 GB), scale by 161 to correct."""
    gb = bytes_val / (1024**3)
    if LTO9_MISREPORT_MIN_GB <= gb <= LTO9_MISREPORT_MAX_GB:
        return bytes_val * LTO9_MISREPORT_FACTOR
    return bytes_val


def query_remaining_capacity_bytes(device: str) -> Optional[int]:
    """
    Query tape capacity on the device (e.g. /dev/nst0).

    Returns **maximum (total) capacity** in bytes, not remaining. The backup app
    rewinds and overwrites the whole tape, so the safety check uses total capacity.
    Uses sg3-utils: sg_logs first, then sg_read_attr. Tries /dev/nst* first,
    then /dev/sgN if resolved from sysfs.

    Returns None if the query fails (tool missing, device doesn't support it, or parse error).
    """
    devices_to_try = [device]
    sg_dev = _nst_to_sg(device)
    if sg_dev:
        devices_to_try.append(sg_dev)
    for dev in devices_to_try:
        result = _query_sg_logs(dev)
        if result is not None:
            return _apply_lto9_misreport_correction(result)
    for dev in devices_to_try:
        result = _query_sg_read_attr(dev)
        if result is not None:
            return _apply_lto9_misreport_correction(result)
    return None
