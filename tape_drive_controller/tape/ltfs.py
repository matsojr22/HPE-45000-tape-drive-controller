"""
LTFS (Linear Tape File System) support: format tape for LTFS using mkltfs,
mount/tape detection (tape_has_ltfs), and rsync backup to LTFS (run_ltfs_rsync).
Requires LTFS to be installed (e.g. build from LinearTapeFileSystem/ltfs or use IBM/Quantum packages).
"""
import os
import re
import shutil
import subprocess
import tempfile
import time
from typing import Callable, Optional

from .capacity import nst_to_sg
from .backup import TapeBackupError


def _compute_total_size(paths: list[str]) -> int:
    """Return total byte size of paths (sum of du -sb). Returns 0 on error or if unavailable."""
    if not paths:
        return 0
    try:
        r = subprocess.run(
            ["du", "-sb"] + paths,
            capture_output=True,
            text=True,
            timeout=3600,
        )
        if r.returncode != 0:
            return 0
        total = 0
        for line in (r.stdout or "").strip().splitlines():
            parts = line.split(None, 1)
            if parts:
                total += int(parts[0])
        return total
    except (FileNotFoundError, ValueError, subprocess.TimeoutExpired):
        return 0


def is_ltfs_available() -> bool:
    """Return True if mkltfs (and optionally mount.ltfs) are available in PATH."""
    return shutil.which("mkltfs") is not None


def format_ltfs(
    device: str,
    *,
    on_log: Optional[Callable[[str], None]] = None,
    force: bool = False,
) -> None:
    """
    Format the tape for LTFS using mkltfs. The device (e.g. /dev/nst0) is resolved to the
    corresponding SCSI generic device (e.g. /dev/sg0) which mkltfs requires.
    Raises TapeBackupError if mkltfs is not installed or format fails.
    """
    if not is_ltfs_available():
        raise TapeBackupError(
            "LTFS not installed. Install the LTFS package or build from source "
            "(e.g. https://github.com/LinearTapeFileSystem/ltfs)."
        )

    sg_device = nst_to_sg(device)
    if not sg_device:
        raise TapeBackupError(
            f"Cannot resolve tape device {device} to SCSI generic device (e.g. /dev/sg0). "
            "LTFS requires the sg device."
        )

    def log(line: str) -> None:
        if on_log:
            on_log(line)

    log("Formatting tape for LTFS (device: %s)…" % sg_device)
    cmd = ["mkltfs", "-d", sg_device]
    if force:
        cmd.append("-f")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    try:
        for line in proc.stdout:
            log(line.rstrip())
        proc.wait()
    except FileNotFoundError:
        raise TapeBackupError(
            "LTFS not installed; mkltfs not found. Build from source or use IBM/Quantum LTFS packages."
        ) from None
    if proc.returncode != 0:
        raise TapeBackupError("mkltfs failed with exit code %s" % proc.returncode)
    log("LTFS format completed.")


def is_ltfs_mount_available() -> bool:
    """Return True if ltfs (mount) and rsync are available in PATH."""
    return bool(shutil.which("ltfs") and shutil.which("rsync"))


def tape_has_ltfs(device: str) -> bool:
    """
    Return True if the tape in the drive appears to be LTFS-formatted.
    Tries to mount the tape with ltfs; if mount succeeds, unmounts and returns True.
    Returns False if device cannot be resolved to sg, ltfs not in PATH, or mount fails/timeout.
    """
    sg_device = nst_to_sg(device)
    if not sg_device or not shutil.which("ltfs"):
        return False
    mount_point = None
    ltfs_proc = None
    try:
        mount_point = tempfile.mkdtemp(prefix="ltfs_detect_")
        ltfs_proc = subprocess.Popen(
            ["ltfs", "-o", "devname=" + sg_device, mount_point],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        timeout = 20
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if ltfs_proc.poll() is not None:
                break
            if os.path.ismount(mount_point):
                break
            time.sleep(0.5)
        if not os.path.ismount(mount_point):
            return False
        # Unmount
        for cmd in (["fusermount", "-u", mount_point], ["umount", mount_point]):
            try:
                subprocess.run(cmd, capture_output=True, timeout=10)
                break
            except (FileNotFoundError, subprocess.TimeoutExpired):
                continue
        return True
    except Exception:
        return False
    finally:
        if ltfs_proc and ltfs_proc.poll() is None:
            ltfs_proc.terminate()
            try:
                ltfs_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                ltfs_proc.kill()
        if mount_point and os.path.exists(mount_point):
            try:
                if os.path.ismount(mount_point):
                    subprocess.run(
                        ["fusermount", "-u", mount_point],
                        capture_output=True,
                        timeout=5,
                    )
                os.rmdir(mount_point)
            except Exception:
                pass


# rsync --info=progress2: "105.45M 13% 602.83kB/s 0:02:50" or "1,234,567  12%" or similar
_RSYNC_PROGRESS2 = re.compile(r"^\s*([\d.,]+\s*[KMG]?)\s+(\d+)%")


def _parse_rsync_progress2(line: str) -> tuple[Optional[int], Optional[int]]:
    """Parse rsync progress2 line; return (bytes_approx, percentage) or (None, None)."""
    line = line.strip().replace("\r", "")
    m = _RSYNC_PROGRESS2.search(line)
    if not m:
        return None, None
    size_str, pct_str = m.group(1).strip(), m.group(2)
    try:
        pct = int(pct_str)
    except ValueError:
        return None, None
    # Parse size: 105.45M, 602.83k, 123, 1,234,567
    size_str = size_str.upper().replace(" ", "").replace(",", "")
    mult = 1
    if size_str.endswith("K"):
        mult = 1024
        size_str = size_str[:-1]
    elif size_str.endswith("M"):
        mult = 1024 * 1024
        size_str = size_str[:-1]
    elif size_str.endswith("G"):
        mult = 1024 * 1024 * 1024
        size_str = size_str[:-1]
    try:
        return int(float(size_str) * mult), pct
    except ValueError:
        return None, pct


def run_ltfs_rsync(
    device: str,
    paths: list[str],
    *,
    on_progress: Optional[Callable[[str], None]] = None,
    on_progress_update: Optional[Callable[[int, Optional[int], float], None]] = None,
    on_log: Optional[Callable[[str], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> None:
    """
    Mount the tape as LTFS, rsync the given paths to the mount, then unmount.
    Uses the same directory list as tar backup; each path appears as a top-level dir on tape.
    Raises TapeBackupError on missing ltfs/rsync, mount timeout, rsync failure, or cancel.
    """
    if not shutil.which("ltfs"):
        raise TapeBackupError(
            "LTFS (ltfs) not installed. Install LTFS to use Backup to LTFS (rsync)."
        )
    if not shutil.which("rsync"):
        raise TapeBackupError(
            "rsync not found. Install rsync to use Backup to LTFS (rsync)."
        )
    sg_device = nst_to_sg(device)
    if not sg_device:
        raise TapeBackupError(
            "Cannot resolve tape device to SCSI generic device (e.g. /dev/sg0). LTFS requires the sg device."
        )
    total_bytes = _compute_total_size(paths)
    if total_bytes == 0:
        total_bytes = None
    mount_point = None
    ltfs_proc = None

    def log(line: str) -> None:
        if on_log:
            on_log(line)

    try:
        mount_point = tempfile.mkdtemp(prefix="ltfs_tape_")
        if on_progress:
            on_progress("Mounting LTFS…")
        ltfs_proc = subprocess.Popen(
            ["ltfs", "-o", "devname=" + sg_device, mount_point],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        timeout = 30
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if ltfs_proc.poll() is not None:
                if os.path.ismount(mount_point):
                    break  # daemonized; mount is valid
                err = ""
                if ltfs_proc.stderr:
                    try:
                        err = ltfs_proc.stderr.read()
                    except (OSError, ValueError):
                        pass
                err = (err or "").strip()
                if err:
                    lines = err.splitlines()
                    if len(lines) > 30:
                        lines = lines[-30:]
                    err = "\nltfs stderr: " + "\n".join(lines)
                else:
                    err = ""
                raise TapeBackupError("LTFS mount failed (ltfs exited early)." + err)
            if os.path.ismount(mount_point):
                break
            time.sleep(0.5)
        if not os.path.ismount(mount_point):
            ltfs_proc.terminate()
            ltfs_proc.wait(timeout=5)
            err = ""
            if ltfs_proc.stderr:
                try:
                    err = ltfs_proc.stderr.read().strip()
                except (OSError, ValueError):
                    pass
            err = "\nltfs stderr: " + err if err else ""
            raise TapeBackupError("LTFS mount timed out." + err)
        if on_progress_update:
            on_progress_update(0, total_bytes, 0.0)
        if on_progress:
            on_progress("Copying to tape…")
        log("Running rsync to %s" % mount_point)
        cmd = ["rsync", "-a", "--info=progress2"] + paths + [mount_point + "/"]
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        start_time = time.monotonic()
        bytes_transferred = 0
        assert proc.stderr is not None
        for line in proc.stderr:
            if cancel_check and cancel_check():
                proc.terminate()
                proc.wait(timeout=10)
                raise TapeBackupError("Backup to LTFS cancelled by user")
            parsed_bytes, pct = _parse_rsync_progress2(line)
            elapsed = time.monotonic() - start_time
            if pct is not None and total_bytes and total_bytes > 0:
                bytes_transferred = int(total_bytes * pct / 100)
            elif parsed_bytes is not None:
                bytes_transferred = parsed_bytes
            if on_progress_update:
                on_progress_update(bytes_transferred, total_bytes, elapsed)
            if on_log and line.strip():
                log(line.strip())
        proc.wait()
        if proc.returncode != 0:
            raise TapeBackupError("rsync failed with exit code %s" % proc.returncode)
        if on_progress_update and total_bytes:
            on_progress_update(total_bytes, total_bytes, time.monotonic() - start_time)
        if on_progress:
            on_progress("Backup to LTFS completed.")
    finally:
        if mount_point and os.path.exists(mount_point):
            if os.path.ismount(mount_point):
                try:
                    subprocess.run(
                        ["fusermount", "-u", mount_point],
                        capture_output=True,
                        timeout=10,
                    )
                except FileNotFoundError:
                    subprocess.run(
                        ["umount", mount_point],
                        capture_output=True,
                        timeout=10,
                    )
            try:
                os.rmdir(mount_point)
            except OSError:
                pass
        if ltfs_proc and ltfs_proc.poll() is None:
            ltfs_proc.terminate()
            try:
                ltfs_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                ltfs_proc.kill()
