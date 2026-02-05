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
import threading
import time
from typing import Callable, Optional

try:
    import pty
except ImportError:
    pty = None  # type: ignore[assignment]

from .capacity import nst_to_sg
from .backup import TapeBackupError


def _compute_total_size(paths: list[str], timeout: int = 3600) -> int:
    """Return total byte size of paths (sum of du -sb). Returns 0 on error or if unavailable."""
    if not paths:
        return 0
    try:
        r = subprocess.run(
            ["du", "-sb"] + paths,
            capture_output=True,
            text=True,
            timeout=timeout,
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
                # Process may have daemonized; keep waiting for mount to appear
                if os.path.ismount(mount_point):
                    break
            elif os.path.ismount(mount_point):
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


def unmount_leftover_ltfs_mounts(on_log: Optional[Callable[[str], None]] = None) -> int:
    """
    Find and unmount any leftover LTFS mounts under /tmp/ltfs_tape_* (e.g. from a crashed run).
    Returns the number of mounts unmounted. Optional on_log is called for each log line.
    """
    count = 0
    try:
        candidates = list(os.path.join("/tmp", d) for d in os.listdir("/tmp") if d.startswith("ltfs_tape_"))
    except OSError:
        return 0
    for path in candidates:
        if not os.path.isdir(path):
            continue
        if not os.path.ismount(path):
            continue
        if on_log:
            on_log("Unmounting leftover LTFS mount: %s" % path)
        for cmd in (["fusermount", "-u", path], ["umount", path]):
            if not os.path.ismount(path):
                break
            try:
                subprocess.run(cmd, capture_output=True, timeout=10)
            except (FileNotFoundError, subprocess.TimeoutExpired):
                continue
        if not os.path.ismount(path):
            count += 1
            try:
                os.rmdir(path)
            except OSError:
                pass
    return count


def unmount_ltfs(
    mount_point: str,
    ltfs_proc: Optional[subprocess.Popen] = None,
    *,
    on_log: Optional[Callable[[str], None]] = None,
) -> None:
    """
    Unmount an LTFS mount and optionally terminate the LTFS process.
    Calls fusermount -u (or umount) on mount_point; if ltfs_proc is provided and
    still running, terminate() and wait(). Optional on_log for "Unmounting…" message.
    """
    def log(line: str) -> None:
        if on_log:
            on_log(line)

    if not mount_point or not os.path.exists(mount_point):
        return
    if os.path.ismount(mount_point):
        log("Unmounting LTFS: %s" % mount_point)
        for cmd in (["fusermount", "-u", mount_point], ["umount", mount_point]):
            try:
                subprocess.run(cmd, capture_output=True, timeout=10)
                break
            except (FileNotFoundError, subprocess.TimeoutExpired):
                continue
    if ltfs_proc is not None and ltfs_proc.poll() is None:
        ltfs_proc.terminate()
        try:
            ltfs_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            ltfs_proc.kill()
    if mount_point and os.path.exists(mount_point) and not os.path.ismount(mount_point):
        try:
            os.rmdir(mount_point)
        except OSError:
            pass


def mount_ltfs_only(
    device: str,
    *,
    on_log: Optional[Callable[[str], None]] = None,
    mount_point_holder: Optional[list] = None,
    process_holder: Optional[list] = None,
) -> None:
    """
    Mount the tape as LTFS (no rsync). Unmount leftovers, create temp dir,
    run ltfs -o devname=<sg> <mount_point>, wait for mount, set mount_point_holder[0]
    and append ltfs_proc to process_holder. Raise TapeBackupError on failure.
    """
    if not shutil.which("ltfs"):
        raise TapeBackupError(
            "LTFS (ltfs) not installed. Install LTFS to mount tape as LTFS."
        )
    sg_device = nst_to_sg(device)
    if not sg_device:
        raise TapeBackupError(
            "Cannot resolve tape device to SCSI generic device (e.g. /dev/sg0). LTFS requires the sg device."
        )

    def log(line: str) -> None:
        if on_log:
            on_log(line)

    unmount_leftover_ltfs_mounts(on_log=log)
    mount_point = tempfile.mkdtemp(prefix="ltfs_tape_")
    log("Mounting LTFS at %s" % mount_point)
    ltfs_proc = subprocess.Popen(
        ["ltfs", "-o", "devname=" + sg_device, mount_point],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process_holder is not None:
        process_holder.append(ltfs_proc)
    timeout = 30
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            if ltfs_proc.poll() is not None:
                if os.path.ismount(mount_point):
                    break
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
        if mount_point_holder is not None:
            mount_point_holder[0] = mount_point
        log("LTFS mounted at %s" % mount_point)
    except Exception:
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
        if ltfs_proc.poll() is None:
            ltfs_proc.terminate()
            try:
                ltfs_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                ltfs_proc.kill()
        if process_holder and process_holder[-1] is ltfs_proc:
            process_holder.pop()
        raise


def run_ltfs_rsync(
    device: str,
    paths: list[str],
    *,
    on_progress: Optional[Callable[[str], None]] = None,
    on_progress_update: Optional[Callable[[int, Optional[int], float], None]] = None,
    on_log: Optional[Callable[[str], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
    process_holder: Optional[list] = None,
    mount_point_holder: Optional[list] = None,
) -> None:
    """
    Mount the tape as LTFS, rsync the given paths to the mount, then unmount.
    Uses the same directory list as tar backup; each path appears as a top-level dir on tape.
    Raises TapeBackupError on missing ltfs/rsync, mount timeout, rsync failure, or cancel.
    If process_holder is provided, [ltfs_proc, rsync_proc] are appended so the caller can
    terminate them on exit (e.g. when closing the app).
    If mount_point_holder is provided (e.g. [None]), it is set to the mount path when mounted
    and cleared in finally so the caller can unmount cleanly before terminating (e.g. on close).
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
        unmount_leftover_ltfs_mounts(on_log=log)
        mount_point = tempfile.mkdtemp(prefix="ltfs_tape_")
        if on_progress:
            on_progress("Mounting LTFS…")
        ltfs_proc = subprocess.Popen(
            ["ltfs", "-o", "devname=" + sg_device, mount_point],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        if process_holder is not None:
            process_holder.append(ltfs_proc)
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
        if mount_point_holder is not None:
            mount_point_holder[0] = mount_point
        if on_progress_update:
            on_progress_update(0, total_bytes, 0.0)
        if on_progress:
            on_progress("Copying to tape…")
        log("Running rsync to %s" % mount_point)
        cmd = [
            "rsync", "-a", "--partial", "--append-verify",
            "--outbuf=L",
            "--info=progress2,flist2,stats2",
        ] + paths + [mount_point + "/"]
        log(" ".join(cmd))
        use_pty = pty is not None
        master_fd = None
        slave_fd = None
        try:
            if use_pty:
                master_fd, slave_fd = pty.openpty()
        except OSError:
            use_pty = False
            if master_fd is not None:
                try:
                    os.close(master_fd)
                except OSError:
                    pass
                master_fd = None
            slave_fd = None

        if use_pty and master_fd is not None:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=slave_fd,
            )
            try:
                os.close(slave_fd)
            except OSError:
                pass
            slave_fd = -1
        else:
            if master_fd is not None:
                try:
                    os.close(master_fd)
                except OSError:
                    pass
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

        if process_holder is not None:
            process_holder.append(proc)
        start_time = time.monotonic()
        bytes_ref = [0]  # mutable so timer thread and read loop can share
        seen_progress2 = [False]  # first progress2 -> set "Copying to tape…"
        seen_non_progress2 = [False]  # first other line -> set "Building file list…"
        stop_timer = threading.Event()

        def timer_tick():
            while not stop_timer.wait(1.0):
                if proc.poll() is not None:
                    return
                elapsed = time.monotonic() - start_time
                if mount_point:
                    dest_size = _compute_total_size([mount_point], timeout=10)
                    if dest_size > 0:
                        bytes_ref[0] = max(bytes_ref[0], dest_size)
                    if total_bytes and total_bytes > 0:
                        bytes_ref[0] = min(bytes_ref[0], total_bytes)
                if on_progress_update:
                    on_progress_update(bytes_ref[0], total_bytes, elapsed)

        timer_thread = threading.Thread(target=timer_tick, daemon=True)
        timer_thread.start()
        try:
            stderr_buffer = ""
            chunk_size = 4096
            while True:
                if cancel_check and cancel_check():
                    proc.terminate()
                    proc.wait(timeout=10)
                    raise TapeBackupError("Backup to LTFS cancelled by user")
                if use_pty and master_fd is not None:
                    try:
                        chunk_bytes = os.read(master_fd, chunk_size)
                    except OSError:
                        chunk_bytes = b""
                    chunk = chunk_bytes.decode("utf-8", errors="replace") if chunk_bytes else ""
                else:
                    chunk = proc.stderr.read(chunk_size) if proc.stderr else ""
                if chunk:
                    stderr_buffer += chunk
                parts = re.split(r"[\r\n]+", stderr_buffer)
                if chunk or proc.poll() is not None:
                    stderr_buffer = parts.pop() if parts else ""
                else:
                    continue
                for segment in parts:
                    seg = segment.strip()
                    if not seg:
                        continue
                    parsed_bytes, pct = _parse_rsync_progress2(segment)
                    elapsed = time.monotonic() - start_time
                    if pct is not None or parsed_bytes is not None:
                        if not seen_progress2[0] and on_progress:
                            seen_progress2[0] = True
                            on_progress("Copying to tape…")
                        if pct is not None and total_bytes and total_bytes > 0:
                            bytes_ref[0] = int(total_bytes * pct / 100)
                        elif parsed_bytes is not None:
                            bytes_ref[0] = parsed_bytes
                        if on_progress_update:
                            on_progress_update(bytes_ref[0], total_bytes, elapsed)
                    else:
                        if not seen_non_progress2[0] and on_progress:
                            seen_non_progress2[0] = True
                            on_progress("Building file list…")
                        if on_log:
                            log(seg)
                if not chunk and proc.poll() is not None:
                    if stderr_buffer.strip():
                        parsed_bytes, pct = _parse_rsync_progress2(stderr_buffer)
                        if pct is not None or parsed_bytes is not None:
                            if pct is not None and total_bytes and total_bytes > 0:
                                bytes_ref[0] = int(total_bytes * pct / 100)
                            elif parsed_bytes is not None:
                                bytes_ref[0] = parsed_bytes
                            if on_progress_update:
                                on_progress_update(
                                    bytes_ref[0], total_bytes,
                                    time.monotonic() - start_time,
                                )
                        elif on_log:
                            log(stderr_buffer.strip())
                    break
            proc.wait()
        finally:
            stop_timer.set()
            timer_thread.join(timeout=2.0)
            if master_fd is not None:
                try:
                    os.close(master_fd)
                except OSError:
                    pass
        if proc.returncode != 0:
            raise TapeBackupError("rsync failed with exit code %s" % proc.returncode)
        if on_progress_update and total_bytes:
            on_progress_update(total_bytes, total_bytes, time.monotonic() - start_time)
        if on_progress:
            on_progress("Backup to LTFS completed.")
    finally:
        if mount_point_holder is not None:
            mount_point_holder[0] = None
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
