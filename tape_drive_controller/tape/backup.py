"""
Run backup to tape: rewind via mt, then stream tar to device.
Supports progress callbacks and cancel.
"""
import queue
import re
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional

# Checkpoint every N files for progress (tar --checkpoint)
CHECKPOINT_EVERY = 500

# Default timeout for mt erase (long erase can take hours on LTO)
ERASE_TIMEOUT_SEC = 4 * 3600  # 4 hours

TapeBackupError = type("TapeBackupError", (Exception,), {})


def _run_mt(device: str, command: str) -> tuple[int, str, str]:
    """Run mt -f <device> <command>. Returns (returncode, stdout, stderr)."""
    r = subprocess.run(
        ["mt", "-f", device, command],
        capture_output=True,
        text=True,
        timeout=60,
    )
    return r.returncode, r.stdout or "", r.stderr or ""


def rewind(device: str) -> None:
    """Rewind the tape. Raises TapeBackupError on failure."""
    code, out, err = _run_mt(device, "rewind")
    if code != 0:
        raise TapeBackupError(f"mt rewind failed: {err or out or f'exit {code}'}")


def erase(
    device: str,
    *,
    on_log: Optional[Callable[[str], None]] = None,
    timeout: int = ERASE_TIMEOUT_SEC,
) -> None:
    """
    Long erase the tape (mt erase). Writes a pattern to the tape; can take many hours.
    Not abortable on most drives. Raises TapeBackupError on failure.
    """
    def log(line: str) -> None:
        if on_log:
            on_log(line)

    log("Erase started (this can take several hours and cannot be aborted).")
    proc = subprocess.Popen(
        ["mt", "-f", device, "erase"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    read_done = threading.Event()

    def read_stdout() -> None:
        try:
            for line in proc.stdout:
                log(line.rstrip())
        finally:
            read_done.set()

    reader = threading.Thread(target=read_stdout, daemon=True)
    reader.start()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        read_done.wait(timeout=5)
        raise TapeBackupError("mt erase timed out (tape may still be erasing on the drive).")
    read_done.wait(timeout=5)
    if proc.returncode != 0:
        raise TapeBackupError(f"mt erase failed with exit code {proc.returncode}")
    log("Erase completed.")


def tape_status(device: str) -> str:
    """Return mt status output for the device."""
    code, out, err = _run_mt(device, "status")
    if code != 0:
        return f"mt status failed (exit {code}):\n{err or out}"
    return out or err or "(no output)"


def forward_space_files(device: str, n: int) -> None:
    """Forward space n tape files (mt fsf n). Raises TapeBackupError on failure."""
    if n <= 0:
        return
    code, out, err = _run_mt(device, "fsf " + str(n))
    if code != 0:
        raise TapeBackupError(f"mt fsf {n} failed: {err or out or f'exit {code}'}")


# GNU tar -tv output: -rw-r--r-- user/group 12345 2024-01-15 12:00 path/to/file
_TAR_LIST_LINE = re.compile(
    r"^(.{10})\s+\S+/\S+\s+(\d+)\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}\s+(.*)$"
)


@dataclass
class TapeEntry:
    """A single entry from listing tape contents (tar archive member)."""
    path: str
    size: int
    is_dir: bool


def list_tape_contents(
    device: str,
    *,
    use_gzip: bool = False,
    skip_rewind: bool = False,
    on_progress: Optional[Callable[[str], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> list[TapeEntry]:
    """
    List contents of the tar archive on the tape. Rewinds first unless skip_rewind.
    Returns list of TapeEntry (path, size, is_dir). Runs tar -tvf (or -tzvf).
    Raises TapeBackupError on mt/tar failure. cancel_check() stops reading if True.
    """
    if not skip_rewind:
        rewind(device)

    cmd = ["tar", "-tvf", device]
    if use_gzip:
        cmd = ["tar", "-tzvf", device]

    entries: list[TapeEntry] = []
    proc: Optional[subprocess.Popen] = None
    count = 0

    line_queue: "queue.Queue[Optional[str]]" = queue.Queue()

    def reader() -> None:
        assert proc is not None and proc.stdout is not None
        try:
            for line in proc.stdout:
                line_queue.put(line)
        finally:
            line_queue.put(None)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        reader_thread = threading.Thread(target=reader, daemon=True)
        reader_thread.start()
        cancel_poll_seconds = 0.5
        while True:
            try:
                line = line_queue.get(timeout=cancel_poll_seconds)
            except queue.Empty:
                if cancel_check and cancel_check():
                    proc.terminate()
                    proc.wait(timeout=10)
                    raise TapeBackupError("List tape contents cancelled by user")
                continue
            if line is None:
                break
            line = line.rstrip()
            match = _TAR_LIST_LINE.match(line)
            if match:
                perms, size_str, path = match.group(1), match.group(2), match.group(3)
                size = int(size_str)
                is_dir = perms.startswith("d")
                entries.append(TapeEntry(path=path, size=size, is_dir=is_dir))
                count += 1
                if on_progress and count % 100 == 0:
                    on_progress(f"Reading… {count} entries")
        proc.wait()
        if proc.returncode != 0:
            raise TapeBackupError(f"tar list exited with code {proc.returncode}")
        if on_progress:
            on_progress(f"Read {count} entries.")
    except FileNotFoundError as e:
        raise TapeBackupError(f"Required command not found (tar): {e}") from e
    except TapeBackupError:
        raise
    except Exception as e:
        if proc and proc.poll() is None:
            proc.terminate()
        raise TapeBackupError(str(e)) from e

    return entries


# Regex to parse bytes written from tar checkpoint line: "W: 512000 (...)"
_CHECKPOINT_W_BYTES = re.compile(r"W:\s*(\d+)")
# Regex to parse bytes read from tar checkpoint line (extract): "R: 512000 (...)"
_CHECKPOINT_R_BYTES = re.compile(r"R:\s*(\d+)")


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


def run_backup(
    device: str,
    paths: list[str],
    *,
    use_gzip: bool = False,
    skip_rewind: bool = False,
    max_tape_bytes: Optional[int] = None,
    on_progress: Optional[Callable[[str], None]] = None,
    on_progress_update: Optional[Callable[[int, Optional[int], float], None]] = None,
    on_log: Optional[Callable[[str], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> None:
    """
    Rewind tape (unless skip_rewind), then write paths to tape as a single tar archive.
    max_tape_bytes: if set, backup is aborted when total source size exceeds this (safety check).
    on_progress(message): e.g. "Writing…", "1234 records written"
    on_progress_update(bytes_written, total_bytes, elapsed_sec): optional; for progress bar and ETA.
    on_log(line): raw tar/checkpoint lines for the log.
    cancel_check(): if returns True, backup is aborted (subprocess killed).
    skip_rewind: set True for testing (e.g. writing to a file instead of tape).
    Raises TapeBackupError on mt or tar failure, or when size exceeds max_tape_bytes.
    """
    total_bytes: Optional[int] = _compute_total_size(paths)
    if total_bytes == 0:
        total_bytes = None
    if on_progress_update:
        on_progress_update(0, total_bytes, 0.0)

    if max_tape_bytes is not None and max_tape_bytes > 0 and total_bytes is not None:
        if total_bytes > max_tape_bytes:
            raise TapeBackupError(
                f"Backup size ({total_bytes:,} bytes, ~{total_bytes / (1024**3):.1f} GB) "
                f"exceeds tape capacity limit ({max_tape_bytes:,} bytes, ~{max_tape_bytes / (1024**3):.1f} GB). "
                "Increase the tape capacity setting or remove directories."
            )

    if not skip_rewind:
        rewind(device)

    # tar -cvf /dev/nst0 [paths] or -z for gzip
    cmd = ["tar", "-cvf", device] + paths
    if use_gzip:
        cmd = ["tar", "-zcvf", device] + paths

    # Checkpoint with %T for bytes written (W: NNNN)
    cmd.extend([
        f"--checkpoint={CHECKPOINT_EVERY}",
        "--checkpoint-action=echo=CHECKPOINT %u %T",
    ])

    proc: Optional[subprocess.Popen] = None
    file_count = 0
    bytes_written = 0
    start_time = time.monotonic()

    def log(line: str) -> None:
        nonlocal file_count, bytes_written
        if "CHECKPOINT " in line:
            try:
                parts = line.split("CHECKPOINT ", 1)[1].split(None, 1)
                n = int(parts[0])
                file_count = n * CHECKPOINT_EVERY
            except (IndexError, ValueError):
                pass
            match = _CHECKPOINT_W_BYTES.search(line)
            if match:
                bytes_written = int(match.group(1))
            elapsed = time.monotonic() - start_time
            if on_progress_update:
                on_progress_update(bytes_written, total_bytes, elapsed)
            if on_progress:
                on_progress(f"Writing… {file_count} records written")
        if on_log:
            on_log(line)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            if cancel_check and cancel_check():
                proc.terminate()
                proc.wait(timeout=10)
                raise TapeBackupError("Backup cancelled by user")
            log(line)
        proc.wait()
        if proc.returncode != 0:
            raise TapeBackupError(f"tar exited with code {proc.returncode}")
        if on_progress:
            on_progress(f"Completed. {file_count} records written.")
    except FileNotFoundError as e:
        raise TapeBackupError(f"Required command not found (mt/tar): {e}") from e
    except TapeBackupError:
        raise
    except Exception as e:
        if proc and proc.poll() is None:
            proc.terminate()
        raise TapeBackupError(str(e)) from e


def run_restore(
    device: str,
    destination: str,
    *,
    use_gzip: bool = False,
    skip_rewind: bool = False,
    archive_number: int = 1,
    on_progress: Optional[Callable[[str], None]] = None,
    on_progress_update: Optional[Callable[[int, Optional[int], float], None]] = None,
    on_log: Optional[Callable[[str], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> None:
    """
    Rewind tape (unless skip_rewind), then optionally skip to archive_number (1-based),
    then extract from tape to destination.
    archive_number: 1 = first archive (default); N > 1 = rewind then fsf(N-1) then extract.
    on_progress(message): e.g. "Extracting… N records"
    on_progress_update(bytes_read, total_bytes, elapsed_sec): optional; total_bytes is always None.
    on_log(line): raw tar/checkpoint lines for the log.
    cancel_check(): if returns True, restore is aborted.
    Raises TapeBackupError on mt or tar failure.
    """
    if on_progress_update:
        on_progress_update(0, None, 0.0)
    if not skip_rewind:
        rewind(device)
    if archive_number > 1:
        forward_space_files(device, archive_number - 1)

    cmd = ["tar", "-xvf", device, "-C", destination]
    if use_gzip:
        cmd = ["tar", "-xzvf", device, "-C", destination]
    cmd.extend([
        f"--checkpoint={CHECKPOINT_EVERY}",
        "--checkpoint-action=echo=CHECKPOINT %u %T",
    ])

    proc: Optional[subprocess.Popen] = None
    file_count = 0
    bytes_read = 0
    start_time = time.monotonic()

    def log(line: str) -> None:
        nonlocal file_count, bytes_read
        if "CHECKPOINT " in line:
            try:
                parts = line.split("CHECKPOINT ", 1)[1].split(None, 1)
                n = int(parts[0])
                file_count = n * CHECKPOINT_EVERY
            except (IndexError, ValueError):
                pass
            match = _CHECKPOINT_R_BYTES.search(line)
            if match:
                bytes_read = int(match.group(1))
            elapsed = time.monotonic() - start_time
            if on_progress_update:
                on_progress_update(bytes_read, None, elapsed)
            if on_progress:
                on_progress(f"Extracting… {file_count} records")
        if on_log:
            on_log(line)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            if cancel_check and cancel_check():
                proc.terminate()
                proc.wait(timeout=10)
                raise TapeBackupError("Restore cancelled by user")
            log(line)
        proc.wait()
        if proc.returncode != 0:
            raise TapeBackupError(f"tar exited with code {proc.returncode}")
        if on_progress:
            on_progress(f"Completed. {file_count} records extracted.")
    except FileNotFoundError as e:
        raise TapeBackupError(f"Required command not found (mt/tar): {e}") from e
    except TapeBackupError:
        raise
    except Exception as e:
        if proc and proc.poll() is None:
            proc.terminate()
        raise TapeBackupError(str(e)) from e
