"""Tests for tape device listing and backup pipeline (no real tape required)."""
import io
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tape_drive_controller.tape.list_devices import list_tape_devices
from tape_drive_controller.tape.backup import run_backup, erase, TapeBackupError
from tape_drive_controller.tape.capacity import nst_to_sg
from tape_drive_controller.tape.ltfs import is_ltfs_available, format_ltfs, run_ltfs_rsync


def test_list_tape_devices_returns_list():
    """list_tape_devices returns a list (may be empty if no tape attached)."""
    devices = list_tape_devices()
    assert isinstance(devices, list)
    for d in devices:
        assert d.path.startswith("/dev/nst")
        assert os.path.basename(d.path).isdigit() or False  # nst0, nst1, etc.


def test_run_backup_to_file():
    """Backup runs to a file (skip_rewind) and produces valid tar output."""
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "src"
        src.mkdir()
        (src / "hello.txt").write_text("hello")
        (src / "sub").mkdir()
        (src / "sub" / "world.txt").write_text("world")
        out_file = Path(tmp) / "backup.tar"
        progress_msgs = []
        log_lines = []

        run_backup(
            str(out_file),
            [str(src)],
            skip_rewind=True,
            on_progress=progress_msgs.append,
            on_log=log_lines.append,
        )

        assert out_file.exists()
        assert out_file.stat().st_size > 0
        assert any("records written" in m for m in progress_msgs)
        # Tar content check: list archive
        import subprocess
        r = subprocess.run(
            ["tar", "-tvf", str(out_file)],
            capture_output=True,
            text=True,
        )
        assert r.returncode == 0
        assert "hello.txt" in r.stdout or "src/hello.txt" in r.stdout


def test_is_ltfs_available_returns_bool():
    """is_ltfs_available returns a boolean."""
    assert isinstance(is_ltfs_available(), bool)


def test_format_ltfs_raises_when_ltfs_not_available():
    """format_ltfs raises TapeBackupError with clear message when mkltfs is not in PATH."""
    with patch("tape_drive_controller.tape.ltfs.is_ltfs_available", return_value=False):
        with pytest.raises(TapeBackupError) as exc_info:
            format_ltfs("/dev/nst0")
        assert "LTFS not installed" in str(exc_info.value)


def test_format_ltfs_raises_when_sg_device_unavailable():
    """format_ltfs raises TapeBackupError when nst cannot be resolved to sg (e.g. not a real tape)."""
    with patch("tape_drive_controller.tape.ltfs.is_ltfs_available", return_value=True):
        with patch("tape_drive_controller.tape.ltfs.nst_to_sg", return_value=None):
            with pytest.raises(TapeBackupError) as exc_info:
                format_ltfs("/dev/nst0")
        assert "SCSI generic" in str(exc_info.value) or "sg" in str(exc_info.value).lower()


def test_nst_to_sg_returns_none_for_non_nst():
    """nst_to_sg returns None for paths that are not /dev/nst*."""
    assert nst_to_sg("/dev/sda1") is None
    assert nst_to_sg("/tmp/foo") is None


def test_erase_on_invalid_device_fails():
    """erase on a non-tape device (e.g. /dev/null) fails with TapeBackupError."""
    log_lines = []
    with pytest.raises(TapeBackupError):
        erase("/dev/null", on_log=log_lines.append, timeout=5)
    assert any("started" in line.lower() or "erase" in line.lower() for line in log_lines)


def test_run_ltfs_rsync_succeeds_when_ltfs_daemonizes():
    """When ltfs process exits (daemonized) but mount point is mounted, backup proceeds to rsync."""
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "src"
        src.mkdir()
        (src / "f").write_text("x")
        paths = [str(src)]

        ltfs_mock = MagicMock()
        ltfs_mock.poll.return_value = 0  # process exited (daemonized)
        ltfs_mock.stderr = io.StringIO("")

        rsync_mock = MagicMock()
        rsync_mock.stderr = io.StringIO("")  # no progress lines; read() returns ""
        rsync_mock.poll.return_value = 0  # exited, so read loop breaks after first read
        rsync_mock.returncode = 0
        rsync_mock.wait.return_value = None

        with patch("tape_drive_controller.tape.ltfs.nst_to_sg", return_value="/dev/sg0"):
            with patch(
                "tape_drive_controller.tape.ltfs.shutil.which",
                side_effect=lambda c: "/usr/bin/ltfs" if c in ("ltfs", "rsync") else None,
            ):
                with patch("tape_drive_controller.tape.ltfs.os.path.ismount", return_value=True):
                    with patch("tape_drive_controller.tape.ltfs.pty", None):  # use pipe so stderr.read() returns ""
                        with patch(
                            "tape_drive_controller.tape.ltfs.subprocess.Popen",
                            side_effect=[ltfs_mock, rsync_mock],
                        ):
                            with patch(
                                "tape_drive_controller.tape.ltfs.subprocess.run",
                                return_value=MagicMock(returncode=0),
                            ):
                                run_ltfs_rsync("/dev/nst0", paths)

        rsync_mock.wait.assert_called_once()
