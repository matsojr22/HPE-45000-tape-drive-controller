"""
Run OS-side diagnostics for a tape device (e.g. who has the sg device open, mounts, dmesg).
Used when format fails with device busy or on demand via "Tape diagnostics" button.
"""
import subprocess
from typing import Callable

from .capacity import nst_to_sg


def run_tape_diagnostics(
    device: str,
    on_log: Callable[[str], None],
    *,
    timeout: int = 5,
) -> None:
    """
    Run fuser, lsof, mount grep, and dmesg for the tape's sg device and log output via on_log.
    If device cannot be resolved to sg, log a message and return. No UI dependency.
    """
    sg = nst_to_sg(device)
    if not sg:
        on_log("Tape diagnostics: could not resolve tape device to sg (e.g. /dev/sg0).")
        return

    def run_cmd(args: list, description: str = "") -> None:
        header = description or " ".join(args)
        on_log("--- %s ---" % header)
        try:
            r = subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            out = (r.stdout or "").strip()
            err = (r.stderr or "").strip()
            if out:
                for line in out.splitlines():
                    on_log(line)
            if err:
                for line in err.splitlines():
                    on_log(line)
            if not out and not err:
                on_log("(no output)")
        except FileNotFoundError:
            on_log("%s: command not found" % header)
        except subprocess.TimeoutExpired:
            on_log("%s: timed out after %s s" % (header, timeout))
        except Exception as e:
            on_log("%s: %s" % (header, e))

    run_cmd(["fuser", "-v", sg], description="fuser -v %s" % sg)
    run_cmd(["lsof", sg], description="lsof %s" % sg)
    run_cmd(["sh", "-c", "mount | grep -E 'ltfs|fuse' || true"], description="mount | grep -E 'ltfs|fuse'")
    run_cmd(["sh", "-c", "dmesg | tail -40"], description="dmesg | tail -40")
    on_log("--- end tape diagnostics ---")
