# Tape Drive Controller

Desktop app for Linux (e.g. Linux Mint MATE) to back up directories to an LTO tape drive (e.g. HPE LTO-9 Ultrium). Choose a tape device, add directories (including Synology NAS mount points), and write a single tar archive to tape.

## Requirements (Linux)

- **Kernel**: Tape shows as SCSI device (e.g. `/dev/nst0`). No HPE-specific driver needed.
- **System packages** (Debian/Ubuntu/Mint):
  ```bash
  sudo apt update
  sudo apt install python3 python3-pip python3-venv \
    python3-gi python3-gi-cairo gir1.2-gtk-3.0 \
    mt-st tar lsscsi
  ```
- **Optional:** Install `sg3-utils` to auto-query tape capacity (Query button in the app):
  ```bash
  sudo apt install sg3-utils
  ```
- **Optional (LTFS):** To use **Format for LTFS** and (in future) backup by copying to a mounted tape, install LTFS. It is often not in distro repos; build from source (e.g. [LinearTapeFileSystem/ltfs](https://github.com/LinearTapeFileSystem/ltfs)) or use IBM/Quantum vendor packages. LTFS requires FUSE.
- **Device access**: Your user must be able to read/write the tape device (e.g. in group `tape` or `root`). If needed:
  ```bash
  sudo chmod 666 /dev/nst0
  # or add your user to the tape group and re-login
  ```

## Setup on the Linux machine

1. Copy the project folder to the Linux machine.
2. (Optional) Create a virtualenv and install Python deps:
   ```bash
   cd tape_drive_controller
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
   Or install the package in editable mode so you can run the app from any directory:
   ```bash
   pip install -e .
   ```
   On many systems the GTK bindings are provided by the system (`python3-gi`); if `pip install -r requirements.txt` fails for PyGObject, skip it and run with system Python.
3. Run the app (from the project root, or from any directory if you ran `pip install -e .`):
   ```bash
   python3 -m tape_drive_controller
   ```
   Or, to only list tape devices (no GUI):
   ```bash
   python3 -m tape_drive_controller --list-devices
   ```

## Using with Synology NAS

1. On the Linux machine, mount the NAS (once per boot or when needed). Example for CIFS:
   ```bash
   sudo mkdir -p /mnt/synology
   sudo mount -t cifs //SYNOLOGY_IP_OR_NAME/sharename /mnt/synology -o credentials=/path/to/smb-credentials,uid=$(id -u),gid=$(id -g)
   ```
   Example for NFS:
   ```bash
   sudo mount -t nfs SYNOLOGY_IP_OR_NAME:/volume1/sharename /mnt/synology
   ```
2. In the app, click **Add directory…** and choose e.g. `/mnt/synology/photo` or `/mnt/synology/video`. You can add multiple directories; they are written as one tar archive to the tape.

## Tape preparation

Before backup you can prepare the tape (optional):

- **Rewind** — Fast. Rewinds the tape to the beginning (BOT). Backup also rewinds automatically when you start it.
- **Erase** — Long and destructive. Runs `mt erase`; can take many hours and cannot be aborted. Use only when you want to fully erase the tape.
- **Format for LTFS** — Formats the tape for the Linear Tape File System so it can be mounted like a disk (e.g. for future “backup to LTFS” by copy). Requires [LTFS](https://github.com/LinearTapeFileSystem/ltfs) to be installed.

**Backup to LTFS (rsync):** Use **Backup to LTFS (rsync)** to copy the added directories onto an LTFS-formatted tape (the app mounts the tape, runs rsync, then unmounts). Requires `ltfs` and `rsync` in PATH. If the app detects an LTFS-formatted tape when you select a device, it may ask whether to switch to **LTFS mode**, which disables raw backup and tape preparation (Rewind, Erase, Format for LTFS, Browse tape) to avoid overwriting the tape. Use **Exit LTFS mode** to re-enable those actions.

**Browse tape and multi-archive:** Use **Browse tape** to list the contents of the tar archive on the tape (before or after a backup, or before restore). This applies to tar backups only; for LTFS-formatted tapes, mount the tape and use your file manager. You can **Append to tape** (check "Append to tape (do not rewind)" before Start backup) to write a new archive without rewinding—useful after an interrupted backup or to add another backup. The tape then contains multiple archives; use **Restore from archive #** (spin button, 1 = first) to choose which archive to restore.

**Important:** Use a **proper mount** (CIFS/NFS as above), not a GVFS/nautilus “network” bookmark. Paths under `/run/user/*/gvfs/smb-share:...` often cause `tar: Cannot stat: Invalid argument` for many files, so the backup can be incomplete. Mount the share to a normal path like `/mnt/synology` and back up that path instead.

## Troubleshooting

**Quick tip** — If the tape isn’t visible: load driver `sudo modprobe st`, rescan `echo "- - -" | sudo tee /sys/class/scsi_host/host*/scan`, then check `ls -l /dev/nst*` and `dmesg | tail -30`.

- **No tape device found**  
  The kernel must create `/dev/nst0` (and `/dev/st0`) for the tape. Try:
  1. Load the SCSI tape driver: `sudo modprobe st`
  2. If the drive was connected after boot, rescan the SCSI bus:  
     `echo "- - -" | sudo tee /sys/class/scsi_host/host*/scan`
  3. Check that the device exists: `ls -l /dev/nst*`
  4. Check kernel messages: `dmesg | tail -30` — you should see the tape detected (e.g. "Attached scsi tape" or the drive model).  
  Also check: cables, power, and that the SAS HBA driver is loaded (e.g. for LSI/Broadcom HBA, the host should appear in `ls /sys/class/scsi_host/`).
- **Permission denied** on the device  
  Ensure your user can open the device (e.g. `sudo chmod 666 /dev/nst0` or add user to `tape` group).
- **mt: command not found**  
  Install `mt-st`: `sudo apt install mt-st`.
- **Tar or backup fails**  
  Check the log in the app. Try **Tape status** to confirm the drive responds. Ensure selected paths exist and are readable.
- **Format for LTFS fails**  
  Ensure LTFS is installed (`mkltfs` in PATH). Build from [LinearTapeFileSystem/ltfs](https://github.com/LinearTapeFileSystem/ltfs) or use IBM/Quantum packages. The app uses the SCSI generic device (e.g. `/dev/sg0`) derived from the selected tape device and uses force format (`-f`) to work around "Length mismatch" / "Cannot read ANSI label" / -21716 on some drives (e.g. HPE Ultrium 9-SCSI). Ensure the FUSE module is loaded (`modprobe fuse`). If format still fails, try running `mkltfs -d /dev/sgN -f` manually (find the tape's sg device with `lsscsi` or from the app's device list).
- **Backup to LTFS fails / "LTFS mount failed (ltfs exited early)"**  
  The app treats daemonizing ltfs (parent exits after mount) as success when the mount point appears. If you still see this error, check the **app log** for the ltfs stderr output (the app includes it in the error message). Ensure the tape is **LTFS-formatted** (use **Format for LTFS** first if needed). Ensure **FUSE** is loaded (`sudo modprobe fuse`) and your user can use FUSE (e.g. in group `fuse` if required by your distro). Ensure the tape device (and thus `/dev/sgN`) is readable by your user (e.g. add user to `tape` group or `sudo chmod 666 /dev/sgN` for testing). If the message is still unclear, run ltfs manually to see the same error: `mkdir -p /mnt/ltfs-test && ltfs -o devname=/dev/sgN /mnt/ltfs-test` (get `/dev/sgN` from the app's device list or `lsscsi`).

## Project layout

- `tape_drive_controller/` — main package  
  - `__main__.py` — entry point (GUI or `--list-devices`)  
  - `tape/` — device listing and backup (mt + tar)  
  - `ui/` — GTK 3 main window  
- `tests/` — tests (run with `pytest`; backup test uses a temp file, no tape required)  
- `requirements.txt` — Python dependencies  

## License

Use and modify as you like.
