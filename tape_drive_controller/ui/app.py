"""GTK 3 application: main window with device selector, directory list, backup controls, and log."""
import subprocess
import sys
import threading
from typing import Optional

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

from ..tape.list_devices import list_tape_devices
from ..tape.backup import (
    run_backup,
    run_restore,
    rewind,
    erase,
    tape_status,
    list_tape_contents,
    TapeBackupError,
)
from ..tape.capacity import query_remaining_capacity_bytes
from ..tape.diagnostics import run_tape_diagnostics
from ..tape.ltfs import (
    format_ltfs,
    is_ltfs_available,
    is_ltfs_mount_available,
    tape_has_ltfs,
    run_ltfs_rsync,
    mount_ltfs_only,
    unmount_ltfs,
    unmount_leftover_ltfs_mounts,
)


def run_app() -> None:
    app = Gtk.Application(application_id="org.tape_drive_controller.app")
    app.connect("activate", _on_activate)
    app.run(sys.argv)


def _on_activate(app: Gtk.Application) -> None:
    win = MainWindow(application=app)
    win.show_all()


def _format_bytes(num_bytes: int) -> str:
    """Format byte count as human-readable (e.g. 1.23 GB)."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num_bytes < 1024:
            return f"{num_bytes:.2f} {unit}" if unit != "B" else f"{num_bytes} B"
        num_bytes /= 1024
    return f"{num_bytes:.2f} PB"


def _format_elapsed(sec: float) -> str:
    """Format seconds as e.g. 5m 23s or 1h 2m 3s."""
    if sec < 60:
        return f"{int(sec)}s"
    if sec < 3600:
        return f"{int(sec // 60)}m {int(sec % 60)}s"
    return f"{int(sec // 3600)}h {int((sec % 3600) // 60)}m {int(sec % 60)}s"


LOG_MAX_LINES = 500
DEFAULT_TAPE_CAPACITY_GB = 18000  # LTO-9 native; ensures backup fits on one standard tape


class MainWindow(Gtk.ApplicationWindow):
    def __init__(self, **kwargs) -> None:
        super().__init__(title="Tape Backup", default_width=700, default_height=500, **kwargs)

        self._cancel_requested = False
        self._cancel_restore_requested = False
        self._backup_thread = None
        self._restore_thread = None
        self._erase_thread = None
        self._format_ltfs_thread = None
        self._browse_thread = None
        self._cancel_browse_requested = False
        self._ltfs_rsync_thread = None
        self._cancel_ltfs_rsync_requested = False
        self._ltfs_rsync_process_holder: list = []  # [ltfs_proc, rsync_proc] when backup; [ltfs_proc] when standalone mount
        self._ltfs_mount_point_holder: list = [None]  # current LTFS mount path when mounted (backup or standalone)
        self._ltfs_standalone_mount = False  # True when mount was created by "Mount LTFS" (so Unmount is offered)
        self._ltfs_mode = False
        self._ltfs_mount_thread = None  # thread for standalone Mount LTFS
        self._ltfs_startup_check_pending = True
        self._restore_destination: Optional[str] = None
        self._progress_is_restore = False
        self._device_list = []

        box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=12)
        box.set_margin_start(12)
        box.set_margin_end(12)
        box.set_margin_top(12)
        box.set_margin_bottom(12)

        # Tape device
        dev_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        dev_row.pack_start(Gtk.Label(label="Tape device:", xalign=0), False, False, 0)
        self._device_store = Gtk.ListStore(str)
        self._device_combo = Gtk.ComboBox.new_with_model(self._device_store)
        renderer = Gtk.CellRendererText()
        self._device_combo.pack_start(renderer, True)
        self._device_combo.add_attribute(renderer, "text", 0)
        dev_row.pack_start(self._device_combo, True, True, 0)
        refresh_btn = Gtk.Button(label="Refresh")
        refresh_btn.connect("clicked", self._on_refresh_devices)
        dev_row.pack_start(refresh_btn, False, False, 0)
        self._tape_diagnostics_btn = Gtk.Button(label="Tape diagnostics")
        self._tape_diagnostics_btn.set_tooltip_text(
            "Run fuser, lsof, mount, and dmesg for the selected tape device and log output (e.g. when device is busy)."
        )
        self._tape_diagnostics_btn.connect("clicked", self._on_tape_diagnostics)
        dev_row.pack_start(self._tape_diagnostics_btn, False, False, 0)
        self._check_ltfs_btn = Gtk.Button(label="Check for LTFS")
        self._check_ltfs_btn.set_tooltip_text(
            "Check whether the selected tape has an LTFS partition and offer to switch to LTFS mode."
        )
        self._check_ltfs_btn.connect("clicked", self._on_check_ltfs)
        dev_row.pack_start(self._check_ltfs_btn, False, False, 0)
        box.pack_start(dev_row, False, False, 0)
        self._refresh_devices()

        # Tape preparation
        prep_label = Gtk.Label(label="Tape preparation:", xalign=0)
        box.pack_start(prep_label, False, False, 0)
        prep_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        self._rewind_btn = Gtk.Button(label="Rewind")
        self._rewind_btn.set_tooltip_text("Rewind tape to beginning (BOT).")
        self._rewind_btn.connect("clicked", self._on_rewind)
        self._erase_btn = Gtk.Button(label="Erase")
        self._erase_btn.set_tooltip_text(
            "Long erase (mt erase). Destructive and can take many hours; cannot be aborted."
        )
        self._erase_btn.connect("clicked", self._on_erase)
        self._format_ltfs_btn = Gtk.Button(label="Format for LTFS")
        _fmt_tt = "Format the tape for LTFS so it can be mounted as a filesystem."
        if not is_ltfs_available():
            _fmt_tt += " LTFS is not installed (build from source or use IBM/Quantum packages)."
        else:
            _fmt_tt += " Requires LTFS to be installed."
        self._format_ltfs_btn.set_tooltip_text(_fmt_tt)
        self._format_ltfs_btn.connect("clicked", self._on_format_ltfs)
        prep_row.pack_start(self._rewind_btn, False, False, 0)
        prep_row.pack_start(self._erase_btn, False, False, 0)
        prep_row.pack_start(self._format_ltfs_btn, False, False, 0)
        box.pack_start(prep_row, False, False, 0)

        # Directories to backup
        dir_label = Gtk.Label(label="Directories to backup:", xalign=0)
        box.pack_start(dir_label, False, False, 0)
        self._dir_store = Gtk.ListStore(str)
        dir_tree = Gtk.TreeView(model=self._dir_store)
        col = Gtk.TreeViewColumn("Path", Gtk.CellRendererText(), text=0)
        dir_tree.append_column(col)
        dir_tree.set_headers_visible(False)
        dir_tree.set_headers_clickable(False)
        dir_sw = Gtk.ScrolledWindow(min_content_height=80)
        dir_sw.add(dir_tree)
        box.pack_start(dir_sw, True, True, 0)
        dir_buttons = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        add_btn = Gtk.Button(label="Add directory…")
        add_btn.connect("clicked", self._on_add_directory)
        remove_btn = Gtk.Button(label="Remove")
        remove_btn.connect("clicked", self._on_remove_directory)
        dir_buttons.pack_start(add_btn, False, False, 0)
        dir_buttons.pack_start(remove_btn, False, False, 0)
        box.pack_start(dir_buttons, False, False, 0)

        # Optional tape capacity limit (safety check: abort if backup size > this)
        cap_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        cap_row.pack_start(Gtk.Label(label="Tape capacity (GB, optional):", xalign=0), False, False, 0)
        self._tape_capacity_spin = Gtk.SpinButton.new_with_range(0, 100000, 1)
        self._tape_capacity_spin.set_value(DEFAULT_TAPE_CAPACITY_GB)
        self._tape_capacity_spin.set_tooltip_text(
            "Backup is aborted if total size exceeds this. Default is LTO-9 (18 TB) so the backup fits on one standard tape. Set to 0 to disable the check."
        )
        cap_row.pack_start(self._tape_capacity_spin, False, False, 0)
        query_cap_btn = Gtk.Button(label="Query")
        query_cap_btn.set_tooltip_text(
            "Query maximum tape capacity from the selected device (requires sg3-utils)."
        )
        query_cap_btn.connect("clicked", self._on_query_capacity)
        cap_row.pack_start(query_cap_btn, False, False, 0)
        box.pack_start(cap_row, False, False, 0)

        # Progress
        self._progress_bar = Gtk.ProgressBar()
        box.pack_start(self._progress_bar, False, False, 0)
        self._progress_label = Gtk.Label(label="", xalign=0, wrap=True)
        self._progress_label.get_style_context().add_class("dim-label")
        box.pack_start(self._progress_label, False, False, 0)
        self._progress_activity_label = Gtk.Label(label="", xalign=0, wrap=True)
        self._progress_activity_label.get_style_context().add_class("dim-label")
        box.pack_start(self._progress_activity_label, False, False, 0)

        # Start / Cancel / Status
        btn_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        self._append_to_tape_cb = Gtk.CheckButton(label="Append to tape (do not rewind)")
        self._append_to_tape_cb.set_tooltip_text(
            "Writes a new archive after the current tape position. Use after an interrupted backup or to add another backup. "
            "Tape will contain multiple archives; use Browse to see contents and Restore from archive # to choose which to restore."
        )
        btn_row.pack_start(self._append_to_tape_cb, False, False, 0)
        self._start_btn = Gtk.Button(label="Start backup")
        self._start_btn.connect("clicked", self._on_start_backup)
        self._cancel_btn = Gtk.Button(label="Cancel")
        self._cancel_btn.connect("clicked", self._on_cancel_operation)
        self._cancel_btn.set_sensitive(False)
        self._status_btn = Gtk.Button(label="Tape status")
        self._status_btn.connect("clicked", self._on_tape_status)
        self._browse_btn = Gtk.Button(label="Browse tape")
        self._browse_btn.set_tooltip_text(
            "List contents of the tape (tar archive). For LTFS-formatted tapes, mount the tape and use the file manager."
        )
        self._browse_btn.connect("clicked", self._on_browse_tape)
        self._ltfs_rsync_btn = Gtk.Button(label="Backup to LTFS (rsync)")
        self._ltfs_rsync_btn.set_tooltip_text(
            "For LTFS-formatted tapes: mount tape and rsync added directories. Tape must be formatted for LTFS first. Requires ltfs and rsync."
        )
        self._ltfs_rsync_btn.connect("clicked", self._on_start_ltfs_rsync)
        self._exit_ltfs_mode_btn = Gtk.Button(label="Exit LTFS mode")
        self._exit_ltfs_mode_btn.set_tooltip_text(
            "Leave LTFS mode and re-enable tape preparation and raw backup."
        )
        self._exit_ltfs_mode_btn.connect("clicked", self._on_exit_ltfs_mode)
        self._mount_ltfs_btn = Gtk.Button(label="Mount LTFS")
        self._mount_ltfs_btn.set_tooltip_text(
            "Mount the selected tape as LTFS so you can browse or copy files. No backup is run."
        )
        self._mount_ltfs_btn.connect("clicked", self._on_mount_ltfs)
        self._unmount_ltfs_btn = Gtk.Button(label="Unmount LTFS")
        self._unmount_ltfs_btn.set_tooltip_text(
            "Unmount the current LTFS mount (only when you mounted via Mount LTFS, not during backup)."
        )
        self._unmount_ltfs_btn.connect("clicked", self._on_unmount_ltfs)
        self._browse_ltfs_btn = Gtk.Button(label="Browse mount")
        self._browse_ltfs_btn.set_tooltip_text(
            "Open the file manager at the current LTFS mount point."
        )
        self._browse_ltfs_btn.connect("clicked", self._on_browse_ltfs)
        btn_row.pack_start(self._start_btn, False, False, 0)
        btn_row.pack_start(self._cancel_btn, False, False, 0)
        btn_row.pack_start(self._status_btn, False, False, 0)
        btn_row.pack_start(self._browse_btn, False, False, 0)
        btn_row.pack_start(self._ltfs_rsync_btn, False, False, 0)
        box.pack_start(btn_row, False, False, 0)
        ltfs_mode_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        ltfs_mode_row.pack_start(Gtk.Label(label="LTFS mode:", xalign=0), False, False, 0)
        ltfs_mode_row.pack_start(self._mount_ltfs_btn, False, False, 0)
        ltfs_mode_row.pack_start(self._unmount_ltfs_btn, False, False, 0)
        ltfs_mode_row.pack_start(self._browse_ltfs_btn, False, False, 0)
        ltfs_mode_row.pack_start(self._exit_ltfs_mode_btn, False, False, 0)
        self._ltfs_mode_row = ltfs_mode_row
        box.pack_start(ltfs_mode_row, False, False, 0)
        ltfs_mode_row.hide()

        # Restore
        restore_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        restore_row.pack_start(Gtk.Label(label="Restore to directory:", xalign=0), False, False, 0)
        self._restore_path_label = Gtk.Label(label="(none)", xalign=0, ellipsize=3)
        restore_row.pack_start(self._restore_path_label, True, True, 0)
        restore_browse_btn = Gtk.Button(label="Browse…")
        restore_browse_btn.connect("clicked", self._on_restore_browse)
        restore_row.pack_start(restore_browse_btn, False, False, 0)
        restore_row.pack_start(Gtk.Label(label="Archive #:", xalign=0), False, False, 0)
        self._restore_archive_spin = Gtk.SpinButton.new_with_range(1, 999, 1)
        self._restore_archive_spin.set_value(1)
        self._restore_archive_spin.set_tooltip_text(
            "Which archive on the tape to restore (1 = first). Use Browse tape to see contents."
        )
        restore_row.pack_start(self._restore_archive_spin, False, False, 0)
        self._start_restore_btn = Gtk.Button(label="Start restore")
        self._start_restore_btn.connect("clicked", self._on_start_restore)
        restore_row.pack_start(self._start_restore_btn, False, False, 0)
        box.pack_start(restore_row, False, False, 0)

        # Log
        log_sw = Gtk.ScrolledWindow()
        log_sw.set_min_content_height(120)
        self._log_buffer = Gtk.TextBuffer()
        self._log_view = Gtk.TextView(buffer=self._log_buffer, editable=False, wrap_mode=Gtk.WrapMode.CHAR)
        self._log_view.set_left_margin(4)
        self._log_view.set_right_margin(4)
        log_sw.add(self._log_view)
        box.pack_start(log_sw, True, True, 0)

        self._dir_tree = dir_tree
        self._update_start_sensitivity()
        self._device_combo.connect("changed", self._on_device_changed)
        self._dir_store.connect("row-deleted", lambda *a: self._update_start_sensitivity())
        self._dir_store.connect("row-inserted", lambda *a: self._update_start_sensitivity())
        self.connect("destroy", self._on_destroy)
        GLib.idle_add(self._maybe_check_ltfs_after_show)

        self._main_box = box
        overlay = Gtk.Overlay()
        overlay.add(box)
        loading_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=12)
        loading_spinner = Gtk.Spinner()
        loading_spinner.start()
        loading_box.pack_start(loading_spinner, False, False, 0)
        loading_label = Gtk.Label(label="Checking for LTFS partitions…")
        loading_box.pack_start(loading_label, False, False, 0)
        loading_box.set_halign(Gtk.Align.CENTER)
        loading_box.set_valign(Gtk.Align.CENTER)
        loading_box.set_hexpand(True)
        loading_box.set_vexpand(True)
        overlay.add_overlay(loading_box)
        self._loading_overlay_box = loading_box
        self._loading_spinner = loading_spinner
        self._main_box.set_sensitive(False)
        self.add(overlay)

    def _refresh_devices(self) -> None:
        self._device_list = list_tape_devices()
        self._device_store.clear()
        if self._device_list:
            for d in self._device_list:
                self._device_store.append([d.display_name()])
            self._device_combo.set_active(0)
        else:
            self._device_store.append(["No tape device found"])
            self._device_combo.set_active(0)
            self._log(
                "Tip: Load SCSI tape driver: sudo modprobe st. "
                "Then rescan: echo \"- - -\" | sudo tee /sys/class/scsi_host/host*/scan. "
                "Check: ls -l /dev/nst* and dmesg | tail -30"
            )

    def _get_selected_device_path(self):
        if not self._device_list:
            return None
        idx = self._device_combo.get_active()
        if idx is None or idx < 0 or idx >= len(self._device_list):
            return None
        return self._device_list[idx].path

    def _on_refresh_devices(self, _btn):
        self._refresh_devices()
        self._update_start_sensitivity()

    def _on_tape_diagnostics(self, _btn):
        device = self._get_selected_device_path()
        if not device:
            self._log("Select a tape device first.")
            return
        self._log("=== Tape diagnostics (selected device: %s) ===" % device)
        def run_diag():
            run_tape_diagnostics(device, lambda line: GLib.idle_add(self._log, line))
        threading.Thread(target=run_diag, daemon=True).start()

    def _on_check_ltfs(self, _btn):
        device = self._get_selected_device_path()
        if not device:
            self._log("Select a tape device first.")
            return
        self._log("Checking for LTFS partition on %s…" % device)

        def completion(has_ltfs):
            if not has_ltfs:
                self._log("No LTFS partition found on %s." % device)
            self._on_ltfs_check_done(has_ltfs)

        def run():
            unmount_leftover_ltfs_mounts(on_log=lambda line: GLib.idle_add(self._log, line))
            has_ltfs = tape_has_ltfs(device)
            GLib.idle_add(completion, has_ltfs)

        threading.Thread(target=run, daemon=True).start()

    def _on_device_changed(self, combo):
        self._update_start_sensitivity()
        self._run_ltfs_check_if_needed()

    def _apply_ready_after_ltfs_check(self) -> None:
        """Hide loading overlay, re-enable main content, and update button sensitivity."""
        self._loading_overlay_box.hide()
        self._loading_spinner.stop()
        self._main_box.set_sensitive(True)
        self._update_start_sensitivity()

    def _on_destroy(self, _widget) -> None:
        """On window close: clean unmount LTFS if active, terminate child processes, then quit."""
        self._cancel_ltfs_rsync_requested = True
        mount_point = self._ltfs_mount_point_holder[0] if self._ltfs_mount_point_holder else None
        if mount_point:
            try:
                subprocess.run(
                    ["fusermount", "-u", mount_point],
                    capture_output=True,
                    timeout=10,
                )
            except (FileNotFoundError, subprocess.TimeoutExpired):
                try:
                    subprocess.run(
                        ["umount", mount_point],
                        capture_output=True,
                        timeout=10,
                    )
                except subprocess.TimeoutExpired:
                    pass
        # Terminate in reverse order: rsync first, then ltfs (stops writes before killing mount)
        for proc in reversed(self._ltfs_rsync_process_holder):
            try:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)
            except (OSError, ValueError):
                pass
        if self._ltfs_rsync_thread is not None and self._ltfs_rsync_thread.is_alive():
            self._ltfs_rsync_thread.join(timeout=3.0)
        app = self.get_application()
        if app is not None:
            app.quit()

    def _maybe_check_ltfs_after_show(self):
        """Called once after window is shown to detect LTFS tape on initial device."""
        device = self._get_selected_device_path()
        any_busy = (
            self._backup_thread is not None
            or self._restore_thread is not None
            or self._erase_thread is not None
            or self._format_ltfs_thread is not None
            or self._browse_thread is not None
            or self._ltfs_rsync_thread is not None
            or self._ltfs_mount_thread is not None
        )
        if not device or self._ltfs_mode or any_busy:
            self._ltfs_startup_check_pending = False
            self._apply_ready_after_ltfs_check()
            return False
        # Defer so the loading overlay is visible for at least one frame before the check runs
        GLib.idle_add(self._run_ltfs_check_if_needed)
        return False

    def _run_ltfs_check_if_needed(self):
        """If a device is selected and not in LTFS mode and no operation running, check tape for LTFS in background."""
        device = self._get_selected_device_path()
        if not device or self._ltfs_mode:
            return
        if (
            self._backup_thread is not None
            or self._restore_thread is not None
            or self._erase_thread is not None
            or self._format_ltfs_thread is not None
            or self._browse_thread is not None
            or self._ltfs_rsync_thread is not None
            or self._ltfs_mount_thread is not None
        ):
            return

        def check():
            unmount_leftover_ltfs_mounts(on_log=lambda line: GLib.idle_add(self._log, line))
            has_ltfs = tape_has_ltfs(device)
            GLib.idle_add(self._on_ltfs_check_done, has_ltfs)

        threading.Thread(target=check, daemon=True).start()

    def _on_ltfs_check_done(self, has_ltfs: bool):
        self._ltfs_startup_check_pending = False
        self._apply_ready_after_ltfs_check()
        if not has_ltfs or self._ltfs_mode:
            return
        dialog = Gtk.MessageDialog(
            transient_for=self,
            modal=True,
            message_type=Gtk.MessageType.QUESTION,
            buttons=Gtk.ButtonsType.YES_NO,
            text="LTFS tape detected",
        )
        dialog.format_secondary_text(
            "Tape appears to be LTFS-formatted. Use LTFS mode? "
            "(Raw backup will be disabled to avoid overwriting the tape.)"
        )
        response = dialog.run()
        dialog.destroy()
        if response == Gtk.ResponseType.YES:
            self._ltfs_mode = True
            self._update_start_sensitivity()
            self._log("LTFS mode enabled (raw backup and tape preparation disabled).")

    def _on_query_capacity(self, _btn):
        device = self._get_selected_device_path()
        if not device:
            self._log("Select a tape device first.")
            return

        def run():
            result = query_remaining_capacity_bytes(device)
            def apply():
                if result is None:
                    self._tape_capacity_spin.set_value(0)
                    self._log(
                        "Could not query capacity. Install sg3-utils (sg_read_attr) and ensure the tape is loaded."
                    )
                else:
                    gb = result / (1024 ** 3)
                    self._tape_capacity_spin.set_value(min(100000, max(0, gb)))
                    self._log("Tape capacity: %.2f GB" % gb)
                return False
            GLib.idle_add(apply)

        threading.Thread(target=run, daemon=True).start()

    def _update_start_sensitivity(self):
        # Clear thread refs if the thread has died (e.g. crash without calling _backup_finished)
        if self._backup_thread is not None and not self._backup_thread.is_alive():
            self._backup_thread = None
        if self._restore_thread is not None and not self._restore_thread.is_alive():
            self._restore_thread = None
        if self._erase_thread is not None and not self._erase_thread.is_alive():
            self._erase_thread = None
        if self._format_ltfs_thread is not None and not self._format_ltfs_thread.is_alive():
            self._format_ltfs_thread = None
        if self._browse_thread is not None and not self._browse_thread.is_alive():
            self._browse_thread = None
        if self._ltfs_rsync_thread is not None and not self._ltfs_rsync_thread.is_alive():
            self._ltfs_rsync_thread = None
        device = self._get_selected_device_path()
        backup_busy = self._backup_thread is not None
        restore_busy = self._restore_thread is not None
        erase_busy = self._erase_thread is not None
        format_busy = self._format_ltfs_thread is not None
        browse_busy = self._browse_thread is not None
        ltfs_rsync_busy = self._ltfs_rsync_thread is not None
        ltfs_mount_busy = self._ltfs_mount_thread is not None
        any_busy = backup_busy or restore_busy or erase_busy or format_busy or browse_busy or ltfs_rsync_busy or ltfs_mount_busy
        mount_point = self._ltfs_mount_point_holder[0] if self._ltfs_mount_point_holder else None
        if self._ltfs_mode:
            self._ltfs_mode_row.show()
            self._start_btn.set_sensitive(False)
            self._append_to_tape_cb.set_sensitive(False)
            self._rewind_btn.set_sensitive(False)
            self._erase_btn.set_sensitive(False)
            self._format_ltfs_btn.set_sensitive(False)
            self._browse_btn.set_sensitive(False)
            self._ltfs_rsync_btn.set_sensitive(
                not any_busy and bool(device and len(self._dir_store) > 0) and is_ltfs_mount_available()
            )
            self._mount_ltfs_btn.set_sensitive(
                not any_busy and bool(device) and not mount_point and is_ltfs_mount_available()
            )
            self._unmount_ltfs_btn.set_sensitive(
                not any_busy and self._ltfs_standalone_mount and bool(mount_point)
            )
            self._browse_ltfs_btn.set_sensitive(bool(mount_point))
            self._status_btn.set_sensitive(not any_busy)
            self._tape_diagnostics_btn.set_sensitive(not any_busy and bool(device))
            self._check_ltfs_btn.set_sensitive(not any_busy and bool(device))
            self._exit_ltfs_mode_btn.set_sensitive(not any_busy)
            self._start_restore_btn.set_sensitive(False)
            self._cancel_btn.set_sensitive(any_busy)
        else:
            self._ltfs_mode_row.hide()
            self._start_btn.set_sensitive(
                not any_busy and bool(device and len(self._dir_store) > 0)
            )
            self._append_to_tape_cb.set_sensitive(True)
            self._start_restore_btn.set_sensitive(
                not any_busy and bool(device and self._restore_destination)
            )
            self._cancel_btn.set_sensitive(any_busy)
            self._status_btn.set_sensitive(not any_busy)
            self._browse_btn.set_sensitive(not any_busy and bool(device))
            self._rewind_btn.set_sensitive(not any_busy and bool(device))
            self._erase_btn.set_sensitive(not any_busy and bool(device))
            self._format_ltfs_btn.set_sensitive(not any_busy and bool(device))
            self._tape_diagnostics_btn.set_sensitive(not any_busy and bool(device))
            self._check_ltfs_btn.set_sensitive(not any_busy and bool(device))
            self._ltfs_rsync_btn.set_sensitive(
                not any_busy and bool(device and len(self._dir_store) > 0) and is_ltfs_mount_available()
            )

    def _on_add_directory(self, _btn):
        dialog = Gtk.FileChooserDialog(
            title="Select directory to backup",
            parent=self,
            action=Gtk.FileChooserAction.SELECT_FOLDER,
        )
        dialog.add_buttons("_Cancel", Gtk.ResponseType.CANCEL, "_Open", Gtk.ResponseType.OK)
        if dialog.run() == Gtk.ResponseType.OK:
            path = dialog.get_filename()
            if path:
                self._dir_store.append([path])
                if "/gvfs/" in path:
                    self._log(
                        "Warning: GVFS/network paths (e.g. smb-share:) often cause 'Cannot stat: Invalid argument' "
                        "and incomplete backups. Prefer a CIFS mount (e.g. /mnt/something) instead."
                    )
        dialog.destroy()

    def _on_remove_directory(self, _btn):
        sel = self._dir_tree.get_selection()
        _, tree_iter = sel.get_selected()
        if tree_iter:
            self._dir_store.remove(tree_iter)

    def _log(self, text):
        def do():
            buf = self._log_buffer
            line_count = buf.get_line_count()
            if line_count >= LOG_MAX_LINES:
                lines_to_remove = line_count - LOG_MAX_LINES + 1
                start = buf.get_start_iter()
                end = buf.get_iter_at_line(lines_to_remove)
                buf.delete(start, end)
            end_iter = buf.get_end_iter()
            buf.insert(end_iter, text + "\n")
            self._log_view.scroll_to_iter(buf.get_end_iter(), 0.0, True, 0.0, 1.0)
            return False
        GLib.idle_add(do)

    def _set_progress(self, text):
        def do():
            self._progress_activity_label.set_label(text)
            return False
        GLib.idle_add(do)

    def _on_progress_update(
        self, bytes_written: int, total_bytes: Optional[int], elapsed_sec: float
    ) -> None:
        """Update progress bar and label from backup thread (call via GLib.idle_add)."""
        def do():
            if total_bytes and total_bytes > 0:
                self._progress_bar.set_fraction(bytes_written / total_bytes)
                pct = 100.0 * bytes_written / total_bytes
                msg = f"{_format_bytes(bytes_written)} / {_format_bytes(total_bytes)} ({pct:.1f}%)"
                if bytes_written > 0 and total_bytes > bytes_written:
                    eta_sec = elapsed_sec * (total_bytes - bytes_written) / bytes_written
                    msg += f"  Elapsed: {_format_elapsed(elapsed_sec)}  ETA: {_format_elapsed(eta_sec)}"
                else:
                    msg += f"  Elapsed: {_format_elapsed(elapsed_sec)}"
                self._progress_label.set_label(msg)
            else:
                self._progress_bar.set_pulse_step(0.1)
                self._progress_bar.pulse()
                verb = "read" if self._progress_is_restore else "written"
                msg = f"{_format_bytes(bytes_written)} {verb}  Elapsed: {_format_elapsed(elapsed_sec)}"
                self._progress_label.set_label(msg)
            return False
        GLib.idle_add(do)

    def _on_tape_status(self, _btn):
        device = self._get_selected_device_path()
        if not device:
            self._log("No tape device selected.")
            return
        self._log("--- Tape status ---")
        try:
            out = tape_status(device)
            self._log(out)
        except Exception as e:
            self._log("Error: %s" % e)
        self._log("-------------------")

    def _on_browse_tape(self, _btn):
        device = self._get_selected_device_path()
        if not device:
            self._log("Select a tape device first.")
            return
        self._cancel_browse_requested = False
        # Dialog: status label, Cancel, TreeView (path, size, type)
        dialog = Gtk.Dialog(
            title="Tape contents",
            transient_for=self,
            modal=False,
        )
        dialog.add_buttons("_Close", Gtk.ResponseType.CLOSE)
        dialog.set_default_size(600, 400)
        content = dialog.get_content_area()
        content.set_spacing(8)
        status_label = Gtk.Label(
            label="Reading tape… (can take several minutes; Cancel may take a few seconds)",
            xalign=0,
            wrap=True,
        )
        content.pack_start(status_label, False, False, 0)
        cancel_btn = Gtk.Button(label="Cancel")
        cancel_btn.connect("clicked", lambda b: setattr(self, "_cancel_browse_requested", True))
        cancel_btn_box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL)
        cancel_btn_box.pack_start(cancel_btn, False, False, 0)
        content.pack_start(cancel_btn_box, False, False, 0)
        store = Gtk.ListStore(str, str, str)
        tree = Gtk.TreeView(model=store)
        tree.append_column(Gtk.TreeViewColumn("Path", Gtk.CellRendererText(), text=0))
        tree.append_column(Gtk.TreeViewColumn("Size", Gtk.CellRendererText(), text=1))
        tree.append_column(Gtk.TreeViewColumn("Type", Gtk.CellRendererText(), text=2))
        sw = Gtk.ScrolledWindow(min_content_height=200)
        sw.add(tree)
        content.pack_start(sw, True, True, 0)
        self._browse_dialog = dialog
        self._browse_dialog_status_label = status_label
        self._browse_dialog_store = store
        self._browse_dialog_cancel_btn = cancel_btn
        dialog.connect("destroy", self._on_browse_dialog_destroy)
        dialog.connect("response", lambda d, _r: d.destroy())
        dialog.show_all()

        def run():
            try:
                entries = list_tape_contents(
                    device,
                    on_progress=lambda m: GLib.idle_add(self._set_browse_status, m),
                    cancel_check=lambda: self._cancel_browse_requested,
                )
                GLib.idle_add(self._browse_finished, entries, None)
            except Exception as e:
                GLib.idle_add(self._browse_finished, None, e)

        self._browse_thread = threading.Thread(target=run, daemon=True)
        self._browse_thread.start()
        self._update_start_sensitivity()

    def _set_browse_status(self, msg: str) -> None:
        if getattr(self, "_browse_dialog_status_label", None):
            self._browse_dialog_status_label.set_label(msg)

    def _browse_finished(self, entries, error) -> None:
        self._browse_thread = None
        status_label = getattr(self, "_browse_dialog_status_label", None)
        store = getattr(self, "_browse_dialog_store", None)
        cancel_btn = getattr(self, "_browse_dialog_cancel_btn", None)
        if cancel_btn:
            cancel_btn.set_sensitive(False)
        if error:
            if status_label:
                status_label.set_label("Error: %s" % error)
            self._log("Browse tape failed: %s" % error)
        elif entries is not None and store is not None:
            for e in entries:
                size_str = _format_bytes(e.size) if not e.is_dir else "—"
                type_str = "Directory" if e.is_dir else "File"
                store.append([e.path, size_str, type_str])
            if status_label:
                status_label.set_label("Done. %d entries." % len(entries))
        self._update_start_sensitivity()

    def _on_browse_dialog_destroy(self, dialog) -> None:
        self._cancel_browse_requested = True
        for attr in ("_browse_dialog", "_browse_dialog_status_label", "_browse_dialog_store", "_browse_dialog_cancel_btn"):
            if hasattr(self, attr):
                setattr(self, attr, None)
        self._update_start_sensitivity()

    def _on_rewind(self, _btn):
        device = self._get_selected_device_path()
        if not device:
            self._log("Select a tape device first.")
            return
        self._log("Rewinding tape…")
        try:
            rewind(device)
            self._log("Rewind completed.")
        except Exception as e:
            self._log("Rewind failed: %s" % e)

    def _on_erase(self, _btn):
        device = self._get_selected_device_path()
        if not device:
            self._log("Select a tape device first.")
            return
        dialog = Gtk.MessageDialog(
            transient_for=self,
            flags=0,
            message_type=Gtk.MessageType.WARNING,
            buttons=Gtk.ButtonsType.OK_CANCEL,
            text="Erase tape?",
        )
        dialog.format_secondary_text(
            "Long erase is destructive and can take many hours. It cannot be aborted. "
            "Only use with a tape you intend to fully erase."
        )
        if dialog.run() != Gtk.ResponseType.OK:
            dialog.destroy()
            return
        dialog.destroy()

        self._log("=== Erase started ===")
        self._erase_thread = threading.Thread(
            target=self._run_erase,
            args=(device,),
            daemon=True,
        )
        self._erase_thread.start()
        self._update_start_sensitivity()

    def _run_erase(self, device: str) -> None:
        try:
            erase(
                device,
                on_log=lambda line: GLib.idle_add(self._log, line),
            )
            GLib.idle_add(self._erase_finished, None)
        except Exception as e:
            GLib.idle_add(self._erase_finished, e)

    def _erase_finished(self, error) -> None:
        self._erase_thread = None
        self._update_start_sensitivity()
        if error:
            self._log("Erase failed: %s" % error)
        self._log("=== Erase ended ===\n")

    def _on_format_ltfs(self, _btn):
        device = self._get_selected_device_path()
        if not device:
            self._log("Select a tape device first.")
            return
        if not is_ltfs_available():
            self._log(
                "LTFS is not installed. Install the LTFS package or build from source "
                "(e.g. https://github.com/LinearTapeFileSystem/ltfs)."
            )
            return
        self._log("=== Format for LTFS started ===")
        self._format_ltfs_thread = threading.Thread(
            target=self._run_format_ltfs,
            args=(device,),
            daemon=True,
        )
        self._format_ltfs_thread.start()
        self._update_start_sensitivity()

    def _run_format_ltfs(self, device: str) -> None:
        try:
            unmount_leftover_ltfs_mounts(on_log=lambda line: GLib.idle_add(self._log, line))
            format_ltfs(
                device,
                on_log=lambda line: GLib.idle_add(self._log, line),
                force=True,
            )
            GLib.idle_add(self._log, "Rewinding tape…")
            rewind(device)
            GLib.idle_add(self._format_ltfs_finished, None)
        except Exception as e:
            GLib.idle_add(self._format_ltfs_finished, e)

    def _format_ltfs_finished(self, error) -> None:
        self._format_ltfs_thread = None
        self._update_start_sensitivity()
        if error:
            self._log("Format for LTFS failed: %s" % error)
            device = self._get_selected_device_path()
            if device:
                self._log("Running tape diagnostics…")
                def run_diag():
                    run_tape_diagnostics(
                        device,
                        lambda line: GLib.idle_add(self._log, line),
                    )
                threading.Thread(target=run_diag, daemon=True).start()
        else:
            self._log("Format for LTFS completed.")
        self._log("=== Format for LTFS ended ===\n")

    def _on_start_ltfs_rsync(self, _btn):
        device = self._get_selected_device_path()
        paths = [row[0] for row in self._dir_store]
        if not device:
            self._log("Select a tape device first.")
            return
        if not paths:
            self._log("Add at least one directory to backup.")
            return
        if not is_ltfs_mount_available():
            self._log("LTFS (ltfs) and rsync are required for Backup to LTFS. Install LTFS and rsync.")
            return
        # Clear any standalone mount so backup owns the tape
        if self._ltfs_standalone_mount and self._ltfs_mount_point_holder and self._ltfs_mount_point_holder[0]:
            mp = self._ltfs_mount_point_holder[0]
            proc = self._ltfs_rsync_process_holder[0] if self._ltfs_rsync_process_holder else None
            unmount_ltfs(mp, proc, on_log=lambda line: GLib.idle_add(self._log, line))
            self._ltfs_mount_point_holder[0] = None
            self._ltfs_standalone_mount = False
            self._ltfs_rsync_process_holder.clear()
        self._cancel_ltfs_rsync_requested = False
        self._progress_is_restore = False
        self._update_start_sensitivity()
        self._progress_bar.set_fraction(0)
        self._progress_activity_label.set_label("Mounting LTFS…")
        self._log("=== Backup to LTFS (rsync) started ===")
        self._log("Device: %s" % device)
        for p in paths:
            self._log("  %s" % p)

        def run():
            self._ltfs_rsync_process_holder.clear()
            try:
                run_ltfs_rsync(
                    device,
                    paths,
                    on_progress=lambda m: GLib.idle_add(self._set_progress, m),
                    on_progress_update=lambda b, t, e: GLib.idle_add(
                        self._on_progress_update, b, t, e
                    ),
                    on_log=lambda line: GLib.idle_add(self._log, line),
                    cancel_check=lambda: self._cancel_ltfs_rsync_requested,
                    process_holder=self._ltfs_rsync_process_holder,
                    mount_point_holder=self._ltfs_mount_point_holder,
                )
                GLib.idle_add(self._ltfs_rsync_finished, None)
            except Exception as e:
                GLib.idle_add(self._ltfs_rsync_finished, e)

        self._ltfs_rsync_thread = threading.Thread(target=run, daemon=True)
        self._ltfs_rsync_thread.start()
        self._update_start_sensitivity()

    def _ltfs_rsync_finished(self, error) -> None:
        self._ltfs_rsync_thread = None
        self._update_start_sensitivity()
        if error:
            self._progress_bar.set_fraction(0)
            self._set_progress("Error")
            self._log("Backup to LTFS failed: %s" % error)
        else:
            self._progress_bar.set_fraction(1.0)
            self._set_progress("Backup to LTFS completed.")
        self._log("=== Backup to LTFS ended ===\n")

    def _on_exit_ltfs_mode(self, _btn):
        self._ltfs_mode = False
        self._update_start_sensitivity()

    def _on_browse_ltfs(self, _btn):
        mount_point = self._ltfs_mount_point_holder[0] if self._ltfs_mount_point_holder else None
        if not mount_point:
            self._log("No LTFS mount. Mount the tape first (Mount LTFS or run Backup to LTFS).")
            return
        try:
            subprocess.Popen(
                ["xdg-open", mount_point],
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except FileNotFoundError:
            try:
                subprocess.Popen(
                    ["gio", "open", mount_point],
                    start_new_session=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            except FileNotFoundError:
                self._log("Could not open file manager (xdg-open and gio not found).")

    def _on_mount_ltfs(self, _btn):
        device = self._get_selected_device_path()
        if not device:
            self._log("Select a tape device first.")
            return
        if not is_ltfs_mount_available():
            self._log("LTFS (ltfs) is required. Install LTFS to mount the tape.")
            return
        self._log("=== Mount LTFS started ===")
        self._log("Device: %s" % device)

        def run():
            try:
                mount_ltfs_only(
                    device,
                    on_log=lambda line: GLib.idle_add(self._log, line),
                    mount_point_holder=self._ltfs_mount_point_holder,
                    process_holder=self._ltfs_rsync_process_holder,
                )
                GLib.idle_add(self._ltfs_mount_finished, None)
            except Exception as e:
                GLib.idle_add(self._ltfs_mount_finished, e)

        self._ltfs_mount_thread = threading.Thread(target=run, daemon=True)
        self._ltfs_mount_thread.start()
        self._update_start_sensitivity()

    def _ltfs_mount_finished(self, error) -> None:
        self._ltfs_mount_thread = None
        self._update_start_sensitivity()
        if error:
            self._log("Mount LTFS failed: %s" % error)
        else:
            self._ltfs_standalone_mount = True
            self._log("LTFS mounted at %s" % (self._ltfs_mount_point_holder[0] if self._ltfs_mount_point_holder else ""))
            self._update_start_sensitivity()
        self._log("=== Mount LTFS ended ===\n")

    def _on_unmount_ltfs(self, _btn):
        mount_point = self._ltfs_mount_point_holder[0] if self._ltfs_mount_point_holder else None
        if not mount_point:
            return
        ltfs_proc = self._ltfs_rsync_process_holder[0] if self._ltfs_rsync_process_holder else None
        unmount_ltfs(
            mount_point,
            ltfs_proc,
            on_log=lambda line: GLib.idle_add(self._log, line),
        )
        self._ltfs_mount_point_holder[0] = None
        self._ltfs_standalone_mount = False
        if self._ltfs_rsync_process_holder and self._ltfs_rsync_process_holder[0] is ltfs_proc:
            self._ltfs_rsync_process_holder.pop(0)
        self._log("LTFS unmounted.")
        self._update_start_sensitivity()

    def _on_restore_browse(self, _btn):
        dialog = Gtk.FileChooserDialog(
            title="Select directory to restore into",
            parent=self,
            action=Gtk.FileChooserAction.SELECT_FOLDER,
        )
        dialog.add_buttons("_Cancel", Gtk.ResponseType.CANCEL, "_Open", Gtk.ResponseType.OK)
        if dialog.run() == Gtk.ResponseType.OK:
            path = dialog.get_filename()
            if path:
                self._restore_destination = path
                self._restore_path_label.set_label(path)
                self._update_start_sensitivity()
        dialog.destroy()

    def _on_start_backup(self, _btn):
        device = self._get_selected_device_path()
        paths = [row[0] for row in self._dir_store]
        if not device:
            self._log("Select a tape device (and click Refresh if needed).")
            return
        if not paths:
            self._log("Add at least one directory to backup.")
            return
        self._cancel_requested = False
        self._progress_is_restore = False
        self._update_start_sensitivity()
        self._progress_bar.set_fraction(0)
        self._progress_activity_label.set_label("Calculating size…")
        self._log("=== Backup started ===")
        self._log("Device: %s" % device)
        for p in paths:
            self._log("  %s" % p)

        cap_gb = self._tape_capacity_spin.get_value_as_int()
        max_tape_bytes = int(cap_gb * (1024 ** 3)) if cap_gb > 0 else None
        skip_rewind = self._append_to_tape_cb.get_active()

        def start_backup_thread():
            try:
                run_backup(
                    device,
                    paths,
                    skip_rewind=skip_rewind,
                    max_tape_bytes=max_tape_bytes,
                    on_progress=lambda m: GLib.idle_add(self._set_progress, m),
                    on_progress_update=lambda b, t, e: GLib.idle_add(
                        self._on_progress_update, b, t, e
                    ),
                    on_log=lambda line: GLib.idle_add(self._log, line),
                    cancel_check=lambda: self._cancel_requested,
                )
                GLib.idle_add(self._backup_finished, None)
            except Exception as e:
                GLib.idle_add(self._backup_finished, e)

        def continuation(has_ltfs):
            if has_ltfs:
                dialog = Gtk.MessageDialog(
                    transient_for=self,
                    modal=True,
                    message_type=Gtk.MessageType.WARNING,
                    buttons=Gtk.ButtonsType.NONE,
                    text="Tape has LTFS partition",
                )
                dialog.format_secondary_text(
                    "This tape appears to be LTFS-formatted. A tar backup will overwrite and "
                    "destroy the LTFS data. Use LTFS mode and 'Backup to LTFS (rsync)' to write "
                    "to this tape. Continue with tar backup anyway?"
                )
                dialog.add_button("Cancel", Gtk.ResponseType.CANCEL)
                dialog.add_button("Continue anyway", Gtk.ResponseType.YES)
                response = dialog.run()
                dialog.destroy()
                if response != Gtk.ResponseType.YES:
                    self._log("Backup cancelled to protect LTFS tape.")
                    self._update_start_sensitivity()
                    return
            self._backup_thread = threading.Thread(target=start_backup_thread, daemon=True)
            self._backup_thread.start()
            self._update_start_sensitivity()

        def pre_check():
            self._log("Checking for LTFS partition before tar backup…")
            unmount_leftover_ltfs_mounts(on_log=lambda line: GLib.idle_add(self._log, line))
            has_ltfs = tape_has_ltfs(device)
            GLib.idle_add(continuation, has_ltfs)

        threading.Thread(target=pre_check, daemon=True).start()
        self._update_start_sensitivity()

    def _backup_finished(self, error):
        self._backup_thread = None
        self._update_start_sensitivity()
        if error:
            self._progress_bar.set_fraction(0)
            self._set_progress("Error")
            self._log("Backup failed: %s" % error)
        else:
            self._progress_bar.set_fraction(1.0)
            self._set_progress("Backup completed.")
        self._log("=== Backup ended ===\n")

    def _on_start_restore(self, _btn):
        device = self._get_selected_device_path()
        if not device or not self._restore_destination:
            return
        self._cancel_restore_requested = False
        self._progress_is_restore = True
        self._update_start_sensitivity()
        self._progress_bar.set_fraction(0)
        self._progress_bar.set_pulse_step(0.1)
        self._progress_activity_label.set_label("Extracting…")
        self._log("=== Restore started ===")
        self._log("Device: %s" % device)
        self._log("Destination: %s" % self._restore_destination)

        def run():
            try:
                run_restore(
                    device,
                    self._restore_destination,
                    archive_number=self._restore_archive_spin.get_value_as_int(),
                    on_progress=lambda m: GLib.idle_add(self._set_progress, m),
                    on_progress_update=lambda b, t, e: GLib.idle_add(
                        self._on_progress_update, b, t, e
                    ),
                    on_log=lambda line: GLib.idle_add(self._log, line),
                    cancel_check=lambda: self._cancel_restore_requested,
                )
                GLib.idle_add(self._restore_finished, None)
            except Exception as e:
                GLib.idle_add(self._restore_finished, e)

        self._restore_thread = threading.Thread(target=run, daemon=True)
        self._restore_thread.start()
        self._update_start_sensitivity()

    def _restore_finished(self, error):
        self._restore_thread = None
        self._progress_is_restore = False
        self._update_start_sensitivity()
        if error:
            self._progress_bar.set_fraction(0)
            self._set_progress("Error")
            self._log("Restore failed: %s" % error)
        else:
            self._progress_bar.set_fraction(1.0)
            self._set_progress("Restore completed.")
        self._log("=== Restore ended ===\n")

    def _on_cancel_operation(self, _btn):
        if self._backup_thread is not None:
            self._cancel_requested = True
            self._log("Cancel requested…")
        elif self._restore_thread is not None:
            self._cancel_restore_requested = True
            self._log("Cancel requested…")
        elif self._ltfs_rsync_thread is not None:
            self._cancel_ltfs_rsync_requested = True
            self._log("Cancel requested…")
