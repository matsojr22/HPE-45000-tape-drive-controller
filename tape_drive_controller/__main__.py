"""Entry point: run GUI or --list-devices CLI."""
import sys

from .tape.list_devices import list_tape_devices


def main() -> None:
    if "--list-devices" in sys.argv:
        devices = list_tape_devices()
        if not devices:
            print("No tape devices found. Check cables, HBA drivers, and dmesg.")
            sys.exit(1)
        for d in devices:
            print(d.display_name())
        sys.exit(0)

    from .ui.app import run_app
    run_app()


if __name__ == "__main__":
    main()
