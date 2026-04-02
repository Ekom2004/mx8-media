#!/usr/bin/env python3
from __future__ import annotations

import io
import sys


def main() -> int:
    try:
        from rembg import remove
    except Exception as exc:  # pragma: no cover - runtime dependency path
        print(
            "remove_background requires Python package 'rembg' in the worker environment",
            file=sys.stderr,
        )
        print(str(exc), file=sys.stderr)
        return 1

    input_bytes = sys.stdin.buffer.read()
    if not input_bytes:
        print("remove_background received empty input", file=sys.stderr)
        return 1

    try:
        output = remove(input_bytes)
    except Exception as exc:  # pragma: no cover - runtime model path
        print(f"remove_background failed: {exc}", file=sys.stderr)
        return 1

    if isinstance(output, bytes):
        sys.stdout.buffer.write(output)
        return 0

    try:
        output.save(sys.stdout.buffer, format="PNG")
    except Exception as exc:  # pragma: no cover - fallback path
        print(f"remove_background output encode failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
