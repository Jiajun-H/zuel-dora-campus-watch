"""Best-effort, bounded runtime evidence without exception text or credentials.

This journal is deliberately independent of SQLite and never raises an error
to its caller. A full or unwriteable journal stops accepting evidence; it does
not rotate files or hide a monitor failure behind repeated logging attempts.
"""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
import stat
import time


SCHEMA = "campus-watch-runtime-v1"
MAX_RECORD_BYTES = 2048
MAX_JOURNAL_BYTES = 512 * 1024
EVENTS = frozenset({
    "process_started", "process_exit", "unhandled_exception",
    "state_write_failed", "state_publish_failed", "watch_runtime_failed",
    "ledger_cleanup_failed",
})
ERROR_TYPES = frozenset({
    "UnexpectedError", "PermissionError", "OSError", "StateWriteError",
    "StoreBusy", "KeyboardInterrupt", "SystemExit", "ImportError",
    "ModuleNotFoundError", "ValueError", "TypeError", "RuntimeError",
    "FileNotFoundError", "TimeoutError", "DatabaseError", "OperationalError",
    "InterruptedError", "AssertionError", "UnicodeError", "JSONDecodeError",
})
STAGES = frozenset({
    "startup", "import", "cli", "run", "state_write", "state_publish",
    "temp_write", "temporary_write", "replace", "ledger_cleanup", "exit",
})
STATES = frozenset({"starting", "scanning", "waiting", "paused", "stopped", "failed", "exited"})
FACTS = frozenset({"exit_code", "error_type", "errno", "winerror", "attempts", "stage", "requested_state"})
REQUIRED = frozenset({"schema", "event", "pid", "at"})


def _integer(value: object, lower: int, upper: int) -> bool:
    return type(value) is int and lower <= value <= upper


def _timestamp(value: object) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and 0 <= value < 10**12


def _fact(key: str, value: object) -> object | None:
    if key == "error_type":
        return value if type(value) is str and value in ERROR_TYPES else "UnexpectedError"
    if key == "stage":
        return value if type(value) is str and value in STAGES else None
    if key == "requested_state":
        return value if type(value) is str and value in STATES else None
    if key == "exit_code":
        return value if _integer(value, -(2**31), 2**32 - 1) else None
    if key in {"errno", "winerror"}:
        return value if _integer(value, 0, 65535) else None
    if key == "attempts":
        return value if _integer(value, 0, 1000) else None
    return None


def _valid_record(record: object) -> bool:
    if type(record) is not dict or not REQUIRED <= record.keys():
        return False
    if set(record) - REQUIRED - FACTS - {"started_at"}:
        return False
    if record["schema"] != SCHEMA or type(record["event"]) is not str or record["event"] not in EVENTS:
        return False
    if not _integer(record["pid"], 1, 2**31 - 1) or not _timestamp(record["at"]):
        return False
    if "started_at" in record and not _timestamp(record["started_at"]):
        return False
    return all(_fact(key, record[key]) == record[key] and record[key] is not None
               for key in FACTS & record.keys())


def _regular_file(info: os.stat_result) -> bool:
    # Windows junctions/symlinks have a reparse attribute even when a runtime
    # follows them as ordinary files. Refuse either representation.
    return (stat.S_ISREG(info.st_mode)
            and not getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 1024))


def _existing_path(path: Path) -> os.stat_result | None:
    if path.suffix.lower() != ".jsonl":
        raise ValueError("invalid journal type")
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if not _regular_file(info) or info.st_size > MAX_JOURNAL_BYTES:
        raise ValueError("invalid journal file")
    return info


def _parse_line(line: bytes) -> dict | None:
    if not line or len(line) > MAX_RECORD_BYTES or not line.endswith(b"\n"):
        return None
    record = json.loads(line.decode("utf-8"))
    return record if _valid_record(record) else None


def record_runtime_event(path, event, /, *, started_at=None, **facts) -> bool:
    """Append one allowlisted record; any evidence failure returns ``False``.

    Unknown fact names are discarded. Unknown error class names become the
    fixed ``UnexpectedError`` label, rather than arbitrary user-controlled
    text. The journal does not include paths, argv, env, or exception strings.
    """
    descriptor = None
    try:
        if type(event) is not str or event not in EVENTS:
            return False
        record = {"schema": SCHEMA, "event": event, "pid": os.getpid(), "at": time.time()}
        if started_at is not None:
            if not _timestamp(started_at):
                return False
            record["started_at"] = started_at
        for key in FACTS & facts.keys():
            value = _fact(key, facts[key])
            if value is not None:
                record[key] = value
        if not _valid_record(record):
            return False
        payload = (json.dumps(record, ensure_ascii=True, separators=(",", ":")) + "\n").encode("utf-8")
        if len(payload) > MAX_RECORD_BYTES:
            return False
        journal = Path(path)
        previous = _existing_path(journal)
        if previous is not None:
            # Empty existing files are not a verified journal and may belong
            # to another application. Never repurpose them automatically.
            with journal.open("rb") as stream:
                if _parse_line(stream.readline(MAX_RECORD_BYTES + 1)) is None:
                    return False
        flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(journal, flags, 0o600)
        opened = os.fstat(descriptor)
        if not _regular_file(opened) or opened.st_size + len(payload) > MAX_JOURNAL_BYTES:
            return False
        if previous is not None and (previous.st_dev, previous.st_ino) != (opened.st_dev, opened.st_ino):
            return False
        if previous is None and opened.st_size != 0:
            return False
        # One append operation prevents ordinary concurrent readers from
        # observing multiple writes for one record. A partial low-level write
        # remains detectable by the bounded reader and does not get retried.
        return os.write(descriptor, payload) == len(payload)
    except BaseException:
        return False
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except BaseException:
                pass


def read_runtime_events(path) -> list[dict]:
    """Read a bounded, fully validated journal; malformed input returns []."""
    try:
        journal = Path(path)
        info = _existing_path(journal)
        if info is None:
            return []
        with journal.open("rb") as stream:
            if not _regular_file(os.fstat(stream.fileno())):
                return []
            payload = stream.read(MAX_JOURNAL_BYTES + 1)
        if len(payload) > MAX_JOURNAL_BYTES:
            return []
        records = []
        for line in payload.splitlines(keepends=True):
            record = _parse_line(line)
            if record is None:
                return []
            records.append(record)
        return records
    except BaseException:
        return []
