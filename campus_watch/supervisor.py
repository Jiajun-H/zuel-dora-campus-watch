"""Independent child-exit alarms; never read or change the post database."""

from __future__ import annotations

from contextlib import contextmanager, closing
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

from .notifier import BarkNotifier
from .runtime_evidence import read_runtime_events, record_runtime_event


SCHEMA = "campus-watch-supervisor-v1"
MAX_ATTEMPTS = 3
RETRY_DELAYS = (10, 30, 60)
REASONS = {
    "monitor_paused": "监控因保护性检查失败而暂停",
    "monitor_failed": "监控报告运行失败",
    "unexpected_exit": "监控进程意外退出",
    "unexpected_clean_exit": "监控进程退出，但没有主动停止证据",
    "child_launch_failed": "监控子进程未能启动",
    "supervisor_failed": "退出提醒守护发生异常，监控是否仍在运行需要核查",
}
ERROR_TYPES = frozenset({"OSError", "PermissionError", "TimeoutError", "ValueError",
                       "PushDeliveryError", "HttpRequestError", "OperationalError", "RuntimeError"})


def _number(value):
    return type(value) in (int, float) and math.isfinite(value) and 0 < value < 10**12


def _read_json(path):
    try:
        path = Path(path)
        if path.stat().st_size > 256 * 1024:
            return {}
        value = json.loads(path.read_text(encoding="utf-8-sig"))
        return value if type(value) is dict else {}
    except (OSError, ValueError, UnicodeError):
        return {}


def spawn_child(command, *, cwd, env):
    """Use the same venv without a console or inherited terminal handles."""
    command = list(command)
    options = {"cwd": cwd, "env": env, "stdin": subprocess.DEVNULL,
               "stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL, "close_fds": True}
    if os.name == "nt":
        windowed = Path(command[0]).with_name("pythonw.exe")
        if not windowed.is_file():
            raise FileNotFoundError("windowed interpreter unavailable")
        command[0] = str(windowed)
        startup = subprocess.STARTUPINFO()
        startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startup.wShowWindow = subprocess.SW_HIDE
        options.update(startupinfo=startup,
                       creationflags=subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP)
    else:
        options["start_new_session"] = True
    return subprocess.Popen(command, **options)


def _owned_process_ids(parent_pid):
    """Capture only the child's current descendants (venv redirector case)."""
    owned = {parent_pid}
    if os.name != "nt":
        return owned
    import ctypes
    from ctypes import wintypes
    class Entry(ctypes.Structure):
        _fields_ = [("dwSize", wintypes.DWORD), ("cntUsage", wintypes.DWORD),
                    ("th32ProcessID", wintypes.DWORD), ("th32DefaultHeapID", ctypes.c_size_t),
                    ("th32ModuleID", wintypes.DWORD), ("cntThreads", wintypes.DWORD),
                    ("th32ParentProcessID", wintypes.DWORD), ("pcPriClassBase", wintypes.LONG),
                    ("dwFlags", wintypes.DWORD), ("szExeFile", wintypes.WCHAR * 260)]
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateToolhelp32Snapshot.argtypes = (wintypes.DWORD, wintypes.DWORD)
    kernel.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    kernel.Process32FirstW.argtypes = (wintypes.HANDLE, ctypes.POINTER(Entry))
    kernel.Process32NextW.argtypes = (wintypes.HANDLE, ctypes.POINTER(Entry))
    kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
    snapshot = kernel.CreateToolhelp32Snapshot(2, 0)
    if snapshot in (None, ctypes.c_void_p(-1).value):
        return owned
    try:
        entry, parents = Entry(), {}
        entry.dwSize = ctypes.sizeof(Entry)
        valid = kernel.Process32FirstW(snapshot, ctypes.byref(entry))
        while valid:
            parents[int(entry.th32ProcessID)] = int(entry.th32ParentProcessID)
            valid = kernel.Process32NextW(snapshot, ctypes.byref(entry))
        for _ in range(8):
            expanded = owned | {pid for pid, parent in parents.items() if parent in owned}
            if expanded == owned:
                break
            owned = expanded
        return owned
    finally:
        kernel.CloseHandle(snapshot)


@contextmanager
def _guard_lock(path):
    """One supervisor per control path; a duplicate cannot spawn or alarm."""
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.open("a+b")
    locked = False
    try:
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        locked = True
        yield
    finally:
        if locked:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        stream.close()


class AlarmOutbox:
    """Independent SQLite outbox with attempts booked before each Bark POST.

    Reopening does not resend history. Explicit deliver(event_id) can resume
    that same event with its remaining bounded attempts and stable Bark id.
    """
    def __init__(self, path, *, clock=time.time, sleeper=time.sleep):
        self.path = Path(path).resolve()
        if self.path.suffix != ".sqlite3":
            raise ValueError("alarm outbox requires its own SQLite file")
        if self.path.exists() and self.path.stat().st_size:
            with closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)) as existing:
                tables = {r[0] for r in existing.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if tables - {"guard_alarm_outbox", "guard_alarm_requests", "sqlite_sequence"}:
                    raise ValueError("alarm outbox belongs to another database")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=5)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS guard_alarm_outbox (
                event_id TEXT PRIMARY KEY, created_at REAL NOT NULL,
                child_pid INTEGER NOT NULL, child_started_at REAL NOT NULL,
                exit_code INTEGER NOT NULL, reason_code TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL, attempt_started_at REAL, notified_at REAL,
                last_error TEXT
            );
            CREATE TABLE IF NOT EXISTS guard_alarm_requests (
                id INTEGER PRIMARY KEY, event_id TEXT NOT NULL, attempted_at REAL NOT NULL
            );
        """)
        self.clock, self.sleeper = clock, sleeper

    def close(self):
        self.connection.close()

    def create(self, *, child_pid, child_started_at, exit_code, reason_code, created_at):
        if (type(child_pid) is not int or not 0 <= child_pid <= 2**31 - 1
                or not _number(child_started_at) or not _number(created_at)
                or type(exit_code) is not int or not -(2**31) <= exit_code < 2**32
                or reason_code not in REASONS):
            raise ValueError("invalid safe exit alarm metadata")
        identifier = hashlib.sha256(f"58:14:{child_pid}:{child_started_at:.9f}".encode("ascii")).hexdigest()
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO guard_alarm_outbox(event_id,created_at,child_pid,child_started_at,"
                "exit_code,reason_code,next_attempt_at) VALUES(?,?,?,?,?,?,?)",
                (identifier, created_at, child_pid, child_started_at, exit_code, reason_code, created_at))
        return identifier

    def event(self, event_id):
        row = self.connection.execute("SELECT * FROM guard_alarm_outbox WHERE event_id=?", (event_id,)).fetchone()
        if row is None:
            raise ValueError("alarm event not found")
        return dict(row)

    @property
    def request_count(self):
        return self.connection.execute("SELECT COUNT(*) FROM guard_alarm_requests").fetchone()[0]

    def deliver(self, event_id, sender, *, progress=lambda event: None):
        with _guard_lock(self.path.with_suffix(".delivery.lock")):
            return self._deliver(event_id, sender, progress=progress)

    def _deliver(self, event_id, sender, *, progress):
        while True:
            event = self.event(event_id)
            if event["state"] == "sent":
                return event
            if event["attempts"] >= MAX_ATTEMPTS:
                if event["state"] != "exhausted":
                    with self.connection:
                        self.connection.execute("UPDATE guard_alarm_outbox SET state='exhausted',"
                            "last_error=COALESCE(last_error,'DeliveryOutcomeUnknown') WHERE event_id=?", (event_id,))
                return self.event(event_id)
            delay = max(0, event["next_attempt_at"] - self.clock())
            while delay > 0:
                self.sleeper(min(1, delay))
                delay = max(0, event["next_attempt_at"] - self.clock())
            with self.connection:
                changed = self.connection.execute(
                    "UPDATE guard_alarm_outbox SET state='sending',attempts=attempts+1,attempt_started_at=?,"
                    "next_attempt_at=? WHERE event_id=? AND state<>'sent' AND attempts=? AND attempts<?",
                    (self.clock(), self.clock() + RETRY_DELAYS[event["attempts"]], event_id, event["attempts"], MAX_ATTEMPTS))
                if not changed.rowcount:
                    continue
                self.connection.execute("INSERT INTO guard_alarm_requests(event_id,attempted_at) VALUES(?,?)",
                                        (event_id, self.clock()))
            event = self.event(event_id)
            self._progress(progress, event)
            try:
                sender.send(event)
            except Exception as failure:
                error_type = type(failure).__name__
                error_type = error_type if error_type in ERROR_TYPES else "UnexpectedError"
                with self.connection:
                    self.connection.execute(
                        "UPDATE guard_alarm_outbox SET state=?,last_error=?,next_attempt_at=? WHERE event_id=? AND state='sending'",
                        ("exhausted" if event["attempts"] >= MAX_ATTEMPTS else "pending", error_type,
                         self.clock() + RETRY_DELAYS[event["attempts"] - 1], event_id))
            else:
                with self.connection:
                    self.connection.execute(
                        "UPDATE guard_alarm_outbox SET state='sent',notified_at=?,last_error=NULL WHERE event_id=?",
                        (self.clock(), event_id))
            self._progress(progress, self.event(event_id))

    @staticmethod
    def _progress(callback, event):
        try:
            callback(event)
        except Exception:
            # Diagnostic publication must never block the actual delivery.
            pass


class AlarmSender:
    def __init__(self):
        self.notifier = BarkNotifier.from_env(require_summary=False)

    def send(self, event):
        reason = REASONS.get(event["reason_code"], "监控意外停止")
        stamp = datetime.fromtimestamp(event["created_at"], timezone(timedelta(hours=8))).isoformat(timespec="seconds")
        payload = {
            "device_key": self.notifier.device_key, "title": "【监控已停止】中南大悬赏",
            "body": f"{reason}。\n检测时间：{stamp}\n退出代码：{event['exit_code']}\n当前没有自动检查；请打开查看监控状态。",
            "group": "campus-watch-health", "id": "campus-watch-health-" + event["event_id"],
            "isArchive": "1",
        }
        if event["reason_code"] == "supervisor_failed":
            payload["title"] = "【守护异常】中南大悬赏"
            payload["body"] = f"{reason}。\n检测时间：{stamp}\n请打开查看监控状态；守护未自动重启监控。"
        self.notifier._send_payload(payload)


class WatchSupervisor:
    def __init__(self, child_command, *, cwd, watch_state_path, stop_path,
                 guard_state_path, alarm_db_path, runtime_path=None, env=None,
                 poll_seconds=1, clock=time.time, sleeper=time.sleep,
                 spawn=spawn_child, sender=None):
        if type(poll_seconds) not in (int, float) or not 0 < poll_seconds <= 1:
            raise ValueError("supervisor polling must be at most one second")
        self.command, self.cwd = list(child_command), Path(cwd).resolve()
        if not self.command or not self.cwd.is_dir():
            raise ValueError("invalid supervisor child configuration")
        self.watch_state_path, self.stop_path, self.guard_state_path, self.alarm_db_path = (
            Path(p).resolve() for p in (watch_state_path, stop_path, guard_state_path, alarm_db_path))
        if any(p.suffix != ".json" for p in (self.watch_state_path, self.stop_path, self.guard_state_path)):
            raise ValueError("supervisor control files must be JSON")
        controls = (self.watch_state_path, self.stop_path, self.guard_state_path,
                    self.guard_state_path.with_suffix(".json.tmp"), self.alarm_db_path)
        if len({os.path.normcase(str(p)) for p in controls}) != len(controls):
            raise ValueError("supervisor control paths collide")
        previous = _read_json(self.guard_state_path)
        if self.guard_state_path.exists() and previous.get("schema") != SCHEMA:
            raise ValueError("guardian state belongs to another document")
        self.runtime_path = Path(runtime_path).resolve() if runtime_path else None
        self.guard_journal = self.guard_state_path.with_suffix(".runtime.jsonl")
        self.env = os.environ.copy() if env is None else dict(env)
        self.receipt_path = self.guard_state_path.with_suffix(".child.json")
        self.env["CAMPUS_WATCH_CHILD_RECEIPT"] = str(self.receipt_path)
        self.poll_seconds, self.clock, self.sleeper = poll_seconds, clock, sleeper
        self.spawn, self.sender = spawn, sender
        self.started_at = self.clock()
        self.child = None
        self.child_started_at = self.started_at
        self.owned_state = None
        self.owned_pids = set()
        self.alarm_attempts, self.alarm_state, self.alarm_requests = 0, "none", 0
        self.state_publish_error = False

    def publish(self, state, reason_code, **extra):
        value = {"schema": SCHEMA, "state": state, "reason_code": reason_code,
                 "supervisor_pid": os.getpid(), "pid": os.getpid(), "started_at": self.started_at,
                 "updated_at": self.clock(), "child_wrapper_pid": self.child.pid if self.child else None,
                 "child_pid": self.owned_state["pid"] if self.owned_state else self.child.pid if self.child else None,
                 "child_started_at": self.owned_state["started_at"] if self.owned_state else self.child_started_at,
                 "watch_started_at": self.owned_state["started_at"] if self.owned_state else None,
                 "alarm_state": self.alarm_state, "alarm_attempts": self.alarm_attempts,
                 "alarm_requests": self.alarm_requests, "state_publish_error": self.state_publish_error,
                 **extra}
        try:
            self.guard_state_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.guard_state_path.with_suffix(".json.tmp")
            temporary.write_text(json.dumps(value, ensure_ascii=True, allow_nan=False), encoding="utf-8")
            for attempt in range(4):
                try:
                    temporary.replace(self.guard_state_path)
                    return True
                except OSError as failure:
                    if os.name == "nt" and getattr(failure, "winerror", None) in (5, 32, 33) and attempt < 3:
                        self.sleeper(0.025)
                    else:
                        raise
        except (OSError, ValueError):
            self.state_publish_error = True
            return False

    def observe_child_state(self):
        self.owned_pids.update(_owned_process_ids(self.child.pid))
        receipt = _read_json(self.receipt_path)
        if (receipt.get("schema") == "campus-watch-child-v1"
                and type(receipt.get("pid")) is int and 0 < receipt["pid"] < 2**31
                and (receipt["pid"] == self.child.pid or receipt.get("parent_pid") == self.child.pid)
                and _number(receipt.get("started_at"))
                and self.child_started_at <= receipt["started_at"] <= self.clock()):
            self.owned_pids.add(receipt["pid"])
        state = _read_json(self.watch_state_path)
        if (type(state.get("pid")) is int and 0 < state["pid"] < 2**31
                and state["pid"] in self.owned_pids
                and _number(state.get("started_at"))
                and self.child_started_at <= state["started_at"] <= self.clock()
                and state.get("state") in {"starting", "scanning", "waiting", "paused", "failed", "stopped"}):
            if self.owned_state is None or (state["pid"], state["started_at"]) == (
                    self.owned_state["pid"], self.owned_state["started_at"]):
                self.owned_state = {"pid": state["pid"], "started_at": state["started_at"],
                                    "state": state["state"],
                                    "stop_requested": state.get("reason") == "stop_requested"}

    def exit_time(self, detected_at):
        if self.runtime_path is not None:
            try:
                for event in reversed(read_runtime_events(self.runtime_path)):
                    pid = self.owned_state["pid"] if self.owned_state else self.child.pid
                    if (event["event"] == "process_exit" and event["pid"] == pid
                            and self.child_started_at <= event["at"] <= detected_at):
                        return event["at"]
            except Exception:
                pass
        return detected_at

    def classify_exit(self, exit_code, detected_at):
        state = self.owned_state
        if state and state["state"] in {"paused", "failed"}:
            return "monitor_paused" if state["state"] == "paused" else "monitor_failed"
        if exit_code == 5 and state is None:
            return "duplicate_instance"
        if state and state["state"] == "stopped" and state["stop_requested"]:
            return "manual_stop"
        marker = _read_json(self.stop_path)
        if (state and type(marker.get("pid")) is int and marker["pid"] == state["pid"]
                and marker.get("started_at") == state["started_at"]
                and _number(marker.get("requested_at"))
                and state["started_at"] <= marker["requested_at"] <= self.exit_time(detected_at)):
            return "manual_stop"
        return "unexpected_clean_exit" if exit_code == 0 else "unexpected_exit"

    def alarm_progress(self, event):
        self.alarm_state, self.alarm_attempts = event["state"], event["attempts"]
        self.alarm_requests = self.outbox.request_count
        self.publish("notifying", event["reason_code"], alarm_event_id=event["event_id"])

    def run(self):
        lock = self.guard_state_path.with_suffix(".json.lock")
        try:
            with _guard_lock(lock):
                self.outbox = AlarmOutbox(self.alarm_db_path, clock=self.clock, sleeper=self.sleeper)
                try:
                    self.publish("starting", "child_starting")
                    self.child_started_at = self.clock()
                    try:
                        self.child = self.spawn(self.command, cwd=self.cwd, env=self.env)
                    except Exception:
                        exit_code, reason = 1, "child_launch_failed"
                    else:
                        while self.child.poll() is None:
                            self.observe_child_state()
                            self.publish("watching", "child_running")
                            self.sleeper(self.poll_seconds)
                        exit_code = self.child.returncode
                        self.observe_child_state()
                        reason = self.classify_exit(exit_code, self.clock())
                    if reason in {"manual_stop", "duplicate_instance"}:
                        self.publish("stopped", reason, exit_code=exit_code)
                        return reason
                    pid = self.owned_state["pid"] if self.owned_state else self.child.pid if self.child else 0
                    started = self.owned_state["started_at"] if self.owned_state else self.child_started_at
                    event_id = self.outbox.create(child_pid=pid, child_started_at=started,
                        exit_code=exit_code, reason_code=reason, created_at=self.clock())
                    if self.sender is None:
                        try:
                            self.sender = AlarmSender()
                        except Exception:
                            self.alarm_state = "configuration_missing"
                            self.publish("stopped", reason, alarm_event_id=event_id, exit_code=exit_code)
                            return "alarm_pending"
                    event = self.outbox.deliver(event_id, self.sender, progress=self.alarm_progress)
                    self.publish("stopped", reason, alarm_event_id=event_id, exit_code=exit_code)
                    return "alarm_sent" if event["state"] == "sent" else "alarm_exhausted"
                except Exception as failure:
                    record_runtime_event(self.guard_journal, "unhandled_exception", started_at=self.started_at,
                                         stage="run", error_type=type(failure).__name__,
                                         errno=getattr(failure, "errno", None), winerror=getattr(failure, "winerror", None))
                    # Alert only for this owned child; never reinterpret old
                    # watcher state or add an unbooked external retry after POST.
                    if self.child is not None and self.alarm_state == "none":
                        try:
                            pid = self.owned_state["pid"] if self.owned_state else self.child.pid
                            start = self.owned_state["started_at"] if self.owned_state else self.child_started_at
                            event_id = self.outbox.create(child_pid=pid, child_started_at=start,
                                exit_code=3, reason_code="supervisor_failed", created_at=self.clock())
                            sender = self.sender or AlarmSender()
                            self.outbox.deliver(event_id, sender, progress=self.alarm_progress)
                        except Exception:
                            self.alarm_state = "evidence_failed"
                    self.publish("failed", "supervisor_failed")
                    return "supervisor_failed"
                finally:
                    try:
                        self.outbox.close()
                    except Exception as failure:
                        record_runtime_event(self.guard_journal, "ledger_cleanup_failed", started_at=self.started_at,
                                             stage="ledger_cleanup", error_type=type(failure).__name__)
        except OSError:
            return "supervisor_busy_or_io_failed"


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description="Windowless watch child supervisor")
    parser.add_argument("--state", required=True)
    parser.add_argument("--stop", required=True)
    parser.add_argument("--guard-state", required=True)
    parser.add_argument("--alarm-db", required=True)
    parser.add_argument("--runtime")
    parser.add_argument("--bark-server")
    parser.add_argument("--link-hosts")
    parser.add_argument("--cwd", default=str(Path.cwd()))
    parser.add_argument("child", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    child = args.child[1:] if args.child[:1] == ["--"] else args.child
    try:
        if args.bark_server:
            from .notifier import _server_base
            os.environ["BARK_SERVER"] = _server_base(args.bark_server)
        if args.link_hosts:
            os.environ["BARK_LINK_HOSTS"] = args.link_hosts
        result = WatchSupervisor(child, cwd=args.cwd, watch_state_path=args.state, stop_path=args.stop,
            guard_state_path=args.guard_state, alarm_db_path=args.alarm_db, runtime_path=args.runtime).run()
        return 0 if result in {"manual_stop", "duplicate_instance", "alarm_sent"} else 3
    except Exception as failure:
        record_runtime_event(Path(args.guard_state).with_suffix(".runtime.jsonl"), "unhandled_exception",
                             stage="run", error_type=type(failure).__name__, errno=getattr(failure, "errno", None))
        return 3
