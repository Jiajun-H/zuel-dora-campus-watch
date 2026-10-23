"""Read-only terminal message stream and explicit controls for the detached watcher."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone, timedelta
from typing import Any, Callable

from rich.text import Text
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Static

from .runtime_evidence import read_runtime_events


BEIJING = timezone(timedelta(hours=8))
DB_RELATIVE_PATH = Path("data/reward_58_14.sqlite3")
RUNTIME_EVENT_NAMES = {
    "process_started", "process_exit", "unhandled_exception", "state_write_failed",
    "state_publish_failed", "watch_runtime_failed", "ledger_cleanup_failed",
}
CATEGORY_EVIDENCE = {"server_filter", "response_field", "fixture_scope"}
SAFE_ERROR_TYPES = {
    "UnexpectedError", "PermissionError", "OSError", "StateWriteError", "StoreBusy",
    "KeyboardInterrupt", "SystemExit", "ImportError", "ModuleNotFoundError", "ValueError",
    "TypeError", "RuntimeError", "FileNotFoundError", "TimeoutError", "DatabaseError",
    "OperationalError", "InterruptedError", "AssertionError", "UnicodeError", "JSONDecodeError",
    "PushDeliveryError", "HttpRequestError",
}
STATE_LABELS = {
    "starting": ("启动中", "yellow"),
    "scanning": ("扫描中", "cyan"),
    "waiting": ("监控中", "green"),
    "paused": ("暂停", "yellow"),
    "stopped": ("已停止", "dim"),
    "not_started": ("未启动", "dim"),
    "failed_exit": ("异常退出", "red"),
    "unexpected_exit": ("异常退出", "red"),
    "exited": ("已退出", "yellow"),
    "state_publication_failed": ("状态发布故障", "red"),
    "status_unavailable": ("状态暂不可读", "yellow"),
    "status_read_error": ("状态读取异常", "red"),
}
REASON_LABELS = {
    "round_failed_requires_review": "本轮检查或推送失败，需要核查后再启动",
    "attempted_pending_requires_review": "存在发送结果不明的待发通知，需要先核对手机和记录",
    "visibility_snapshot_requires_rebaseline": "列表快照需要人工核查",
    "fresh_parameter_missing": "缺少本轮 freshKey，监控已暂停",
    "insufficient_daily_round_budget": "剩余请求预算不足",
    "startup_or_runtime_failed": "启动或运行过程中发生错误",
    "stop_requested": "已收到明确停止请求",
    "next_round_scheduled": "下一轮检查已排期",
    "bounded_round_started": "正在检查最近列表",
    "fetching_initial_parameter": "正在获取首次 freshKey",
}
FRESHNESS_LABELS = {
    "not_started": "尚未启动",
    "initial_parameter_acquired": "已获取初始 freshKey",
    "initial_parameter_required": "等待初始 freshKey",
    "fresh_request_failed_stop": "freshKey 刷新失败，监控暂停",
    "fresh_request_failed": "freshKey 刷新失败",
    "refreshed": "本轮已刷新",
    "same_key_returned": "本轮刷新成功，值未变化",
    "fixed_interval": "等待固定刷新周期",
    "cooldown": "刷新冷却中",
    "daily_fresh_cap": "已达到今日 freshKey 刷新上限",
    "preserve_required_pagination": "为列表请求保留预算，暂不刷新",
    "process_mode": "使用进程级 freshKey 策略",
    "cached_age_threshold": "按缓存年龄策略刷新",
    "age_below_threshold": "缓存仍在刷新阈值内",
    "age_unknown": "尚无可用缓存年龄证据",
    "not_cached_evidence": "尚无缓存年龄证据",
    "adaptive_budget_pacing": "按剩余请求预算安排刷新",
    "budget_changed_preserve_pagination": "为列表请求保留预算，暂不刷新",
}


@dataclass(frozen=True, slots=True)
class RecentPost:
    post_id: int
    first_seen_at: int
    preview: str
    bark_status: str


@dataclass(frozen=True, slots=True)
class DatabaseSnapshot:
    baseline_count: int = 0
    distinct_count: int = 0
    new_count: int = 0
    bark_accepted_count: int = 0
    pending_count: int = 0
    bark_failure_count: int = 0
    uncertain_count: int = 0
    last_bark_error_type: str | None = None
    last_success_at: int | None = None
    last_pages: int = 0
    last_posts: int = 0
    recent_posts: tuple[RecentPost, ...] = ()
    error: str | None = None


@dataclass(frozen=True, slots=True)
class DashboardSnapshot:
    runtime: dict[str, Any]
    database: DatabaseSnapshot
    events: tuple[dict[str, Any], ...] = ()
    status_error: str | None = None


@dataclass(frozen=True, slots=True)
class OperationResult:
    ok: bool
    status: str
    error_type: str | None = None


class LocalReadError(RuntimeError):
    """A local-only status/database read failed; exception text stays private."""


def _safe_error_type(value: Any) -> str | None:
    return value if type(value) is str and value in SAFE_ERROR_TYPES else None


def _safe_count(value: Any, default: int = 0) -> int:
    if type(value) is int and 0 <= value <= 1_000_000_000:
        return value
    if type(value) is float and math.isfinite(value) and 0 <= value <= 1_000_000_000:
        return int(value)
    return default


def _finite_number(value: Any) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def _safe_preview(value: Any, limit: int = 280) -> str:
    if type(value) is not str:
        return "[正文暂不可用]"
    text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", value)
    text = "".join(char if char in "\n\t" or ord(char) >= 32 else " " for char in text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"(?i)\b(?:COOKIE|SET-COOKIE)\b\s*[:=].*$", "Cookie: [敏感值已隐藏]", text)
    secret_patterns = (
        r"(?i)\b(?:BARK_KEY|DORA_FRESH_KEY_14|FRESHKEY|ACCESS[_ -]?TOKEN|TOKEN|KEY|SECRET|COOKIE|SET-COOKIE|AUTHORIZATION|API[_ -]?KEY|AES(?:[_ -]?KEY)?|IV)\b\s*[:=]\s*\S+",
        r"(?i)\bBearer\s+\S+",
        r"(?i)https?://\S+",
        r"\b[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{10,}\b",
    )
    for pattern in secret_patterns:
        text = re.sub(pattern, "[敏感值已隐藏]", text)
    text = re.sub(r"(?<!\d)(?:\+?86[ -]?)?1[3-9]\d{9}(?!\d)", "1**********", text)
    text = re.sub(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", "[邮箱已隐藏]", text)
    return text[:limit] + ("…" if len(text) > limit else "") if text else "[正文暂不可用]"


def read_database_snapshot(path: str | Path, *, limit: int = 100) -> DatabaseSnapshot:
    """Read only validated reward rows; never opens the monitor database writable."""
    database = Path(path)
    if not database.is_file():
        return DatabaseSnapshot(error="数据库尚未建立")
    uri = database.resolve().as_uri() + "?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=0.25)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA busy_timeout=250")
            tables = {row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if not {"posts", "runs"} <= tables:
                raise LocalReadError("数据库表结构不可识别")
            columns = {row[1] for row in connection.execute("PRAGMA table_info(posts)")}
            required = {
                "post_id", "first_seen_at", "status", "source_school", "top", "attempts", "last_error",
                "notified_at", "notification_eligible", "source_category", "category_evidence", "body_text",
                "body_status", "body_evidence", "body_encoding", "delivery_mode", "send_started_at",
            }
            if not required <= columns:
                raise LocalReadError("数据库帖子字段不完整")
            counts = connection.execute(
                "SELECT COUNT(*) AS distinct_count,"
                "COALESCE(SUM(status='baseline'),0) AS baseline_count,"
                "COALESCE(SUM(status IN ('pending','sent') AND notification_eligible=1),0) AS new_count,"
                "COALESCE(SUM(status='sent' AND notification_eligible=1),0) AS accepted_count,"
                "COALESCE(SUM(status='pending' AND notification_eligible=1),0) AS pending_count,"
                "COALESCE(SUM(status='pending' AND notification_eligible=1 AND attempts>0),0) AS failure_count,"
                "COALESCE(SUM(status='pending' AND notification_eligible=1 AND send_started_at IS NOT NULL AND attempts=0),0) AS uncertain_count "
                "FROM posts WHERE source_school=58 AND source_category=14 "
                "AND category_evidence IN ('server_filter','response_field','fixture_scope')"
            ).fetchone()
            bark_error_row = connection.execute(
                "SELECT last_error FROM posts WHERE source_school=58 AND status='pending' "
                "AND notification_eligible=1 AND attempts>0 AND last_error IS NOT NULL "
                "ORDER BY first_seen_at DESC,post_id DESC LIMIT 1"
            ).fetchone()
            run = connection.execute(
                "SELECT finished_at,pages,fetched_count FROM runs "
                "WHERE finished_at IS NOT NULL AND status IN ('ok','partial','baseline','rebaselined') "
                "ORDER BY finished_at DESC,id DESC LIMIT 1"
            ).fetchone()
            rows = connection.execute(
                "SELECT post_id,first_seen_at,status,attempts,last_error,send_started_at,body_text,body_status,"
                "body_evidence,body_encoding,delivery_mode FROM posts "
                "WHERE source_school=58 AND top=0 AND source_category=14 "
                "AND category_evidence IN ('server_filter','response_field','fixture_scope') "
                "AND notification_eligible=1 AND status IN ('pending','sent') "
                "ORDER BY first_seen_at DESC,post_id DESC LIMIT ?", (limit,)
            ).fetchall()
            posts = []
            for row in rows:
                body_is_verified = (
                    row["body_status"] in ("full", "preview") and bool(row["body_text"])
                    and bool(row["body_evidence"]) and row["body_encoding"] not in (None, "", "unknown")
                    and row["delivery_mode"] != "metadata"
                )
                preview = _safe_preview(row["body_text"]) if body_is_verified else "[正文暂不可用]"
                if row["status"] == "sent":
                    bark_status = "Bark 已接受"
                elif row["send_started_at"] is not None and row["attempts"] == 0:
                    bark_status = "发送状态待核对"
                elif row["attempts"] > 0 or row["last_error"]:
                    bark_status = "Bark 发送失败"
                else:
                    bark_status = "待发送"
                posts.append(RecentPost(
                    post_id=int(row["post_id"]), first_seen_at=int(row["first_seen_at"]),
                    preview=preview, bark_status=bark_status,
                ))
            return DatabaseSnapshot(
                baseline_count=int(counts["baseline_count"]),
                distinct_count=int(counts["distinct_count"]),
                new_count=int(counts["new_count"]),
                bark_accepted_count=int(counts["accepted_count"]),
                pending_count=int(counts["pending_count"]),
                bark_failure_count=int(counts["failure_count"]),
                uncertain_count=int(counts["uncertain_count"]),
                last_bark_error_type=_safe_error_type(bark_error_row[0]) if bark_error_row else None,
                last_success_at=int(run["finished_at"]) if run else None,
                last_pages=int(run["pages"]) if run else 0,
                last_posts=int(run["fetched_count"]) if run else 0,
                recent_posts=tuple(posts),
            )
        finally:
            connection.close()
    except LocalReadError:
        raise
    except (sqlite3.Error, OSError, ValueError) as exc:
        raise LocalReadError("SQLite 暂时不可读") from None


def read_recent_events(root: Path, *, limit: int = 8) -> tuple[dict[str, Any], ...]:
    events = []
    for name in ("reward_watch.runtime.jsonl", "reward_watch.supervisor.runtime.jsonl"):
        for record in read_runtime_events(root / "data" / name):
            if record.get("event") not in RUNTIME_EVENT_NAMES:
                continue
            events.append(record)
    events.sort(key=lambda item: float(item.get("at", 0)))
    return tuple(events[-limit:])


def resolve_powershell() -> str:
    if os.name == "nt":
        candidate = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32/WindowsPowerShell/v1.0/powershell.exe"
        if candidate.is_file():
            return str(candidate)
    for name in ("powershell.exe", "powershell", "pwsh"):
        found = shutil.which(name)
        if found:
            return found
    raise LocalReadError("Windows PowerShell 不可用")


class PowerShellController:
    """Use the project's existing bounded Get/Start/Stop PowerShell controls."""

    def __init__(self, root: str | Path, *, shell: str | None = None, timeout: float = 30):
        self.root = Path(root).resolve()
        self.scripts = self.root / "scripts"
        self.shell = shell or resolve_powershell()
        self.timeout = timeout

    def _run(
        self,
        name: str,
        args: tuple[str, ...] = (),
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        script = self.scripts / name
        if not script.is_file():
            raise LocalReadError("监控控制脚本不可用")
        command = [self.shell, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script), *args]
        flags = 0
        if os.name == "nt":
            flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        try:
            completed = subprocess.run(
                command, cwd=self.root, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                timeout=self.timeout if timeout is None else min(timeout, self.timeout),
                creationflags=flags, check=False,
            )
        except (OSError, subprocess.SubprocessError):
            raise LocalReadError("本地监控控制调用失败") from None
        output = completed.stdout
        if isinstance(output, bytes):
            try:
                output = output.decode("utf-8-sig")
            except UnicodeError:
                raise LocalReadError("本地监控控制返回格式不可读") from None
        try:
            value = json.loads(output)
        except (TypeError, ValueError):
            raise LocalReadError("本地监控控制返回格式不可读") from None
        if type(value) is not dict:
            raise LocalReadError("本地监控控制返回格式不可读")
        if completed.returncode:
            error_type = _safe_error_type(value.get("error_type"))
            return {"status": "control_failed", "error_type": error_type}
        return value

    def status(self) -> dict[str, Any]:
        return self._run("GetRewardWatch.ps1", timeout=3)

    def start(self) -> OperationResult:
        freshness = self.root / "config" / "freshness.daily.json"
        script = self.scripts / "StartRewardWatch.ps1"
        if not script.is_file():
            return OperationResult(False, "control_failed")
        try:
            before = self.status()
            if before.get("running"):
                return OperationResult(True, "already_running")
            if before.get("process_alive") or before.get("guardian_alive"):
                return OperationResult(False, "control_failed")
            command = [self.shell, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script),
                       "-FreshnessConfig", str(freshness)]
            completed = subprocess.run(
                command, cwd=self.root, stdin=None, stdout=None, stderr=None,
                timeout=self.timeout, creationflags=0, check=False,
            )
            if completed.returncode:
                return OperationResult(False, "control_failed")
            after = self.status()
        except (OSError, subprocess.SubprocessError, LocalReadError):
            return OperationResult(False, "control_failed")
        if after.get("running") or after.get("process_alive") or after.get("guardian_alive"):
            return OperationResult(True, "launching")
        monitor = after.get("monitor") if type(after.get("monitor")) is dict else {}
        summary = monitor.get("last_summary") if type(monitor.get("last_summary")) is dict else {}
        error_type = _safe_error_type(monitor.get("error_type")) or _safe_error_type(summary.get("error_type"))
        return OperationResult(False, "not_launched", error_type)

    def stop(self) -> OperationResult:
        result = self._run("StopRewardWatch.ps1", timeout=5)
        error_type = _safe_error_type(result.get("error_type"))
        status = result.get("status")
        if type(status) is not str or status not in {"stop_requested", "not_running"}:
            status = "control_failed"
        status = str(status)
        return OperationResult(status in {"stop_requested", "not_running"}, status, error_type)


def collect_dashboard(
    root: str | Path,
    controller: Any,
    *,
    database_reader: Callable[[str | Path], DatabaseSnapshot] = read_database_snapshot,
) -> DashboardSnapshot:
    root_path = Path(root)
    status_error = None
    try:
        runtime = controller.status()
        if type(runtime) is not dict:
            raise LocalReadError("状态返回格式不可读")
    except Exception as exc:
        runtime = {"runtime_state": "status_read_error", "running": False, "process_alive": False}
        status_error = type(exc).__name__ if isinstance(exc, LocalReadError) else "LocalReadError"
    try:
        database = database_reader(root_path / DB_RELATIVE_PATH)
    except Exception:
        database = DatabaseSnapshot(error="SQLite 暂时不可读")
    return DashboardSnapshot(runtime, database, read_recent_events(root_path), status_error)


def _epoch_label(value: int | float | None) -> str:
    if type(value) not in (int, float) or value <= 0:
        return "暂无"
    try:
        return datetime.fromtimestamp(value, BEIJING).strftime("%m-%d %H:%M:%S")
    except (OverflowError, OSError, ValueError):
        return "暂无"


def _runtime_state(runtime: dict[str, Any], status_error: str | None) -> tuple[str, str]:
    if status_error:
        return STATE_LABELS["status_read_error"]
    runtime_state = runtime.get("runtime_state")
    if type(runtime_state) is str and runtime_state in STATE_LABELS:
        return STATE_LABELS[runtime_state]
    monitor = runtime.get("monitor") if type(runtime.get("monitor")) is dict else {}
    monitor_state = monitor.get("state")
    if monitor_state == "paused":
        return STATE_LABELS["paused"]
    if monitor_state == "stopped":
        return STATE_LABELS["stopped"]
    if runtime.get("running") and type(monitor_state) is str and monitor_state in STATE_LABELS:
        return STATE_LABELS[monitor_state]
    guardian = runtime.get("guardian") if type(runtime.get("guardian")) is dict else {}
    if runtime.get("guardian_alive") and guardian.get("state") in ("starting", "watching", "notifying"):
        return STATE_LABELS["starting"]
    if type(runtime_state) is str and runtime_state in {"failed_exit", "unexpected_exit", "exited"}:
        return STATE_LABELS[runtime_state]
    return STATE_LABELS["not_started"]


def _freshness_label(runtime: dict[str, Any]) -> str:
    monitor = runtime.get("monitor") if type(runtime.get("monitor")) is dict else {}
    freshness = monitor.get("freshness") if type(monitor.get("freshness")) is dict else {}
    reason = freshness.get("reason")
    label = FRESHNESS_LABELS.get(reason, "状态未知") if type(reason) is str else "状态未知"
    elapsed = freshness.get("seconds_since_last_successful_acquisition")
    if _finite_number(elapsed) and elapsed >= 0:
        label += f" · 最近获取 {int(elapsed)} 秒前"
    return label


def _runtime_description(snapshot: DashboardSnapshot) -> str:
    runtime = snapshot.runtime
    monitor = runtime.get("monitor") if type(runtime.get("monitor")) is dict else {}
    if snapshot.status_error:
        return "本地状态 JSON 暂时不可读；界面继续运行，不会把旧状态显示为正常。"
    if runtime.get("runtime_state") == "failed_exit":
        failure = runtime.get("last_runtime_failure") or {}
        process_exit = runtime.get("last_process_exit") or {}
        candidate_event = failure.get("event") if type(failure) is dict else None
        event_name = candidate_event if type(candidate_event) is str and candidate_event in RUNTIME_EVENT_NAMES else None
        failure_type = _safe_error_type(failure.get("error_type")) if type(failure) is dict else None
        exit_code = process_exit.get("exit_code") if type(process_exit) is dict else None
        return "监控异常退出 · " + "; ".join(filter(None, (
            f"故障 {event_name}" if event_name else None,
            f"错误类型 {failure_type}" if failure_type else None,
            f"退出码 {exit_code}" if type(exit_code) is int else None,
        )))
    if runtime.get("runtime_state") == "unexpected_exit":
        return "监控进程已消失，旧状态不能证明仍在检查。"
    if runtime.get("runtime_state") == "status_unavailable":
        return "后台进程存活，但状态文件尚不可用；当前健康状态无法确认。"
    reason = monitor.get("reason")
    error_type = _safe_error_type(monitor.get("error_type"))
    summary = monitor.get("last_summary") if type(monitor.get("last_summary")) is dict else {}
    summary_error = _safe_error_type(summary.get("error_type"))
    details = []
    if reason:
        details.append(REASON_LABELS.get(reason, "原因代码未识别") if type(reason) is str else "原因代码未识别")
    if error_type or summary_error:
        details.append(f"错误类型：{error_type or summary_error}")
    database = snapshot.database
    if database.bark_failure_count:
        failure = f"Bark 发送失败记录：{database.bark_failure_count} 条"
        if database.last_bark_error_type:
            failure += f" · {database.last_bark_error_type}"
        details.append(failure)
    if database.uncertain_count:
        details.append(f"发送结果待核对：{database.uncertain_count} 条")
    return " · ".join(details) if details else "状态来自本地运行证据；Bark 接受不代表手机已送达。"


def _event_line(record: dict[str, Any]) -> Text:
    at = _epoch_label(record.get("at"))
    event = record.get("event", "event")
    labels = {
        "process_started": "后台已启动",
        "process_exit": "后台进程退出",
        "unhandled_exception": "未处理异常",
        "state_write_failed": "状态写入失败",
        "state_publish_failed": "状态发布失败",
        "watch_runtime_failed": "监控运行失败",
        "ledger_cleanup_failed": "请求账本关闭失败",
    }
    text = Text(f"{at}  {labels.get(event, '运行事件') if type(event) is str else '运行事件'}")
    error = _safe_error_type(record.get("error_type"))
    if error:
        text.append(f" · {error}", style="yellow")
    if type(record.get("exit_code")) is int:
        text.append(f" · 退出码 {record['exit_code']}", style="yellow")
    return text


class StopConfirmation(ModalScreen[bool]):
    BINDINGS = [Binding("escape", "cancel", "取消")]
    CSS = """
    """

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm-box"):
            yield Static("停止后台监控？", id="confirm-title")
            yield Static("这会请求现有 supervisor 在当前有界请求结束后退出。停止请求发送后，本界面会等待确认。", id="confirm-copy")
            with Horizontal(id="confirm-actions"):
                yield Button("取消", id="cancel", variant="default")
                yield Button("确认停止监控并退出", id="confirm", variant="error")

    @on(Button.Pressed)
    def handle_button(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "confirm")

    def action_cancel(self) -> None:
        self.dismiss(False)


class WatchTUI(App[None]):
    """Single-column post stream; all watcher control remains behind explicit commands."""

    TITLE = "中南大悬赏监控"
    SUB_TITLE = ""
    COMMANDS = ("/start", "/status", "/posts", "/help", "/stop", "/quit", "/home")
    BINDINGS = [
        Binding("up", "completion_previous", show=False, priority=True),
        Binding("down", "completion_next", show=False, priority=True),
        Binding("tab", "complete_command", show=False, priority=True),
        Binding("ctrl+c", "detach", show=False, priority=True),
    ]
    CSS = """
    Screen { background: #0b1015; color: #d6e0e8; }
    #main { height: 1fr; padding: 1 2; }
    #app-title { height: 1; color: #9fc6df; text-style: bold; }
    #status-line { height: auto; min-height: 1; margin-top: 1; margin-bottom: 1; }
    #content-view { height: 1fr; padding: 0 1; scrollbar-gutter: stable; }
    .post-entry { height: auto; padding: 0 0 1 0; margin-bottom: 1; border-bottom: solid #1d2933; }
    .view-heading { height: auto; margin-bottom: 1; color: #9fc6df; text-style: bold; }
    #command-shell { height: auto; margin-top: 1; }
    #command-hint { height: 1; color: #8494a2; }
    #command-input { height: 3; border: round #405261; background: #111820; padding: 0 1; }
    """

    def __init__(
        self,
        *,
        root: str | Path | None = None,
        controller: Any | None = None,
        poll_seconds: float = 1.5,
        stop_timeout_seconds: float = 25,
        stop_poll_seconds: float = 1,
        database_reader: Callable[[str | Path], DatabaseSnapshot] = read_database_snapshot,
    ):
        self.root = Path(root or Path(__file__).resolve().parents[1]).resolve()
        self.controller = controller or PowerShellController(self.root)
        self.poll_seconds = max(1.0, poll_seconds)
        self.stop_timeout_seconds = max(0.01, stop_timeout_seconds)
        self.stop_poll_seconds = max(0.01, stop_poll_seconds)
        self.database_reader = database_reader
        self.snapshot = DashboardSnapshot(
            {"runtime_state": "not_started", "running": False, "process_alive": False},
            DatabaseSnapshot(),
        )
        self._view_mode = "messages"
        self._stream_initialized = False
        self._rendered_posts: dict[int, tuple[RecentPost, Static]] = {}
        self._posts_signature: tuple[RecentPost, ...] | None = None
        self._view_widget: Static | None = None
        self._view_signature: str | None = None
        self._status_line_signature: str | None = None
        self._completion_candidates: list[str] = []
        self._completion_index = -1
        self._pending_completion_value: str | None = None
        self._control_busy = False
        self._refresh_busy = False
        self._refresh_lock = asyncio.Lock()
        self._last_action_message = ""
        super().__init__()

    def compose(self) -> ComposeResult:
        with Vertical(id="main"):
            yield Static("中南大悬赏监控", id="app-title")
            yield Static(id="status-line")
            yield VerticalScroll(id="content-view")
            with Vertical(id="command-shell"):
                yield Static("命令 /help · ↑↓选择 · Tab 补全 · Enter 执行", id="command-hint")
                yield Input(placeholder="输入命令，例如 /status", id="command-input", select_on_focus=False)

    def on_mount(self) -> None:
        self.query_one("#command-input", Input).focus()
        self.request_refresh()
        self.set_interval(self.poll_seconds, self.request_refresh)

    def request_refresh(self) -> None:
        if self._refresh_busy:
            return
        self._refresh_busy = True
        self.run_worker(self._scheduled_refresh(), group="local-refresh", exclusive=True)

    async def _scheduled_refresh(self) -> None:
        try:
            await self.refresh_dashboard()
        finally:
            self._refresh_busy = False

    async def refresh_dashboard(self) -> None:
        async with self._refresh_lock:
            latest = await asyncio.to_thread(
                collect_dashboard, self.root, self.controller, database_reader=self.database_reader,
            )
        if latest.database.error:
            latest = replace(latest, database=replace(
                latest.database,
                baseline_count=self.snapshot.database.baseline_count,
                distinct_count=self.snapshot.database.distinct_count,
                new_count=self.snapshot.database.new_count,
                bark_accepted_count=self.snapshot.database.bark_accepted_count,
                pending_count=self.snapshot.database.pending_count,
                bark_failure_count=self.snapshot.database.bark_failure_count,
                uncertain_count=self.snapshot.database.uncertain_count,
                last_bark_error_type=self.snapshot.database.last_bark_error_type,
                last_success_at=self.snapshot.database.last_success_at,
                last_pages=self.snapshot.database.last_pages,
                last_posts=self.snapshot.database.last_posts,
                recent_posts=self.snapshot.database.recent_posts,
            ))
        self.snapshot = latest
        self._render_status_line()
        if self._view_mode == "messages":
            await self._sync_message_stream()
        elif self._view_mode == "posts":
            await self._sync_history_view()
        elif self._view_mode == "status":
            self._sync_status_view()

    def _countdown(self) -> str:
        runtime = self.snapshot.runtime
        monitor = runtime.get("monitor") if type(runtime.get("monitor")) is dict else {}
        if monitor.get("state") == "paused":
            return "已暂停"
        if not runtime.get("running"):
            return "—"
        if monitor.get("state") == "scanning":
            return "扫描中"
        next_check_at = monitor.get("next_check_at")
        if _finite_number(next_check_at):
            return f"{max(0, int(next_check_at - time.time()))} 秒"
        return "等待状态更新"

    def _status_reason(self) -> str | None:
        snapshot = self.snapshot
        if snapshot.status_error:
            return "本地状态暂时不可读"
        monitor = snapshot.runtime.get("monitor") if type(snapshot.runtime.get("monitor")) is dict else {}
        label, _ = _runtime_state(snapshot.runtime, snapshot.status_error)
        if label not in {"暂停", "异常退出", "状态暂不可读", "状态读取异常", "状态发布故障"}:
            return None
        reason = monitor.get("reason")
        description = REASON_LABELS.get(reason, "监控异常，需要查看详情") if type(reason) is str else "监控异常，需要查看详情"
        summary = monitor.get("last_summary") if type(monitor.get("last_summary")) is dict else {}
        error_type = _safe_error_type(monitor.get("error_type")) or _safe_error_type(summary.get("error_type"))
        if error_type:
            description += f" · {error_type}"
        return description

    def _render_status_line(self) -> None:
        label, color = _runtime_state(self.snapshot.runtime, self.snapshot.status_error)
        content = Text()
        content.append("● ", style=color)
        content.append(label, style=f"bold {color}")
        content.append(f"   下次检查：{self._countdown()}")
        reason = self._status_reason()
        if reason:
            content.append(f"   ·   {reason}", style="bold #f3c969" if color != "red" else "bold red")
        signature = content.plain
        if signature != self._status_line_signature:
            self.query_one("#status-line", Static).update(content)
            self._status_line_signature = signature

    @staticmethod
    def _ordered_posts(posts: tuple[RecentPost, ...]) -> list[RecentPost]:
        return sorted(posts, key=lambda post: (post.first_seen_at, post.post_id))

    @staticmethod
    def _post_content(post: RecentPost) -> Text:
        content = Text()
        content.append(f"{_epoch_label(post.first_seen_at)}  #{post.post_id}", style="bold #d7e6ef")
        bark_style = "green" if post.bark_status == "Bark 已接受" else "yellow"
        content.append(f"    {post.bark_status}\n", style=bark_style)
        content.append(post.preview, style="#c4ced6")
        return content

    @classmethod
    def _post_widget(cls, post: RecentPost) -> Static:
        content = cls._post_content(post)
        return Static(content, classes="post-entry", markup=False)

    async def _sync_message_stream(self) -> None:
        container = self.query_one("#content-view", VerticalScroll)
        posts = self._ordered_posts(self.snapshot.database.recent_posts)
        if not self._stream_initialized:
            widgets = [self._post_widget(post) for post in posts]
            self._rendered_posts = {post.post_id: (post, widget) for post, widget in zip(posts, widgets)}
            if widgets:
                await container.mount(*widgets)
                container.scroll_end(animate=False)
            self._stream_initialized = True
            return

        appended: list[Static] = []
        changed = False
        for post in posts:
            existing = self._rendered_posts.get(post.post_id)
            if existing is None:
                widget = self._post_widget(post)
                self._rendered_posts[post.post_id] = (post, widget)
                appended.append(widget)
                changed = True
            elif existing[0] != post:
                existing[1].update(self._post_content(post))
                self._rendered_posts[post.post_id] = (post, existing[1])
                changed = True
        if appended:
            await container.mount(*appended)
            container.scroll_end(animate=False)
        if changed:
            await self._trim_stream(container)

    async def _trim_stream(self, container: VerticalScroll, *, limit: int = 100) -> None:
        while len(self._rendered_posts) > limit:
            oldest_id = min(self._rendered_posts, key=lambda post_id: (
                self._rendered_posts[post_id][0].first_seen_at, post_id,
            ))
            _, widget = self._rendered_posts.pop(oldest_id)
            if widget.parent is container:
                await widget.remove()

    async def _sync_history_view(self, *, force: bool = False) -> None:
        signature = self.snapshot.database.recent_posts
        if not force and signature == self._posts_signature:
            return
        self._posts_signature = signature
        widgets: list[Static] = [Static("历史帖子 · ↑↓滚动 · /home 返回消息流", classes="view-heading")]
        posts = self._ordered_posts(signature)
        if posts:
            widgets.extend(self._post_widget(post) for post in posts)
        else:
            widgets.append(Static("暂无已记录的帖子", classes="post-entry"))
        await self._replace_content(widgets)

    async def _replace_content(self, widgets: list[Static]) -> None:
        container = self.query_one("#content-view", VerticalScroll)
        await container.remove_children()
        if widgets:
            await container.mount(*widgets)

    def _status_report(self) -> str:
        snapshot = self.snapshot
        runtime = snapshot.runtime
        monitor = runtime.get("monitor") if type(runtime.get("monitor")) is dict else {}
        label, _ = _runtime_state(runtime, snapshot.status_error)
        stats = snapshot.database
        lines = [
            f"运行状态：{label}",
            f"监控进程：{'运行中' if runtime.get('process_alive') else '未运行'}；后台守护：{'运行中' if runtime.get('guardian_alive') else '未运行'}",
            f"下次检查：{self._countdown()}",
            f"最近成功扫描：{_epoch_label(stats.last_success_at)} · {stats.last_pages} 页 / {stats.last_posts} 条",
            f"freshKey：{_freshness_label(runtime)}",
            f"帖子统计：历史基线 {stats.baseline_count} · 累计不同 {stats.distinct_count} · 累计新增 {stats.new_count}",
            f"通知统计：Bark 已接受 {stats.bark_accepted_count} · 当前待发 {stats.pending_count} · 失败 {stats.bark_failure_count} · 状态待核对 {stats.uncertain_count}",
            "说明：Bark 服务端接受不代表手机实际送达。",
        ]
        diagnostic = _runtime_description(snapshot)
        if diagnostic and diagnostic != "状态来自本地运行证据；Bark 接受不代表手机已送达。":
            lines.append(f"运行诊断：{diagnostic}")
        if stats.error:
            lines.append(f"本地数据：{stats.error}")
        if snapshot.status_error:
            lines.append("后台状态：暂时不可读；以上保留的数据库数字可能是上次成功读取值。")
        if self._last_action_message:
            lines.append(f"最近操作：{self._last_action_message}")
        if snapshot.events:
            lines.append("最近运行故障事件：")
            lines.extend(f"  {_event_line(event).plain}" for event in reversed(snapshot.events[-8:]))
        return "\n".join(lines)

    def _sync_status_view(self) -> None:
        if self._view_widget is None:
            return
        report = self._status_report()
        if report != self._view_signature:
            self._view_widget.update(report)
            self._view_signature = report

    async def _show_view(self, mode: str) -> None:
        self._view_mode = mode
        self._view_widget = None
        self._view_signature = None
        self._stream_initialized = False
        self._rendered_posts.clear()
        self._posts_signature = None
        await self._replace_content([])
        if mode == "messages":
            await self._sync_message_stream()
        elif mode == "posts":
            await self._sync_history_view(force=True)
        elif mode == "status":
            report = self._status_report()
            self._view_widget = Static(report, id="status-report", markup=False)
            self._view_signature = report
            await self._replace_content([self._view_widget])
        elif mode == "help":
            help_text = (
                "可用命令\n\n"
                "/start   启动或连接后台监控\n"
                "/status  查看完整状态、统计和运行诊断\n"
                "/posts   查看历史帖子并滚动浏览\n"
                "/home    返回实时帖子消息流\n"
                "/help    显示本帮助\n"
                "/stop    二次确认后停止后台监控\n"
                "/quit    只关闭界面，后台继续运行\n\n"
                "↑ / ↓ 选择命令补全 · Tab 补全 · Enter 执行 · Ctrl+C 关闭界面"
            )
            self._view_widget = Static(help_text, id="help-view", markup=False)
            await self._replace_content([self._view_widget])

    @on(Input.Changed, "#command-input")
    def handle_command_changed(self, event: Input.Changed) -> None:
        value = event.value.strip()
        if self._pending_completion_value == value:
            self._pending_completion_value = None
            return
        self._pending_completion_value = None
        self._completion_index = -1
        self._completion_candidates = [
            command for command in self.COMMANDS
            if value and command.startswith(value) and command != value
        ]
        hint = self.query_one("#command-hint", Static)
        if self._completion_candidates:
            hint.update(f"{'  '.join(self._completion_candidates)}   ·   ↑↓选择 · Tab 补全 · Enter 执行")
        else:
            hint.update("命令 /help · ↑↓选择 · Tab 补全 · Enter 执行")

    def _select_completion(self, direction: int) -> None:
        if not self._completion_candidates:
            return
        if self._completion_index < 0:
            self._completion_index = 0 if direction > 0 else len(self._completion_candidates) - 1
        else:
            self._completion_index = (self._completion_index + direction) % len(self._completion_candidates)
        value = self._completion_candidates[self._completion_index]
        self._pending_completion_value = value
        command_input = self.query_one("#command-input", Input)
        command_input.value = value
        command_input.cursor_position = len(value)

    def action_completion_previous(self) -> None:
        if self._completion_candidates:
            self._select_completion(-1)
        else:
            self.query_one("#content-view", VerticalScroll).scroll_relative(y=-3, animate=False, immediate=True)

    def action_completion_next(self) -> None:
        if self._completion_candidates:
            self._select_completion(1)
        else:
            self.query_one("#content-view", VerticalScroll).scroll_relative(y=3, animate=False, immediate=True)

    def action_complete_command(self) -> None:
        if self._completion_candidates:
            index = self._completion_index if self._completion_index >= 0 else 0
            value = self._completion_candidates[index]
            self._pending_completion_value = value
            command_input = self.query_one("#command-input", Input)
            command_input.value = value
            command_input.cursor_position = len(value)

    @on(Input.Submitted, "#command-input")
    async def handle_command_submitted(self, event: Input.Submitted) -> None:
        command = event.value.strip().lower()
        self.query_one("#command-input", Input).value = ""
        self._completion_candidates = []
        self._completion_index = -1
        if command == "/start":
            await self.action_start()
        elif command == "/status":
            await self._show_view("status")
        elif command == "/posts":
            await self._show_view("posts")
        elif command in {"/home", "/messages"}:
            await self._show_view("messages")
        elif command == "/help":
            await self._show_view("help")
        elif command == "/stop":
            self.action_confirm_stop()
        elif command == "/quit":
            self.action_detach()
        elif command:
            self._last_action_message = f"未知命令：{command}（输入 /help 查看可用命令）"
            await self._show_view("status")
        if self.is_running:
            self.query_one("#command-input", Input).focus()

    async def action_start(self) -> None:
        if self._control_busy:
            return
        self._control_busy = True
        self._last_action_message = "正在调用现有启动流程……"
        try:
            # The existing script can securely request missing local Bark configuration.
            # Suspending Textual gives Read-Host its normal hidden-input console UI.
            if isinstance(self.controller, PowerShellController):
                with self.suspend():
                    result = self.controller.start()
            else:
                result = self.controller.start()
            if result.status == "already_running":
                self._last_action_message = "后台已在运行，已连接到现有实例。"
            elif result.status == "launching":
                self._last_action_message = "启动流程已提交；等待后台确认运行状态。"
            else:
                self._last_action_message = "启动未成功" + (f" · {result.error_type}" if result.error_type else "")
        except Exception as exc:
            self._last_action_message = f"启动未成功 · {type(exc).__name__}"
        finally:
            self._control_busy = False
            await self.refresh_dashboard()
            await self._show_view("status")

    def action_confirm_stop(self) -> None:
        if not self._control_busy:
            self.push_screen(StopConfirmation(), self._handle_stop_confirmation)

    def _handle_stop_confirmation(self, confirmed: bool | None) -> None:
        if confirmed is True:
            self.run_worker(self._stop_and_exit(), group="control", exclusive=True)
        elif self.is_running:
            self.query_one("#command-input", Input).focus()

    async def _stop_and_exit(self) -> None:
        if self._control_busy:
            return
        self._control_busy = True
        self._last_action_message = "正在发送经确认的停止请求……"
        try:
            result = await asyncio.to_thread(self.controller.stop)
            if not result.ok:
                self._last_action_message = "停止请求未成功" + (f" · {result.error_type}" if result.error_type else "")
                await self.refresh_dashboard()
                await self._show_view("status")
                return
            deadline = time.monotonic() + self.stop_timeout_seconds
            while time.monotonic() < deadline:
                await asyncio.sleep(min(self.stop_poll_seconds, max(0, deadline - time.monotonic())))
                await self.refresh_dashboard()
                runtime = self.snapshot.runtime
                if not runtime.get("process_alive") and not runtime.get("guardian_alive"):
                    self._last_action_message = "已确认后台进程退出；界面关闭。"
                    self.exit()
                    return
            self._last_action_message = "停止请求已发送，但尚未确认后台退出。"
            await self.refresh_dashboard()
            await self._show_view("status")
        except Exception as exc:
            self._last_action_message = f"停止未确认 · {type(exc).__name__}"
            await self.refresh_dashboard()
            await self._show_view("status")
        finally:
            self._control_busy = False

    def action_detach(self) -> None:
        # Closing the TUI never writes a watcher stop request.
        self.exit()

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="中南大悬赏监控本地终端面板")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1], help=argparse.SUPPRESS)
    parser.add_argument("--poll-seconds", type=float, default=1.5, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        WatchTUI(root=args.root, poll_seconds=args.poll_seconds).run()
        return 0
    except Exception as exc:
        print(f"TUI 无法启动：{type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
