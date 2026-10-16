"""Sequential, budgeted local runner. Credentials and bodies never enter logs."""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone, timedelta
import json
import os
from pathlib import Path
import signal
import sqlite3
import threading
import time
from typing import Callable

from .adapters import fresh_key_request_params
from .http import get_json
from .monitor import Monitor
from .models import SourceProtocolError
from .storage import SQLiteStore, StoreBusy
from .runtime_evidence import record_runtime_event

BEIJING = timezone(timedelta(hours=8))
FRESH_ENDPOINT = "https://wm.dolacc.com/user/getHomeFreshkey"


def numeric_fresh_key(value):
    """Only the observed non-sensitive numeric form is allowed in diagnostics."""
    if (type(value) is str and value.isascii() and value.isdecimal()
            and 0 < len(value) <= 19 and value[0] != "0"):
        number = int(value)
        return number if 0 < number < 2**63 else None
    return None


class WatchStopped(SourceProtocolError):
    pass


class StateWriteError(RuntimeError):
    """Public state publishing failure with no private path or exception text."""

    def __init__(self, stage, attempts, failure):
        self.stage, self.attempts = stage, attempts
        self.error_type, self.errno, self.winerror = failure
        super().__init__("watch_state_publish_failed")


def atomic_write_state(path: Path, value: dict) -> int:
    """Keep the previous JSON intact; tolerate only brief Windows replace locks."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    failure = None
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    except OSError as exc:
        failure = (type(exc).__name__, exc.errno, getattr(exc, "winerror", None))
    if failure is not None:
        raise StateWriteError("temporary_write", 1, failure)
    for attempt in range(1, 11):
        try:
            temporary.replace(path)
            return attempt
        except OSError as exc:
            failure = (type(exc).__name__, exc.errno, getattr(exc, "winerror", None))
            retry = os.name == "nt" and failure[2] in (5, 32, 33) and attempt < 10
        if not retry:
            break
        # Real bounded sleep, independent of injected business clocks; never
        # repeat HTTP or delivery and never modify the SQLite transaction.
        time.sleep(0.05)
    raise StateWriteError("replace", attempt, failure)


class RequestBudgetExceeded(SourceProtocolError):
    pass


class FreshParameterError(SourceProtocolError):
    def __init__(self, shape):
        super().__init__("fresh_parameter_contract_unverified")
        self.shape = shape


class RequestLedger:
    """Persist attempts before I/O; restarting does not reset daily usage."""

    def __init__(self, path: Path, daily_get_limit: int, clock=time.time):
        if type(daily_get_limit) is not int or not 1 <= daily_get_limit <= 4320:
            raise ValueError("daily GET budget must be 1..4320")
        self.clock, self.limit = clock, daily_get_limit
        self.con = sqlite3.connect(path)
        tables = {r[0] for r in self.con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables - {"attempts"}:
            self.con.close()
            raise ValueError("ledger path belongs to another database")
        self.con.execute("CREATE TABLE IF NOT EXISTS attempts(id INTEGER PRIMARY KEY,at INTEGER NOT NULL,day TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('list_get','fresh_get','bark_post')))")
        self.con.commit()

    def day(self):
        return datetime.fromtimestamp(self.clock(), BEIJING).date().isoformat()

    def counts(self):
        values = dict(self.con.execute("SELECT kind,COUNT(*) FROM attempts WHERE day=? GROUP BY kind", (self.day(),)))
        return {name: values.get(name, 0) for name in ("list_get", "fresh_get", "bark_post")}

    def remaining(self):
        values = self.counts()
        return self.limit - values["list_get"] - values["fresh_get"]

    def reserve(self, kind):
        if kind not in ("list_get", "fresh_get", "bark_post"):
            raise ValueError("invalid request kind")
        stamp = int(self.clock())
        day = datetime.fromtimestamp(stamp, BEIJING).date().isoformat()
        with self.con:
            self.con.execute("BEGIN IMMEDIATE")
            used = self.con.execute("SELECT COUNT(*) FROM attempts WHERE day=? AND kind IN ('list_get','fresh_get')", (day,)).fetchone()[0]
            if kind != "bark_post" and used >= self.limit:
                raise RequestBudgetExceeded("daily_get_budget_exhausted")
            self.con.execute("INSERT INTO attempts(at,day,kind) VALUES(?,?,?)", (stamp, day, kind))

    def close(self):
        self.con.close()

    def reserve_fresh(self, *, daily_fresh_limit, list_reserve, external_gets=0, total_limit=None):
        """Atomically count fresh attempts without spending reserved list pages."""
        total_limit = self.limit if total_limit is None else min(self.limit, total_limit)
        if (type(daily_fresh_limit) is not int or not 1 <= daily_fresh_limit <= 1440
                or any(type(v) is not int or v < 0 for v in (list_reserve, external_gets))):
            raise ValueError("invalid fresh reservation bounds")
        stamp = int(self.clock())
        day = datetime.fromtimestamp(stamp, BEIJING).date().isoformat()
        with self.con:
            self.con.execute("BEGIN IMMEDIATE")
            counts = dict(self.con.execute("SELECT kind,COUNT(*) FROM attempts WHERE day=? GROUP BY kind", (day,)))
            used = counts.get("list_get", 0) + counts.get("fresh_get", 0)
            if (counts.get("fresh_get", 0) >= daily_fresh_limit
                    or used + external_gets + list_reserve + 1 > total_limit):
                raise RequestBudgetExceeded("refresh_would_consume_reserved_pagination")
            self.con.execute("INSERT INTO attempts(at,day,kind) VALUES(?,?,'fresh_get')", (stamp, day))


def fetch_current_fresh_key(ledger: RequestLedger, *, timeout=10, stopped=lambda: False):
    """One explicit initial parameter request; never refresh after failure."""
    if stopped():
        raise WatchStopped("watch_stop_requested")
    ledger.reserve("fresh_get")
    if stopped():
        raise WatchStopped("watch_stop_requested")
    result = get_json(FRESH_ENDPOINT, params=fresh_key_request_params(), timeout_seconds=timeout)
    return parse_fresh_key(result)


def parse_fresh_key(result):
    """Observed parameter response: code=1/data.freshKey positive integer.

    Canonical decimal normalization changes no query bytes. Values remain
    transient; this is not a stored or derived long-term credential.
    """
    shape = {"top_fields": sorted(key for key in result if key in ("code", "data", "freshKey", "freshkey", "status", "error_code", "errorCode")),
             "data_type": type(result.get("data")).__name__}
    if isinstance(result.get("data"), dict):
        shape["data_fields"] = sorted(key for key in result["data"] if key in ("freshKey", "freshkey", "key", "token", "code"))
    if type(result.get("code")) is int and -1 <= result["code"] <= 1000:
        shape["business_code"] = result["code"]
    if type(result.get("code")) is not int or result["code"] != 1:
        raise FreshParameterError(shape)
    # Only exact parameter response paths, never list-post freshkey fields.
    candidates = []
    for mapping in (result, result.get("data")):
        if isinstance(mapping, dict) and "freshKey" in mapping:
            value = mapping["freshKey"]
            if type(value) is int and 0 < value <= (1 << 63)-1:
                value = str(value)
            candidates.append(value)
    if not candidates or any(type(value) is not str or not value or len(value) > 4096
                             or any(ord(c) <= 32 or ord(c) > 126 for c in value) for value in candidates):
        raise FreshParameterError(shape)
    if len(set(candidates)) != 1:
        raise SourceProtocolError("fresh_parameter_fields_conflict")
    return candidates[0]


class BudgetAdapter:
    def __init__(self, adapter, ledger, stopped: Callable[[], bool], page_limit=3):
        self.adapter, self.ledger, self.stopped = adapter, ledger, stopped
        self.source_scope = adapter.source_scope
        self.page_limit, self.requests = page_limit, 0
        self.last_response_metadata = {}
        self.diagnostic_fresh_key = getattr(adapter, "diagnostic_fresh_key", None)

    def fetch_page(self, cursor=-1):
        self.last_response_metadata = {}
        if self.stopped():
            raise WatchStopped("watch_stop_requested")
        if self.requests >= self.page_limit:
            raise RequestBudgetExceeded("round_get_budget_exhausted")
        self.ledger.reserve("list_get")
        self.requests += 1
        if self.stopped():
            raise WatchStopped("watch_stop_requested")
        try:
            page = self.adapter.fetch_page(cursor)
        finally:
            self.last_response_metadata = getattr(self.adapter, "last_response_metadata", {})
        for post in page.posts:
            if post.top == 0 and (post.source_category != 14 or post.category_evidence != "response_field"):
                raise SourceProtocolError("watch_category_contract_failed")
            if post.top == 0 and post.body is not None and post.body.status in ("invalid", "unknown_format", "unverified"):
                raise SourceProtocolError("watch_body_contract_failed")
        return page


class BudgetNotifier:
    def __init__(self, notifier, ledger, stopped):
        self.notifier, self.ledger, self.stopped = notifier, ledger, stopped

    def send(self, post):
        if self.stopped():
            raise WatchStopped("watch_stop_requested")
        self.ledger.reserve("bark_post")
        if self.stopped():
            raise WatchStopped("watch_stop_requested")
        self.notifier.send(post)


class WatchMonitor(Monitor):
    def __init__(self, *args, stopped, **kwargs):
        super().__init__(*args, **kwargs)
        self.stopped = stopped
        self.freshness_report = None

    def _annotate_page_observations(self, result):
        super()._annotate_page_observations(result)
        if self.freshness_report is not None:
            result.freshness = self.freshness_report(result)

    def _deliver_pending(self, result):
        if self.stopped():
            result.warnings.append("watch_stop_pending_preserved")
            return
        super()._deliver_pending(result)


class WatchRunner:
    def __init__(self, store: SQLiteStore, adapter_factory, notifier, *, state_path: Path,
                 stop_path: Path, ledger_path: Path, interval=60, daily_get_limit=4320,
                 baseline_pages=2, coverage_pages=2, max_pages=3, max_alerts=20,
                 fresh_key=None, fetch_fresh=False, timeout=10, clock=time.time,
                 monotonic=time.monotonic, sleeper=None, deep_candidate_ttl_seconds=900,
                 refresh_policy=None):
        from .freshness import RefreshPolicy
        self.refresh_policy = refresh_policy or RefreshPolicy()
        if (not isinstance(self.refresh_policy, RefreshPolicy)
                or self.refresh_policy.mode not in ("process", "fixed")
                or (self.refresh_policy.mode == "fixed" and (
                    self.refresh_policy.min_interval_seconds < 120
                    or not 1 <= self.refresh_policy.daily_fresh_limit <= 720
                    or self.refresh_policy.daily_get_limit != daily_get_limit
                    or self.refresh_policy.poll_seconds != interval
                    or self.refresh_policy.max_list_pages != max_pages))):
            raise ValueError("unsupported production refresh configuration")
        if store.discovery_policy != "visibility" or store.target_category != 14:
            raise ValueError("watch only supports isolated reward visibility state")
        if type(interval) is not int or interval < 60:
            raise ValueError("watch interval must be at least 60 seconds")
        if not 1 <= baseline_pages <= max_pages <= 3 or not 1 <= coverage_pages <= max_pages:
            raise ValueError("watch requires a bounded 1..3 page window")
        if type(deep_candidate_ttl_seconds) is not int or not 0 <= deep_candidate_ttl_seconds <= 3600:
            raise ValueError("deep candidate lifetime must be 0..3600 seconds")
        self.deep_candidate_ttl_seconds = deep_candidate_ttl_seconds
        self.store, self.adapter_factory, self.notifier = store, adapter_factory, notifier
        self.state_path, self.stop_path, self.ledger_path = map(Path, (state_path, stop_path, ledger_path))
        self.failure_path = self.state_path.with_suffix(".failures.jsonl")
        database = Path(store.database_path).resolve()
        if self.state_path.suffix != ".json" or self.stop_path.suffix != ".json" or self.ledger_path.suffix != ".sqlite3":
            raise ValueError("watch control paths have invalid file types")
        if any(path.resolve().parent != database.parent for path in (self.state_path, self.stop_path, self.ledger_path)):
            raise ValueError("watch control paths must be database siblings")
        protected = [database, Path(str(database) + ".run.lock"), Path(str(database) + "-wal"),
                     Path(str(database) + "-shm"), Path(str(database) + "-journal"),
                     database.parent / "body_cipher.local.json"]
        controls = [self.state_path, self.state_path.with_suffix(self.state_path.suffix + ".tmp"), self.failure_path,
                    self.state_path.parent / "reward_watch.runtime.jsonl", self.stop_path,
                    self.ledger_path, Path(str(self.ledger_path) + "-wal"), Path(str(self.ledger_path) + "-shm")]
        normalized = [os.path.normcase(str(path.resolve())) for path in (*protected, *controls)]
        if len(normalized) != len(set(normalized)):
            raise ValueError("watch paths collide with protected monitoring files")
        if self.state_path.exists():
            state = json.loads(self.state_path.read_text(encoding="utf-8-sig"))
            if not isinstance(state, dict) or not {"state", "pid", "started_at"} <= set(state):
                raise ValueError("state path belongs to another JSON document")
        self.interval, self.daily_get_limit = interval, daily_get_limit
        self.baseline_pages, self.coverage_pages, self.max_pages, self.max_alerts = baseline_pages, coverage_pages, max_pages, max_alerts
        self.fresh_key, self.fetch_fresh, self.timeout = fresh_key, fetch_fresh, timeout
        self.clock, self.monotonic = clock, monotonic
        self.stop_event = threading.Event()
        self.sleeper = sleeper or self.stop_event.wait
        self.started_at = self.clock()
        self.ledger = None
        self.rounds = 0
        self.last_summary = None
        self.last_state_write_attempts = 0
        self.last_requested_state = "starting"
        self.key_manager = None
        self.last_fresh_success_at = None
        self.freshness_state = {"mode": self.refresh_policy.mode, "refreshed": False,
                                "reason": "not_started", "current_fresh_key": None}

    def stopped(self):
        if self.stop_event.is_set():
            return True
        try:
            if not self.stop_path.exists():
                return False
            marker = json.loads(self.stop_path.read_text(encoding="utf-8-sig"))
            if isinstance(marker, dict) and {"pid", "started_at", "requested_at"} <= marker.keys():
                return (type(marker["pid"]) is int and marker["pid"] == os.getpid()
                        and type(marker["started_at"]) in (int, float)
                        and 0 <= self.started_at - marker["started_at"] < 10
                        and type(marker["requested_at"]) in (int, float)
                        and marker["requested_at"] >= marker["started_at"])
        except (OSError, ValueError, UnicodeError):
            pass
        # Preserve manual/legacy standalone controls; the external guardian
        # requires scoped evidence before suppressing an exit alarm.
        try:
            return self.stop_path.exists() and self.stop_path.stat().st_mtime >= self.started_at
        except OSError:
            return False

    def write_state(self, state, reason, **extra):
        self.last_requested_state = state
        value = {"state": state, "reason": reason, "pid": os.getpid(), "started_at": self.started_at,
                 "updated_at": int(self.clock()), "interval_seconds": self.interval,
                 "coverage_pages": self.coverage_pages, "max_pages": self.max_pages,
                 "notification_policy": "notify-window", "body_policy": "require",
                 "rounds": self.rounds, "daily_get_limit": self.daily_get_limit,
                 "requests_today": self.ledger.counts() if self.ledger else {},
                 "freshness": self.current_freshness(),
                 "last_summary": self.last_summary, **extra}
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.last_state_write_attempts = atomic_write_state(self.state_path, value)

    def current_freshness(self):
        result = dict(self.freshness_state)
        if self.key_manager is not None and self.key_manager.last_decision is not None:
            result.update(self.key_manager.last_decision)
            result["refreshed"] = self.key_manager.last_refreshed
            result["key_changed"] = self.key_manager.last_key_changed
        result["current_fresh_key"] = numeric_fresh_key(self.fresh_key)
        result["seconds_since_last_successful_acquisition"] = (
            max(0, round(self.clock() - self.last_fresh_success_at, 3))
            if self.last_fresh_success_at is not None else None)
        if self.ledger is not None:
            counts = self.ledger.counts()
            result.update(fresh_get_today=counts["fresh_get"], list_get_today=counts["list_get"],
                          remaining_gets=self.ledger.remaining())
        return result

    def round_freshness(self, result):
        if result.page_observations:
            headers = result.page_observations[0].get("response", {}).get("headers", {})
            self.freshness_state["home_response"] = headers
        return self.current_freshness()

    def parameter_payload(self):
        """Caller must already reserve its one GET in the shared ledger."""
        if self.stopped():
            raise WatchStopped("watch_stop_requested")
        payload = get_json(FRESH_ENDPOINT, params=fresh_key_request_params(), timeout_seconds=self.timeout)
        if self.stopped():
            raise WatchStopped("watch_stop_requested")
        # Fixed-mode diagnostics only accept the observed numeric parameter.
        if numeric_fresh_key(parse_fresh_key(payload)) is None:
            raise SourceProtocolError("numeric_refresh_parameter_required")
        return payload

    def record_failure(self, event, exc):
        facts = {"error_type": type(exc).__name__, "requested_state": self.last_requested_state}
        if isinstance(exc, StateWriteError):
            facts.update(stage=exc.stage, attempts=exc.attempts, error_type=exc.error_type,
                         errno=exc.errno, winerror=exc.winerror)
        try:
            return record_runtime_event(self.failure_path, event, started_at=self.started_at, **facts)
        except Exception:
            return False

    def publish_terminal_safely(self, state, reason, **extra):
        try:
            self.write_state(state, reason, **extra)
        except Exception as exc:
            self.record_failure("state_publish_failed", exc)

    def attempted_pending(self):
        return self.store._connection.execute(
            "SELECT COUNT(*) FROM posts WHERE status='pending' AND (attempts>0 OR send_started_at IS NOT NULL)"
        ).fetchone()[0]

    @contextmanager
    def signals(self):
        previous = {}
        if threading.current_thread() is threading.main_thread():
            for number in (signal.SIGINT, signal.SIGTERM):
                previous[number] = signal.signal(number, lambda *_: self.stop_event.set())
        try:
            yield
        finally:
            for number, handler in previous.items():
                signal.signal(number, handler)

    def run(self, *, max_cycles=None):
        """max_cycles exists for bounded offline validation, not catch-up loops."""
        owns_lock = False
        try:
            with self.store.run_lock(), self.signals():
                owns_lock = True
                self.ledger = RequestLedger(self.ledger_path, self.daily_get_limit, self.clock)
                if self.attempted_pending():
                    self.write_state("paused", "attempted_pending_requires_review")
                    return "paused"
                if self.store.baseline_complete and self.store.visibility_snapshot is None:
                    self.write_state("paused", "visibility_snapshot_requires_rebaseline")
                    return "paused"
                if self.stopped():
                    self.write_state("stopped", "stop_requested")
                    return "stopped"
                if not self.fresh_key:
                    if not self.fetch_fresh:
                        self.write_state("paused", "fresh_parameter_missing")
                        return "paused"
                    if self.ledger.remaining() < self.max_pages + 1:
                        self.write_state("paused", "insufficient_daily_round_budget")
                        return "paused"
                    self.write_state("starting", "fetching_initial_parameter")
                    initial_at = self.clock()
                    try:
                        if self.refresh_policy.mode == "fixed":
                            self.ledger.reserve_fresh(daily_fresh_limit=self.refresh_policy.daily_fresh_limit,
                                                      list_reserve=self.max_pages)
                            self.fresh_key = parse_fresh_key(self.parameter_payload())
                        else:
                            self.fresh_key = fetch_current_fresh_key(self.ledger, timeout=self.timeout, stopped=self.stopped)
                    except Exception:
                        self.freshness_state["reason"] = "initial_parameter_failed_stop"
                        raise
                    self.last_fresh_success_at = initial_at
                    self.freshness_state.update(reason="initial_parameter_acquired", initial_acquired=True)
                else:
                    initial_at = None
                if self.refresh_policy.mode == "fixed":
                    from .freshness import RoundKeys
                    if numeric_fresh_key(self.fresh_key) is None:
                        raise SourceProtocolError("numeric_refresh_parameter_required")
                    self.key_manager = RoundKeys(self.fresh_key, self.refresh_policy, self.ledger,
                                                 self.parameter_payload, clock=self.clock)
                    if initial_at is not None:
                        self.key_manager.note_acquisition(initial_at)
                adapter = BudgetAdapter(self.adapter_factory(self.fresh_key), self.ledger, self.stopped, self.max_pages)
                adapter.diagnostic_fresh_key = numeric_fresh_key(self.fresh_key)
                sender = BudgetNotifier(self.notifier, self.ledger, self.stopped)
                monitor = WatchMonitor(adapter, self.store, sender, stopped=self.stopped,
                    baseline_pages=self.baseline_pages, coverage_pages=self.coverage_pages,
                    max_pages_per_poll=self.max_pages, lookback_pages=0,
                    missing_content_policy="require", uncertain_policy="notify-window",
                    max_alerts=self.max_alerts, clock=self.clock,
                    deep_candidate_ttl_seconds=self.deep_candidate_ttl_seconds)
                monitor.freshness_report = self.round_freshness
                while not self.stopped():
                    if self.attempted_pending():
                        self.write_state("paused", "attempted_pending_requires_review")
                        return "paused"
                    if self.ledger.remaining() < self.max_pages:
                        self.write_state("paused", "insufficient_daily_round_budget")
                        return "paused"
                    started = self.monotonic()
                    self.write_state("scanning", "bounded_round_started")
                    command = "check" if self.store.baseline_complete else "init"
                    lease = nullcontext(self.fresh_key)
                    if self.key_manager is not None:
                        from .freshness import BudgetView, remaining_rounds_after_current
                        counts = self.ledger.counts()
                        budget = BudgetView(counts["fresh_get"] + counts["list_get"], counts["fresh_get"],
                                            remaining_rounds_after_current(self.clock(), self.interval), self.max_pages)
                        lease = self.key_manager.round(budget, self.freshness_state.get("home_response"))
                    with lease as round_key:
                        if self.key_manager is not None and self.key_manager.last_refreshed:
                            self.last_fresh_success_at = self.key_manager._last_attempt_precise
                        elif self.key_manager is None:
                            self.freshness_state["reason"] = "process_mode"
                        if round_key != self.fresh_key:
                            adapter = BudgetAdapter(self.adapter_factory(round_key), self.ledger, self.stopped, self.max_pages)
                            self.fresh_key = round_key
                            adapter.diagnostic_fresh_key = numeric_fresh_key(round_key)
                            monitor.adapter = adapter
                        adapter.requests = 0
                        result = monitor.run_once(command, _lock_held=True)
                    self.last_summary = result.to_dict()
                    self.rounds += 1
                    self.freshness_state["initial_acquired"] = False
                    if self.stopped():
                        self.write_state("stopped", "stop_requested")
                        return "stopped"
                    if result.status not in ("baseline", "ok", "partial") or result.retry_failures:
                        self.write_state("paused", "round_failed_requires_review")
                        return "paused"
                    # A successful gap recovery already records unknown backlog
                    # silently and advances the window. Continue from it rather
                    # than requiring a second manual launch. Incomplete scans
                    # and uncertain delivery attempts still pause above.
                    if max_cycles is not None and self.rounds >= max_cycles:
                        self.write_state("stopped", "offline_cycle_limit")
                        return "stopped"
                    finished = self.monotonic()
                    due = started + self.interval if finished < started + self.interval else finished + self.interval
                    self.write_state("waiting", "next_round_scheduled", next_check_at=int(self.clock() + max(0, due-finished)))
                    while not self.stopped() and self.monotonic() < due:
                        self.sleeper(min(1, due - self.monotonic()))
                self.write_state("stopped", "stop_requested")
                return "stopped"
        except StoreBusy:
            # A second runner must not overwrite the first runner's status.
            return "busy"
        except WatchStopped:
            if owns_lock:
                self.publish_terminal_safely("stopped", "stop_requested")
            return "stopped"
        except StateWriteError as exc:
            if owns_lock:
                self.record_failure("state_write_failed", exc)
            return "paused"
        except Exception as exc:
            if owns_lock:
                self.record_failure("watch_runtime_failed", exc)
                extra = {"error_type": type(exc).__name__}
                if isinstance(exc, FreshParameterError):
                    extra["fresh_response_shape"] = exc.shape
                self.publish_terminal_safely("paused", "startup_or_runtime_failed", **extra)
            return "paused"
        finally:
            if self.ledger:
                try:
                    self.ledger.close()
                except Exception as exc:
                    self.record_failure("ledger_cleanup_failed", exc)
