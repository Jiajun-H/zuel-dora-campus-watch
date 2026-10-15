"""Deterministic fixture -> SQLite -> Bark rehearsal; no forum access exists here."""

from __future__ import annotations

import time
import sqlite3
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from .adapters import FixtureForumAdapter
from .monitor import Monitor
from .notifier import BarkNotifier
from .storage import SQLiteStore, StoreError
from . import notifier as notification_transport

SIMULATED_POST_ID = 900000001


def run_fixture_bark_demo(database: str | Path, *, send_bark: bool = False,
                          timeout_seconds: float = 10) -> dict:
    """One synthetic push at most; rerunning the same DB does not resend sent."""
    # Reject a real or unrelated DB before its constructor can migrate schema
    # or recover run history. This preflight is strictly read-only.
    path = Path(database) if str(database) != ":memory:" else None
    if path is not None and path.exists():
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as existing:
            tables = {row[0] for row in existing.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not {"posts", "app_state"}.issubset(tables):
                raise StoreError("simulation requires an empty or existing simulation database")
            marker = existing.execute("SELECT value FROM app_state WHERE key='source_kind'").fetchone()
            kind = marker[0] if marker else None
            baseline_row = existing.execute("SELECT value FROM app_state WHERE key='baseline_at'").fetchone()
            occupied = existing.execute("SELECT 1 FROM posts LIMIT 1").fetchone()
            if kind != "fixture_bark_demo" and (kind is not None or baseline_row or occupied):
                raise StoreError("simulation refuses a non-simulation database")
    with SQLiteStore(database) as store:
        with store.run_lock(recover_runs=False):
            kind = store._state("source_kind")
            if kind is None:
                if store.known_ids() or store.baseline_complete:
                    raise StoreError("simulation requires a dedicated empty database")
                with store._connection:
                    store._set_state("source_kind", "fixture_bark_demo")
                    store._set_state("simulation_seed", str(int(time.time()) - 2))
            elif kind != "fixture_bark_demo":
                raise StoreError("database is not a Bark simulation database")
            seed = int(store._state("simulation_seed"))

        def page(new=False):
            values = []
            if new:
                values.append({
                    "id": SIMULATED_POST_ID, "createtime": seed + 1, "top": 0,
                    "summary": "【离线模拟】这是一条合成校园帖子，用于验证发现、持久化和手机通知；不是实际校圈内容。",
                })
            values.extend({"id": number, "createtime": seed - 1000, "top": 0} for number in (100, 99))
            return {"code": 1, "data": {"posts": values}}

        source = FixtureForumAdapter({"-1": page()}, now=seed + 1)
        baseline = Monitor(source, store, clock=lambda: seed,
                           confirmed_notifications=True).run_once("init")
        source = FixtureForumAdapter({"-1": page(True), "99": {"code": 1, "data": {"posts": []}}}, now=seed + 1)

        if send_bark:
            sender = replace(BarkNotifier.from_env(timeout_seconds=timeout_seconds), simulation=True)
        else:
            # An unmistakably synthetic key is used only behind a mocked HTTP
            # call. No actual credential is embedded in this demo or its logs.
            sender = BarkNotifier("SYNTHETIC_DEVICE_KEY", require_summary=True, simulation=True)

        monitor = Monitor(source, store, sender, max_alerts=1, clock=lambda: seed + 1,
                          confirmed_notifications=True)
        attempts = 0
        transport = notification_transport.get_json

        def counted_bark(url, **kwargs):
            nonlocal attempts
            attempts += 1
            return transport(url, **kwargs) if send_bark else {"code": 200}

        with patch("campus_watch.notifier.get_json", side_effect=counted_bark):
            checked = monitor.run_once()
        # Check duplicate detection without attempting a second push, even if
        # the first HTTP response was lost; pending stays for an explicit retry.
        repeated = Monitor(source, store, clock=lambda: seed + 1,
                           confirmed_notifications=True).run_once()
        return {
            "mode": "synthetic_posts_real_bark" if send_bark else "offline_bark_mock",
            "forum_requests": 0,
            "bark_http_attempts": attempts,
            "expected_notification_title": "【模拟】中南大校圈集成验收",
            "baseline": baseline.to_dict(), "check": checked.to_dict(),
            "repeat": repeated.to_dict(), "database": store.status(),
            "iphone_receipt": "requires_user_confirmation" if send_bark else "simulated_only",
        }
