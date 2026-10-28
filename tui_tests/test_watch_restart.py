"""Portable offline WatchRunner regressions; no imports from ignored tests/."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from campus_watch.adapters import FixtureForumAdapter, SourceScope, verified_reward_category
from campus_watch.body import BodySpec
from campus_watch.monitor import Monitor
from campus_watch.storage import SQLiteStore
from campus_watch.watch import WatchRunner


NOW = 1_791_804_000


def pages(*new_ids):
    # Deliberately use IDs unrelated to publication order.
    ids = [*new_ids, 100, 99, 98, 97]
    rows = [
        {"id": number, "createtime": NOW - 100, "top": 0,
         "name": f"Synthetic reward preview {number}", "topic_id": 14,
         "topic": {"id": 14, "name": "悬赏"}}
        for number in ids
    ]
    result = {}
    cursor = -1
    for index in range(0, len(rows), 2):
        batch = rows[index:index + 2]
        result[cursor] = {"code": 1, "data": {"posts": batch}}
        cursor = batch[-1]["id"]
    result[cursor] = {"code": 1, "data": {"posts": []}}
    return result


def fixture(values):
    return FixtureForumAdapter(
        values, now=NOW + 10_000, source_scope=SourceScope(58, 14),
        target_category=14, category_mapping=verified_reward_category(),
        body_spec=BodySpec(encoding="plain_utf8", kind="preview",
                           evidence_id="synthetic-watch-restart"),
    )


class VirtualTime:
    def __init__(self):
        self.value = float(NOW)

    def now(self):
        return self.value

    def sleep(self, seconds):
        self.value += seconds


class RoundSource:
    source_scope = SourceScope(58, 14)

    def __init__(self, traces):
        self.traces = traces
        self.round = -1
        self.calls = []

    def fetch_page(self, cursor=-1):
        if cursor == -1:
            self.round += 1
        self.calls.append((self.round, cursor))
        return fixture(self.traces[min(self.round, len(self.traces) - 1)]).fetch_page(cursor)


class RecordingSender:
    def __init__(self, *, fail_first=False):
        self.calls = []
        self.fail_first = fail_first

    def send(self, post):
        self.calls.append(post.post_id)
        if self.fail_first and len(self.calls) == 1:
            raise RuntimeError("synthetic delivery failure")


class WatchRestartTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="watch-restart-offline-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.database = self.root / "watch.sqlite3"
        self.timer = VirtualTime()
        # Fail closed even if an accidental future change reaches a real transport.
        for target in ("campus_watch.adapters.get_json", "campus_watch.watch.get_json",
                       "campus_watch.notifier.get_json", "socket.socket.connect"):
            guard = patch(target, side_effect=AssertionError("network forbidden"))
            mocked = guard.start()
            self.addCleanup(mocked.assert_not_called)
            self.addCleanup(guard.stop)

    def runner(self, store, source, sender):
        return WatchRunner(
            store, lambda _: source, sender, state_path=self.root / "state.json",
            stop_path=self.root / "stop.json", ledger_path=self.root / "ledger.sqlite3",
            fresh_key="SYNTHETIC_FRESH", baseline_pages=2, coverage_pages=2,
            max_pages=3, max_alerts=1, clock=self.timer.now,
            monotonic=self.timer.now, sleeper=self.timer.sleep,
        )

    def row(self, store, post_id):
        return dict(store._connection.execute(
            "SELECT * FROM posts WHERE post_id=?", (post_id,),
        ).fetchone())

    def assert_completed(self, runner, source, cycles):
        self.assertEqual(runner.run(max_cycles=cycles), "stopped")
        self.assertEqual(runner.rounds, cycles)
        self.assertEqual(sum(cursor == -1 for _, cursor in source.calls), cycles)
        state = json.loads((self.root / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["reason"], "offline_cycle_limit")

    def assert_delivery_preserved(self, current, previous):
        # Continued scans may advance last_seen_at, but must preserve delivery evidence.
        for field in ("status", "attempts", "last_error", "send_started_at", "notified_at",
                      "delivery_mode", "body_text", "first_seen_at"):
            self.assertEqual(current[field], previous[field], field)

    def test_failed_delivery_keeps_scanning_and_restart_sends_only_new_posts(self):
        sender = RecordingSender(fail_first=True)
        with SQLiteStore(self.database, target_category=14) as store:
            source = RoundSource([pages(), pages(7), pages(8, 7)])
            self.assert_completed(self.runner(store, source, sender), source, 4)
            self.assertEqual(sender.calls, [7, 8])
            old = self.row(store, 7)
            self.assertEqual((old["status"], old["attempts"], old["last_error"]),
                             ("pending", 1, "RuntimeError"))
            self.assertIsNotNone(old["send_started_at"])
            self.assertIsNone(old["notified_at"])
            self.assertEqual(self.row(store, 8)["status"], "sent")

        resumed_sender = RecordingSender()
        with SQLiteStore(self.database, target_category=14) as reopened:
            source = RoundSource([pages(8, 7), pages(9, 8, 7)])
            runner = self.runner(reopened, source, resumed_sender)
            self.assert_completed(runner, source, 2)
            self.assertEqual(resumed_sender.calls, [9])
            self.assert_delivery_preserved(self.row(reopened, 7), old)
            self.assertEqual(self.row(reopened, 9)["status"], "sent")
            self.assertIn("attempted_notifications_held_no_auto_retry",
                          runner.last_summary["warnings"])

    def test_legacy_failure_or_unfinished_send_does_not_block_new_delivery(self):
        for mode in ("legacy_failure", "unfinished_send"):
            with self.subTest(mode=mode):
                # Separate temporary databases keep both restart histories independent.
                self.database = self.root / f"{mode}.sqlite3"
                with SQLiteStore(self.database, target_category=14) as store:
                    baseline = Monitor(fixture(pages()), store, clock=self.timer.now,
                                       baseline_pages=2, lookback_pages=0).run_once("init")
                    self.assertEqual(baseline.status, "baseline")
                    self.timer.sleep(10)
                    observed = Monitor(
                        fixture(pages(7)), store, clock=self.timer.now,
                        coverage_pages=2, lookback_pages=0,
                        uncertain_policy="notify-window",
                    ).run_once("check")
                    self.assertIn(observed.status, ("ok", "partial"))
                    with store.run_lock():
                        if mode == "legacy_failure":
                            store.mark_delivery_failed(7, "SyntheticLegacyFailure")
                        else:
                            store.freeze_delivery(7, "preview")
                            store.mark_delivery_started(7, int(self.timer.now()))
                    old = self.row(store, 7)
                    self.assertEqual(old["attempts"], 1 if mode == "legacy_failure" else 0)
                    self.assertEqual(old["send_started_at"] is None, mode == "legacy_failure")

                sender = RecordingSender()
                with SQLiteStore(self.database, target_category=14) as reopened:
                    source = RoundSource([pages(7), pages(8, 7)])
                    runner = self.runner(reopened, source, sender)
                    self.assert_completed(runner, source, 2)
                    self.assertEqual(sender.calls, [8])
                    self.assert_delivery_preserved(self.row(reopened, 7), old)
                    self.assertEqual(self.row(reopened, 8)["status"], "sent")


if __name__ == "__main__":
    unittest.main()
