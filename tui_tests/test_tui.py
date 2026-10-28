from __future__ import annotations

import hashlib
import json
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from campus_watch.tui import (
    DatabaseSnapshot,
    OperationResult,
    PowerShellController,
    WatchTUI,
    _runtime_state,
    read_database_snapshot,
)


class FakeController:
    def __init__(self, runtime: dict):
        self.runtime = runtime
        self.start_calls = 0
        self.stop_calls = 0
        self.status_calls = 0
        self.status_error = False

    def status(self):
        self.status_calls += 1
        if self.status_error:
            raise RuntimeError("synthetic unreadable state")
        return dict(self.runtime)

    def start(self):
        self.start_calls += 1
        return OperationResult(True, "already_running")

    def stop(self):
        self.stop_calls += 1
        self.runtime = {"running": False, "process_alive": False, "guardian_alive": False,
                        "runtime_state": "exited", "monitor": {"state": "stopped"}}
        return OperationResult(True, "stop_requested")


def create_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript("""
        CREATE TABLE posts (
            post_id INTEGER PRIMARY KEY, first_seen_at INTEGER NOT NULL,
            status TEXT NOT NULL, source_school INTEGER NOT NULL, top INTEGER NOT NULL,
            attempts INTEGER NOT NULL, last_error TEXT, notified_at INTEGER,
            notification_eligible INTEGER NOT NULL, source_category INTEGER,
            category_evidence TEXT, body_text TEXT NOT NULL, body_status TEXT NOT NULL,
            body_evidence TEXT, body_encoding TEXT, delivery_mode TEXT,
            send_started_at INTEGER
        );
        CREATE TABLE runs (
            id INTEGER PRIMARY KEY, finished_at INTEGER, pages INTEGER,
            fetched_count INTEGER, status TEXT
        );
        INSERT INTO runs VALUES(1, 1792700000, 2, 20, 'ok');
        INSERT INTO posts VALUES
            (1,1792690000,'baseline',58,0,0,NULL,NULL,0,14,'response_field','', 'missing',NULL,NULL,'',NULL),
            (2,1792699000,'sent',58,0,1,NULL,1792699050,1,14,'response_field','新悬赏内容 token=secret_payload_value 联系 13800138000 Cookie: SID=session_cookie_secret; auth=other_secret', 'full','synthetic-evidence','aes_cbc_base64','full',NULL),
            (3,1792699500,'pending',58,0,0,NULL,NULL,1,14,'server_filter','另一个新帖', 'preview','synthetic-evidence','utf8','preview',NULL),
            (4,1792699600,'pending',58,0,0,NULL,NULL,1,0,'response_field','错误分类不可见', 'full','synthetic-evidence','utf8','full',NULL),
            (5,1792699600,'sent',99,0,1,NULL,1792699650,1,14,'response_field','其他学校', 'full','synthetic-evidence','utf8','full',NULL);
    """)
    connection.commit()
    connection.close()


class TuiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / "data").mkdir()
        self.db = self.root / "data" / "reward_58_14.sqlite3"
        create_database(self.db)
        self.runtime = {
            "running": True,
            "process_alive": True,
            "guardian_alive": True,
            "monitor": {
                "state": "waiting", "reason": "next_round_scheduled", "next_check_at": 1792700060,
                "freshness": {"reason": "fixed_interval", "seconds_since_last_successful_acquisition": 80},
            },
        }

    async def asyncTearDown(self):
        self.temp.cleanup()

    def app(self, controller: FakeController, **kwargs) -> WatchTUI:
        return WatchTUI(root=self.root, controller=controller, poll_seconds=30,
                        stop_timeout_seconds=0.2, stop_poll_seconds=0.02, **kwargs)

    def test_read_only_database_metrics_history_and_secret_redaction(self):
        before = hashlib.sha256(self.db.read_bytes()).hexdigest()
        snapshot = read_database_snapshot(self.db)
        after = hashlib.sha256(self.db.read_bytes()).hexdigest()
        self.assertEqual(before, after)
        self.assertEqual(snapshot.baseline_count, 1)
        self.assertEqual(snapshot.distinct_count, 3)
        self.assertEqual(snapshot.new_count, 2)
        self.assertEqual(snapshot.bark_accepted_count, 1)
        self.assertEqual(snapshot.pending_count, 1)
        self.assertEqual([post.post_id for post in snapshot.recent_posts], [3, 2])
        self.assertNotIn("secret_payload_value", snapshot.recent_posts[1].preview)
        self.assertNotIn("13800138000", snapshot.recent_posts[1].preview)
        self.assertNotIn("session_cookie_secret", snapshot.recent_posts[1].preview)
        self.assertNotIn("other_secret", snapshot.recent_posts[1].preview)
        self.assertEqual(snapshot.last_pages, 2)
        self.assertEqual(snapshot.last_posts, 20)

    def test_paused_and_failed_exit_are_not_displayed_as_normal(self):
        self.assertEqual(_runtime_state({"runtime_state": None, "monitor": {"state": "paused"}}, None)[0], "暂停")
        self.assertEqual(_runtime_state({"runtime_state": "failed_exit"}, None)[0], "异常退出")
        self.assertEqual(_runtime_state({}, "LocalReadError")[0], "状态读取异常")
        self.assertEqual(_runtime_state({"runtime_state": []}, None)[0], "未启动")

    def test_attempted_notifications_remain_visible_but_are_not_counted_as_waiting_to_send(self):
        with closing(sqlite3.connect(self.db)) as connection, connection:
            connection.execute("UPDATE posts SET attempts=1, last_error='PushDeliveryError', send_started_at=1792699510 WHERE post_id=3")
            connection.execute("INSERT INTO posts VALUES (6,1792699700,'pending',58,0,0,NULL,NULL,1,14,'response_field','结果不明', 'preview','synthetic-evidence','utf8','preview',1792699710)")
        snapshot = read_database_snapshot(self.db)
        self.assertEqual(snapshot.pending_count, 0)
        self.assertEqual(snapshot.bark_failure_count, 1)
        self.assertEqual(snapshot.uncertain_count, 1)
        labels = {post.post_id: post.bark_status for post in snapshot.recent_posts}
        self.assertIn('不自动重发', labels[3])
        self.assertIn('不自动重发', labels[6])

    async def test_closing_tui_while_scanning_never_calls_stop(self):
        controller = FakeController({
            "running": True, "process_alive": True, "guardian_alive": True,
            "monitor": {"state": "scanning", "last_summary": {"pages": 1, "fetched_count": 10}},
        })
        app = self.app(controller)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            await pilot.press("ctrl+c")
            await pilot.pause()
        self.assertEqual(controller.stop_calls, 0)
        self.assertEqual(controller.start_calls, 0)

    async def test_reopening_attaches_without_starting_another_watcher(self):
        controller = FakeController(self.runtime)
        for _ in range(2):
            app = self.app(controller)
            async with app.run_test(size=(120, 40)) as pilot:
                await pilot.pause()
                self.assertGreater(controller.status_calls, 0)
                app.query_one("#command-input").value = "/quit"
                await pilot.press("enter")
                await pilot.pause()
        self.assertEqual(controller.start_calls, 0)
        self.assertEqual(controller.stop_calls, 0)

    async def test_command_completion_arrows_and_enter_open_status(self):
        controller = FakeController(self.runtime)
        app = self.app(controller)
        async with app.run_test(size=(80, 24)) as pilot:
            await pilot.pause(0.05)
            await pilot.press(*"/st")
            await pilot.press("down", "down", "tab", "enter")
            await pilot.pause(0.05)
            self.assertEqual(app._view_mode, "status")
            self.assertIn("历史基线 1", app._status_report())
            self.assertEqual(controller.start_calls, 0)
            self.assertEqual(controller.stop_calls, 0)
            self.assertFalse(app.query("#stats, #events-pane, #actions, Footer"))
            app.query_one("#command-input").value = "/home"
            await pilot.press("enter")
            await pilot.pause(0.05)
            self.assertEqual(app._view_mode, "messages")
            app.query_one("#command-input").value = "/quit"
            await pilot.press("enter")

    async def test_message_stream_only_appends_new_posts_once_and_idle_refresh_keeps_widgets(self):
        controller = FakeController(self.runtime)
        app = self.app(controller)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause(0.05)
            initial_widgets = tuple(app.query("#content-view .post-entry"))
            self.assertEqual(list(app._rendered_posts), [2, 3])
            await app.refresh_dashboard()
            await pilot.pause(0.05)
            idle_widgets = tuple(app.query("#content-view .post-entry"))
            self.assertEqual(tuple(map(id, idle_widgets)), tuple(map(id, initial_widgets)))

            connection = sqlite3.connect(self.db)
            connection.execute(
                "INSERT INTO posts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (6, 1792699700, "pending", 58, 0, 0, None, None, 1, 14,
                 "response_field", "新发现离线测试帖子", "full", "synthetic-evidence", "utf8", "full", None),
            )
            connection.commit()
            connection.close()
            await app.refresh_dashboard()
            await pilot.pause(0.05)
            self.assertEqual(list(app._rendered_posts), [2, 3, 6])
            self.assertEqual(len(app.query("#content-view .post-entry")), 3)
            self.assertIs(app._rendered_posts[2][1], initial_widgets[0])
            self.assertIs(app._rendered_posts[3][1], initial_widgets[1])
            app.query_one("#command-input").value = "/quit"
            await pilot.press("enter")

    async def test_commands_show_help_posts_start_and_quit_without_implicit_stop(self):
        controller = FakeController(self.runtime)
        app = self.app(controller)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause(0.05)
            app.query_one("#command-input").value = "/help"
            await pilot.press("enter")
            await pilot.pause(0.05)
            self.assertEqual(app._view_mode, "help")
            app.query_one("#command-input").value = "/posts"
            await pilot.press("enter")
            await pilot.pause(0.05)
            self.assertEqual(app._view_mode, "posts")
            self.assertEqual(len(app.query("#content-view .post-entry")), 2)
            app.query_one("#command-input").value = "/start"
            await pilot.press("enter")
            await pilot.pause(0.05)
            self.assertEqual(controller.start_calls, 1)
            self.assertEqual(app._view_mode, "status")
            app.query_one("#command-input").value = "/quit"
            await pilot.press("enter")
            self.assertEqual(controller.stop_calls, 0)

    async def test_posts_view_scrolls_with_arrow_keys(self):
        connection = sqlite3.connect(self.db)
        connection.executemany(
            "INSERT INTO posts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (post_id, 1792699700 + post_id, "sent", 58, 0, 1, None,
                 1792699700 + post_id, 1, 14, "response_field", f"历史帖子 {post_id}",
                 "full", "synthetic-evidence", "utf8", "full", None)
                for post_id in range(10, 35)
            ],
        )
        connection.commit()
        connection.close()
        app = self.app(FakeController(self.runtime))
        async with app.run_test(size=(100, 16)) as pilot:
            await pilot.pause(0.05)
            app.query_one("#command-input").value = "/posts"
            await pilot.press("enter")
            await pilot.pause(0.05)
            view = app.query_one("#content-view")
            before = view.scroll_y
            await pilot.press("down")
            await pilot.pause(0.05)
            self.assertGreater(view.scroll_y, before)
            await pilot.press("up")
            await pilot.pause(0.05)
            self.assertEqual(view.scroll_y, before)
            app.query_one("#command-input").value = "/quit"
            await pilot.press("enter")

    async def test_unreadable_status_json_keeps_ui_alive_and_reports_error(self):
        (self.root / "scripts").mkdir(exist_ok=True)
        (self.root / "scripts" / "GetRewardWatch.ps1").write_text("# synthetic offline script", encoding="utf-8")
        controller = PowerShellController(self.root, shell="powershell.exe")
        invalid_json_result = type("Completed", (), {"stdout": b"{temporarily truncated", "returncode": 0})()
        app = self.app(controller)
        with patch("campus_watch.tui.subprocess.run", return_value=invalid_json_result):
            async with app.run_test(size=(120, 40)) as pilot:
                await pilot.pause()
                self.assertEqual(app.snapshot.runtime["runtime_state"], "status_read_error")
                self.assertEqual(_runtime_state(app.snapshot.runtime, app.snapshot.status_error)[0], "状态读取异常")
                app.query_one("#command-input").value = "/status"
                await pilot.press("enter")
                await pilot.pause()
                self.assertTrue(app.is_running)
                self.assertIn("状态读取异常", app._status_report())
                app.query_one("#command-input").value = "/quit"
                await pilot.press("enter")

    async def test_stop_requires_confirmation_and_exits_after_existing_control_confirms(self):
        controller = FakeController(self.runtime)
        app = self.app(controller)
        async with app.run_test(size=(120, 40)) as pilot:
            app.query_one("#command-input").value = "/stop"
            await pilot.press("enter")
            await pilot.pause()
            self.assertEqual(controller.stop_calls, 0)
            await pilot.press("escape")
            await pilot.pause()
            self.assertEqual(controller.stop_calls, 0)
            app.query_one("#command-input").value = "/stop"
            await pilot.press("enter")
            await pilot.pause()
            await pilot.click("#confirm")
            await pilot.pause(0.2)
            self.assertEqual(controller.stop_calls, 1)
            self.assertFalse(app.is_running)

    async def test_history_is_loaded_again_when_tui_reopens(self):
        controller = FakeController(self.runtime)
        signatures = []
        for _ in range(2):
            app = self.app(controller)
            async with app.run_test(size=(120, 40)) as pilot:
                await pilot.pause(0.05)
                signatures.append(app.snapshot.database.recent_posts)
                app.query_one("#command-input").value = "/quit"
                await pilot.press("enter")
        self.assertEqual(signatures[0], signatures[1])
        self.assertEqual([post.post_id for post in signatures[0]], [3, 2])
        self.assertEqual(controller.stop_calls, 0)

    async def test_paused_state_is_prominent_in_single_status_line(self):
        controller = FakeController({
            "running": True, "process_alive": True, "guardian_alive": True,
            "monitor": {"state": "paused", "reason": "fresh_parameter_missing"},
        })
        app = self.app(controller)
        async with app.run_test(size=(100, 24)) as pilot:
            await pilot.pause(0.05)
            line = app._status_line_signature or ""
            self.assertIn("暂停", line)
            self.assertIn("freshKey", line)
            self.assertIn("已暂停", line)
            app.query_one("#command-input").value = "/quit"
            await pilot.press("enter")

    def test_start_controller_uses_existing_script_and_attaches_when_already_running(self):
        (self.root / "scripts").mkdir(exist_ok=True)
        (self.root / "scripts" / "StartRewardWatch.ps1").write_text("# synthetic offline script", encoding="utf-8")
        completed = type("Completed", (), {"returncode": 0})()
        controller = PowerShellController(self.root, shell="powershell.exe")
        status_views = iter((
            {"running": False, "process_alive": False, "guardian_alive": False},
            {"running": True, "process_alive": True, "guardian_alive": True},
        ))
        controller.status = lambda: next(status_views)
        with patch("campus_watch.tui.subprocess.run", return_value=completed) as run:
            result = controller.start()
        self.assertEqual(result.status, "launching")
        self.assertTrue(result.ok)
        command = run.call_args.args[0]
        self.assertTrue(command[command.index("-File") + 1].endswith("StartRewardWatch.ps1"))
        self.assertIn("-FreshnessConfig", command)
        self.assertEqual(run.call_args.kwargs["stdin"], None)
        self.assertIsNone(run.call_args.kwargs["stdout"])
        self.assertIsNone(run.call_args.kwargs["stderr"])

    def test_status_controller_uses_existing_get_script(self):
        (self.root / "scripts").mkdir(exist_ok=True)
        (self.root / "scripts" / "GetRewardWatch.ps1").write_text("# synthetic offline script", encoding="utf-8")
        controller = PowerShellController(self.root, shell="powershell.exe")
        result = type("Completed", (), {
            "stdout": b'{"running":true,"process_alive":true,"monitor":{"state":"waiting"}}',
            "returncode": 0,
        })()
        with patch("campus_watch.tui.subprocess.run", return_value=result) as run:
            status = controller.status()
        self.assertTrue(status["running"])
        command = run.call_args.args[0]
        self.assertTrue(command[command.index("-File") + 1].endswith("GetRewardWatch.ps1"))
        self.assertEqual(run.call_args.kwargs["timeout"], 3)

    def test_start_while_backend_is_running_attaches_without_another_launcher(self):
        (self.root / "scripts").mkdir(exist_ok=True)
        (self.root / "scripts" / "StartRewardWatch.ps1").write_text("# synthetic offline script", encoding="utf-8")
        controller = PowerShellController(self.root, shell="powershell.exe")
        controller.status = lambda: {"running": True, "process_alive": True, "guardian_alive": True}
        with patch("campus_watch.tui.subprocess.run") as run:
            result = controller.start()
        self.assertEqual(result.status, "already_running")
        self.assertTrue(result.ok)
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
