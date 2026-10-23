"""Single-run CLI. External requests require explicit source/delivery choices."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

from .adapters import DoraForumAdapter, FixtureForumAdapter, CategoryMapping, verified_reward_category
from .body import BodySpec
from .body_cipher import CipherParameters
from .monitor import Monitor
from .content import ContentMapping
from .notifier import BarkNotifier, RewardBarkNotifier, ConsoleNotifier
from .simulation import run_fixture_bark_demo
from .storage import SQLiteStore, StoreBusy

DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "reward_58_14.sqlite3"


def emit(value: dict) -> None:
    print(json.dumps(value, ensure_ascii=False))


def run_demo() -> int:
    now = int(time.time())

    def page(*ids):
        return {"code": 1, "data": {"posts": [
            {"id": item, "createtime": now - (110 - item), "top": 0}
            for item in ids
        ]}}

    with tempfile.TemporaryDirectory(prefix="campus-watch-demo-") as directory:
        with SQLiteStore(Path(directory) / "demo.sqlite3") as store:
            adapter = FixtureForumAdapter({"-1": page(100, 99)})
            monitor = Monitor(adapter, store, ConsoleNotifier())
            emit({"stage": "baseline", **monitor.run_once("init").to_dict()})
            monitor.adapter = FixtureForumAdapter({
                "-1": page(103, 102), "102": page(101, 100, 99), "99": page(),
            })
            new = monitor.run_once()
            emit({"stage": "new_ids", **new.to_dict()})
            monitor.adapter = FixtureForumAdapter({
                "-1": page(103, 102), "102": page(101, 100, 99),
            })
            repeat = monitor.run_once()
            emit({"stage": "repeat", **repeat.to_dict()})
            return 0 if new.new_count == 3 and repeat.new_count == 0 else 1


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="中南大校圈：有界单次检测，SQLite 与 Bark")
    parser.add_argument("command", nargs="?", default="check",
                        choices=("init", "rebaseline", "migrate-visibility", "check", "notify", "status", "diagnose", "demo", "fixture-bark-demo", "bark-test", "watch"))
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--fixture", type=Path, help="离线分页 JSON；不访问论坛")
    source.add_argument("--live", action="store_true", help="使用已验证地址做有限真实读取")
    parser.add_argument("--confirm-access", action="store_true", help="已确认本次读取符合平台规则")
    parser.add_argument("--fresh-key", default=None, help="本次有效且分类对应的参数；悬赏环境变量为DORA_FRESH_KEY_14")
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--category", type=int, choices=(0, 14), default=14,
                        help="默认悬赏14；全部0仅保留独立旧模式")
    parser.add_argument("--fresh-key-category", type=int, choices=(0, 14),
                        help="显式传入freshKey对应分类，悬赏必须为14")
    parser.add_argument("--max-pages", type=int, choices=range(1, 6), default=3)
    parser.add_argument("--baseline-pages", type=int, choices=range(1, 6), default=1,
                        help="启动时静默同步的固定页数")
    parser.add_argument("--coverage-pages", type=int, choices=range(1, 6), default=None,
                        help="每次悬赏列表固定扫描页数；默认沿用静默基线页数，最多受每轮列表预算限制")
    parser.add_argument("--max-observation-gap-seconds", type=int, default=900,
                        help="超过此观测间隔时静默同步当前窗口，不补发停机期间历史")
    parser.add_argument("--uncertain-policy", choices=("hold", "notify-window"), default="notify-window",
                        help="默认通知窗口内首次见帖；hold保留候选但不发送")
    parser.add_argument("--post-id", type=int, help="diagnose只读查看指定帖子首次观测前10分钟至后1小时的被动诊断")
    parser.add_argument("--backup", type=Path, help="显式迁移使用的新备份路径，不覆盖已有备份")
    parser.add_argument("--lookback-pages", type=int, choices=(0, 1, 2), default=None,
                        help="额外回看页数；全部帖子模式使用，悬赏最新窗口不回看")
    parser.add_argument("--old-window-hours", type=int, choices=range(0, 721), default=24,
                        help="init时固定：历史首次可见候选记录窗口；不扩大正式时间资格")
    parser.add_argument("--max-alerts", type=int, choices=range(1, 101), default=30)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--interval-seconds", type=int, default=60, help="watch间隔，至少60秒，不补跑或重叠")
    parser.add_argument("--daily-get-limit", type=int, default=4320, help="watch每天GET总预算，包含失败与初始参数")
    parser.add_argument("--fetch-fresh-key", action="store_true", help="watch启动缺少参数时仅获取一次分类14参数；失败暂停")
    parser.add_argument("--freshness-config", type=Path,
                        help="显式freshKey策略JSON；不传或mode=process保持进程级固定参数")
    parser.add_argument("--watch-state", type=Path, help="安全状态JSON，默认data/reward_watch.status.json")
    parser.add_argument("--watch-stop", type=Path, help="停止标记，默认data/reward_watch.stop.json")
    parser.add_argument("--send-bark", action="store_true", help="显式发送待推送；否则仅持久化")
    parser.add_argument("--summary-field", help="已核实的列表摘要/明文字段；不猜字段")
    parser.add_argument("--link-field", help="已核实的帖子URL字段；不推断深链")
    parser.add_argument("--link-host", action="append", default=[], help="明确允许的帖子HTTPS域名；可重复")
    parser.add_argument("--category-field", help="已有可靠证据的响应分类字段")
    parser.add_argument("--category-evidence", help="分类字段映射证据标识；不得猜topic_id")
    parser.add_argument("--body-field", default="name", help="待核实正文源字段，默认name")
    parser.add_argument("--body-encoding", choices=("unknown", "plain_utf8", "base64_utf8", "gzip_base64", "zlib_base64", "aes_cbc_base64"), default="unknown")
    parser.add_argument("--body-kind", choices=("unknown", "full", "preview"), default="unknown")
    parser.add_argument("--body-evidence", help="已核实编码和完整性的证据标识")
    parser.add_argument("--body-limit", type=int, default=500)
    parser.add_argument("--body-profile", choices=("verified-reward-list",),
                        help="本地51/53验证的AES列表预览+三字段分类契约；微信完整性仍待人工核对")
    parser.add_argument("--body-cipher-config", type=Path,
                        help="本机私有AES配置文件路径（不传参数值），AES默认读取data/body_cipher.local.json")
    parser.add_argument("--missing-body", choices=("require", "metadata", "wait"), default=None,
                        help="可见性模式默认require正文，不进行元数据兜底；wait/metadata必须显式选择")
    parser.add_argument("--body-wait-seconds", type=int, default=120,
                        help="wait等待1..3600秒，到期在下次手动检查/notify兜底")
    args = parser.parse_args(argv)
    if args.freshness_config is not None and args.command != "watch":
        emit({"status": "configuration_error", "message": "freshness-config仅适用于watch。"})
        return 2
    if args.db is None:
        args.db = DEFAULT_DB if args.category == 14 else DEFAULT_DB.parent / "campus_watch_v2.sqlite3"
    if args.command == "watch":
        if (args.category != 14 or not args.live or not args.confirm_access or not args.send_bark
            or args.fixture or args.fresh_key or args.body_profile != "verified-reward-list"
            or args.missing_body not in (None, "require") or args.uncertain_policy != "notify-window"
            or args.lookback_pages not in (None, 0)):
            emit({"status": "configuration_error", "message": "watch仅允许明确获准的悬赏真实读取、Bark、已验证正文及窗口模式；参数仅从环境读取。"})
            return 2
        try:
            from .watch import WatchRunner
            from .freshness_config import load_refresh_policy
            refresh_policy = load_refresh_policy(args.freshness_config,
                daily_get_limit=args.daily_get_limit, poll_seconds=args.interval_seconds,
                max_list_pages=args.max_pages)
            # Validate secret and cipher before opening writable monitoring state.
            notifier = RewardBarkNotifier.from_env(timeout_seconds=args.timeout)
            cipher = CipherParameters.from_file(args.body_cipher_config or DEFAULT_DB.parent / "body_cipher.local.json")
            fresh = os.environ.get("DORA_FRESH_KEY_14", "")
            if not fresh and not args.fetch_fresh_key:
                emit({"status": "configuration_error", "message": "需要本次分类参数，或明确--fetch-fresh-key获取一次；不自动循环刷新。"})
                return 2
            with SQLiteStore(args.db, target_category=14) as store:
                spec = BodySpec("name", "aes_cbc_base64", "preview", "local-51-53-20261012-body", args.body_limit)
                def factory(key):
                    return DoraForumAdapter(key, permission_confirmed=True, timeout_seconds=args.timeout,
                        target_category=14, fresh_key_category=14, category_mapping=verified_reward_category(),
                        body_spec=spec, cipher_parameters=cipher, passive_diagnostics=True)
                runner = WatchRunner(store, factory, notifier,
                    state_path=args.watch_state or args.db.parent / "reward_watch.status.json",
                    stop_path=args.watch_stop or args.db.parent / "reward_watch.stop.json",
                    ledger_path=Path(str(args.db) + ".watch_budget.sqlite3"),
                    interval=args.interval_seconds, daily_get_limit=args.daily_get_limit,
                    baseline_pages=args.baseline_pages, coverage_pages=args.coverage_pages or args.baseline_pages,
                    max_pages=args.max_pages, max_alerts=args.max_alerts,
                    fresh_key=fresh, fetch_fresh=args.fetch_fresh_key, timeout=args.timeout,
                    refresh_policy=refresh_policy)
                outcome = runner.run()
                emit({"status": outcome, "message": "watch已结束；只读状态JSON确认原因。"})
                return {"stopped": 0, "busy": 5, "paused": 3}.get(outcome, 1)
        except Exception as exc:
            emit({"status": "configuration_error", "error_type": type(exc).__name__, "message": "watch未启动；请检查本机配置与显式数据库迁移，不输出凭证。"})
            return 2
    try:
        if args.command == "diagnose":
            if args.category != 14 or args.send_bark or args.live or args.fixture:
                raise ValueError("diagnose is an isolated read-only metadata command")
            from .diagnostics import diagnose_post
            emit(diagnose_post(args.db, args.post_id))
            return 0
        if args.command == "demo":
            return run_demo()
        if args.command == "fixture-bark-demo":
            database = args.db if args.db != DEFAULT_DB else DEFAULT_DB.parent / "bark_fixture.sqlite3"
            result = run_fixture_bark_demo(database, send_bark=args.send_bark,
                                          timeout_seconds=args.timeout)
            emit(result)
            return 0 if result["check"]["status"] == "ok" else 3
        if args.command == "bark-test":
            BarkNotifier.from_env(timeout_seconds=args.timeout).send_test()
            emit({"status": "accepted", "message": "Bark服务端已接受；请在iPhone确认实际送达。"})
            return 0
        if args.command == "migrate-visibility":
            if args.category != 14 or args.backup is None or args.live or args.fixture or args.send_bark:
                emit({"status": "configuration_error", "message": "迁移仅限本地悬赏库，需新 --backup 路径；不允许联网或推送。"})
                return 2
            emit({"status": "ok", **SQLiteStore.migrate_visibility_database(args.db, args.backup)})
            return 0
        if args.command == "status":
            if not args.db.exists():
                emit({"status": "missing_database", "message": "请先 init；本命令不创建数据库。"})
                return 2
            # Read-only inspection neither creates schema nor repairs run history.
            connection = sqlite3.connect(args.db.resolve().as_uri() + "?mode=ro", uri=True)
            connection.row_factory = sqlite3.Row
            try:
                state = {r[0]: r[1] for r in connection.execute("SELECT key,value FROM app_state")}
                counts = {r[0]: r[1] for r in connection.execute("SELECT status,COUNT(*) FROM posts GROUP BY status")}
                columns = {r[1] for r in connection.execute("PRAGMA table_info(posts)")}
                if "notification_eligible" in columns:
                    body_column = "body_text" if "body_text" in columns else "summary"
                    queue = connection.execute(
                        "SELECT COALESCE(SUM(notification_eligible=1),0),"
                        "COALESCE(SUM(notification_eligible=0),0),"
                        f"COALESCE(SUM(notification_eligible=1 AND {body_column}=''),0) "
                        "FROM posts WHERE status='pending'"
                    ).fetchone()
                    delivery = dict(zip(("eligible_pending_count", "blocked_unconfirmed_count",
                                         "deferred_missing_summary_count"), queue))
                else:
                    delivery = {"eligible_pending_count": 0,
                                "blocked_unconfirmed_count": counts.get("pending", 0),
                                "deferred_missing_summary_count": 0}
                last = connection.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 1").fetchone()
                visibility = {}
                if "visibility_state" in columns:
                    visibility = {
                        "visibility_state_counts":dict(connection.execute("SELECT visibility_state,COUNT(*) FROM posts GROUP BY visibility_state")),
                        "visibility_snapshot":json.loads(state.get("visibility_snapshot", "null")),
                        "partial_diagnostic_count":connection.execute("SELECT COUNT(*) FROM partial_diagnostics").fetchone()[0],
                        "reason_counts":dict(connection.execute("SELECT visibility_reason,COUNT(*) FROM posts GROUP BY visibility_reason")),
                    }
                    delivery["blocked_unconfirmed_count"] = connection.execute("SELECT COUNT(*) FROM posts WHERE visibility_state='uncertain_first_seen' AND notification_eligible=0").fetchone()[0]
                emit({"status": "ok", "state": state, "counts": counts,
                      "notification_queue": delivery, "last_run": dict(last) if last else None, **visibility})
            finally:
                connection.close()
            return 0
        if args.category == 0 and args.send_bark:
            emit({"status": "configuration_error",
                  "message": "正式通知仅允许悬赏分类14；全部模式仅供读取/兼容检查。"})
            return 2
        adapter = None
        cipher_parameters = None
        if args.body_profile and (args.category != 14 or args.body_field != "name" or
                                  args.body_encoding != "unknown" or args.body_kind != "unknown" or
                                  args.body_evidence or args.category_field or args.category_evidence):
            emit({"status": "configuration_error", "message": "已验证列表profile不能与自选映射混用。"})
            return 2
        body_spec = (BodySpec("name", "aes_cbc_base64", "preview", "local-51-53-20261012-body", args.body_limit)
                     if args.body_profile else BodySpec(args.body_field, args.body_encoding, args.body_kind,
                                                       args.body_evidence, args.body_limit)) if args.category == 14 else None
        category_mapping = None
        if args.body_profile:
            category_mapping = verified_reward_category()
        elif args.category_field or args.category_evidence:
            if args.live and args.category == 14 and args.category_field == "topic_id":
                category_mapping = verified_reward_category(args.category_evidence)
            else:
                category_mapping = CategoryMapping(args.category_field, args.category_evidence)
        if args.command != "notify" and body_spec is not None and body_spec.encoding == "aes_cbc_base64":
            config = args.body_cipher_config or DEFAULT_DB.parent / "body_cipher.local.json"
            if args.body_cipher_config or config.exists():
                cipher_parameters = CipherParameters.from_file(config)
            else:
                cipher_parameters = CipherParameters.from_env()
        hosts = tuple(args.link_host) or tuple(
            item.strip() for item in os.environ.get("BARK_LINK_HOSTS", "").split(",") if item.strip()
        )
        if args.command != "notify":
            if args.fixture:
                mapping = None if not args.summary_field and not args.link_field else ContentMapping(
                    args.summary_field or "summary", args.link_field or "url", hosts
                )
                adapter = FixtureForumAdapter.from_file(args.fixture, allowed_link_hosts=hosts if mapping is None else (),
                                                       content_mapping=mapping, target_category=args.category,
                                                       category_mapping=category_mapping, body_spec=body_spec,
                                                       cipher_parameters=cipher_parameters)
            elif args.live and args.confirm_access:
                category_env = f"DORA_FRESH_KEY_{args.category}"
                fresh_key = args.fresh_key or os.environ.get(category_env, "")
                fresh_category = args.fresh_key_category if args.fresh_key else args.category
                if args.category == 0 and not fresh_key:
                    fresh_key = os.environ.get("DORA_FRESH_KEY", "")
                if not fresh_key:
                    emit({"status": "configuration_error", "message": "需要本次有效 freshKey；没有永久默认值。"})
                    return 2
                if fresh_category != args.category:
                    emit({"status": "configuration_error",
                          "message": "freshKey未明确绑定当前分类；请使用分类专用环境变量或 --fresh-key-category。"})
                    return 2
                adapter = DoraForumAdapter(fresh_key, permission_confirmed=True,
                                           timeout_seconds=args.timeout,
                                           content_mapping=ContentMapping(args.summary_field, args.link_field, hosts),
                                           target_category=args.category, fresh_key_category=fresh_category,
                                           category_mapping=category_mapping, body_spec=body_spec,
                                           cipher_parameters=cipher_parameters, passive_diagnostics=True)
            else:
                emit({"status": "configuration_error",
                      "message": "选择 --fixture，或确认规则后显式 --live --confirm-access。"})
                return 2
        if args.command == "notify" and not args.send_bark:
            emit({"status": "configuration_error", "message": "notify 需显式 --send-bark；可先 status 查看。"})
            return 2
        if args.command not in ("init", "rebaseline") and not args.db.exists():
            emit({"status": "configuration_error", "message": "基线不存在；请先 init。"})
            return 2
        # Validate scope before even constructing a push backend; old all-post
        # databases are refused read-only in the new default reward mode.
        with SQLiteStore(args.db, target_category=args.category) as store:
            notifier_class = RewardBarkNotifier if args.category == 14 else BarkNotifier
            notifier = notifier_class.from_env(timeout_seconds=args.timeout, allowed_link_hosts=hosts) if args.send_bark else None
            summary = Monitor(adapter, store, notifier, max_pages_per_poll=args.max_pages,
                              lookback_pages=(args.lookback_pages if args.lookback_pages is not None else
                                              0 if args.category == 14 and args.uncertain_policy == "notify-window" else 1),
                              old_window_seconds=args.old_window_hours * 3600,
                              max_alerts=args.max_alerts, confirmed_notifications=True,
                              missing_content_policy=args.missing_body,
                              body_wait_seconds=args.body_wait_seconds,
                              baseline_pages=args.baseline_pages, coverage_pages=args.coverage_pages,
                              max_observation_gap_seconds=args.max_observation_gap_seconds,
                              uncertain_policy=args.uncertain_policy).run_once(args.command)
            emit(summary.to_dict())
        return {"baseline": 0, "rebaselined":0, "already_initialized": 0, "ok": 0,
                "partial": 3, "incomplete": 4, "busy": 5}.get(summary.status, 1)
    except StoreBusy:
        emit({"status": "busy", "error_type": "StoreBusy",
              "message": "同库已有进程工作；本次未继续请求或发送。"})
        return 5
    except KeyboardInterrupt:
        emit({"status": "interrupted", "message": "已停止；待发送记录可在重启后恢复。"})
        return 130
    except Exception as exc:
        emit({"status": "failed", "error_type": type(exc).__name__,
              "message": "操作已停止。配置/响应错误；未输出凭证或响应正文。"})
        return 1


if __name__ == "__main__":
    sys.exit(main())
