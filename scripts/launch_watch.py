"""Detach a Windows watcher; credential seed is stdin-only and never saved."""

import json
import getpass
import os
from pathlib import Path
import subprocess
import sys
import importlib
import argparse
import time
from urllib.parse import urlsplit


def spawn_background(command, *, cwd, env):
    """Launch without a Windows console, including the venv redirector child.

    DETACHED_PROCESS alone does not stop a venv console redirector from
    creating a second console. Use the same venv's windowed interpreter and
    CREATE_NO_WINDOW; keep standard handles separate from the launch window.
    """
    command = list(command)
    options = {"cwd": cwd, "env": env, "stdin": subprocess.DEVNULL,
               "stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL,
               "close_fds": True}
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


def main(argv=None):
    root = Path(__file__).resolve().parents[1]
    try:
        sys.path.insert(0, str(root))
        parser = argparse.ArgumentParser(description="Launch the existing single-instance Windows watch flow")
        parser.add_argument("--key-status", action="store_true")
        parser.add_argument("--probe-fresh", action="store_true")
        parser.add_argument("--freshness-config", type=Path)
        parser.add_argument("--retry-alarm")
        args = parser.parse_args(argv)
        if args.retry_alarm:
            from campus_watch.alarm_recovery import validate_recovery_event
            event = validate_recovery_event(root / "data/reward_watch.alarms.sqlite3",
                                           root / "data/reward_watch.supervisor.json", args.retry_alarm)
            if event["state"] == "sent":
                print(json.dumps({"status": "already_sent"}))
                return 0
            command = [sys.executable, "-X", "utf8", str(root / "scripts/watch_supervisor.py"),
                       "--state", str(root / "data/reward_watch.status.json"),
                       "--stop", str(root / "data/reward_watch.stop.json"),
                       "--guard-state", str(root / "data/reward_watch.supervisor.json"),
                       "--alarm-db", str(root / "data/reward_watch.alarms.sqlite3"),
                       "--retry-alarm", args.retry_alarm]
            if os.environ.get("BARK_SERVER"):
                from campus_watch.notifier import _server_base
                command.extend(["--bark-server", _server_base(os.environ["BARK_SERVER"].strip())])
            if os.name == "nt":
                from campus_watch.windows_launch import spawn_independent
                process = spawn_independent(command, cwd=root, script=root / "scripts/SpawnIndependentWatch.ps1")
            else:
                process = spawn_background(command, cwd=root, env=os.environ.copy())
            print(json.dumps({"status": "alarm_recovery_launching", "pid": process.pid}))
            return 0
        config_path = args.freshness_config.resolve() if args.freshness_config is not None else None
        from campus_watch.freshness_config import load_refresh_policy
        refresh_policy = load_refresh_policy(config_path, daily_get_limit=4320, poll_seconds=60, max_list_pages=3)
        if refresh_policy is not None and args.probe_fresh:
            # Legacy probe books a startup GET outside the fixed-mode daily
            # fresh cap. Refuse before reading seed/credentials or any I/O.
            raise ValueError("Legacy parameter probe cannot be combined with fixed freshness policy.")
        from campus_watch.local_secrets import load_bark_key, save_bark_key, normalize_bark_key
        secret_path = root / "data/bark_key.dpapi"
        if args.key_status:
            candidate = os.environ.get("BARK_KEY", "").strip()
            key = normalize_bark_key(candidate) if candidate else load_bark_key(secret_path)
            print(json.dumps({"status": "key_status", "key_available": bool(key),
                              "source": "environment" if candidate else "encrypted_local" if key else "missing"}))
            return 0
        if sys.stdin.isatty():
            seed_line = getpass.getpass("Startup seed (hidden input): ")
        else:
            seed_line = sys.stdin.buffer.readline(8192).decode("utf-8-sig")
        seed = json.loads(seed_line)
        if not isinstance(seed, dict) or set(seed) - {"bark", "fresh"}:
            raise ValueError("invalid seed")
        env = os.environ.copy()
        supplied = seed.get("bark") or env.get("BARK_KEY", "")
        key = normalize_bark_key(supplied) if supplied else load_bark_key(secret_path)
        if not key:
            raise ValueError("local notification key missing")
        if supplied:
            save_bark_key(secret_path, key)
        env["BARK_KEY"] = key
        fresh = seed.get("fresh") or env.get("DORA_FRESH_KEY_14", "")
        if args.probe_fresh and not fresh:
            # Explicit one-shot contract review. Hold the response only in RAM;
            # print types/predicates, never the token, and reuse it after review.
            sys.path.insert(0, str(root))
            from campus_watch.storage import SQLiteStore
            from campus_watch.watch import RequestLedger, FRESH_ENDPOINT
            from campus_watch.adapters import fresh_key_request_params
            from campus_watch.http import get_json
            with SQLiteStore(root / "data/reward_58_14.sqlite3", target_category=14) as store, store.run_lock():
                ledger = RequestLedger(root / "data/reward_58_14.sqlite3.watch_budget.sqlite3", 4320)
                try:
                    ledger.reserve("fresh_get")
                    response = get_json(FRESH_ENDPOINT, params=fresh_key_request_params())
                finally:
                    ledger.close()
            value = response.get("data", {}).get("freshKey") if isinstance(response.get("data"), dict) else None
            print(json.dumps({"status": "parameter_contract_review", "business_code_success": response.get("code") == 1,
                "fresh_value_type": type(value).__name__, "nonempty_string": type(value) is str and bool(value),
                "canonical_positive_integer": type(value) is int and 0 < value <= (1 << 63)-1}, ensure_ascii=False), flush=True)
            if getpass.getpass("Review decision (hidden input): ") != "resume":
                print(json.dumps({"status": "not_launched", "reason": "parameter_review_not_approved"}))
                return 2
            module = importlib.reload(importlib.import_module("campus_watch.watch"))
            fresh = module.parse_fresh_key(response)
        if fresh:
            if type(fresh) is not str or len(fresh) > 4096 or any(ord(c) <= 32 or ord(c) > 126 for c in fresh):
                raise ValueError("invalid parameter")
            env["DORA_FRESH_KEY_14"] = fresh
        child_command = [sys.executable, "-X", "utf8", str(root / "scripts/watch_background.py"),
                   str(root / "data/reward_watch.runtime.jsonl"), "watch", "--live", "--confirm-access",
                   "--category", "14", "--db", str(root / "data/reward_58_14.sqlite3"),
                   "--body-profile", "verified-reward-list", "--missing-body", "require",
                   "--baseline-pages", "2", "--coverage-pages", "2", "--max-pages", "3",
                   "--lookback-pages", "0", "--uncertain-policy", "notify-window", "--interval-seconds", "60",
                   "--daily-get-limit", "4320", "--max-alerts", "20", "--timeout", "10", "--send-bark"]
        if not fresh:
            child_command.append("--fetch-fresh-key")
        if config_path is not None:
            child_command.extend(["--freshness-config", str(config_path)])
        command = [sys.executable, "-X", "utf8", str(root / "scripts/watch_supervisor.py"),
                   "--state", str(root / "data/reward_watch.status.json"),
                   "--stop", str(root / "data/reward_watch.stop.json"),
                   "--guard-state", str(root / "data/reward_watch.supervisor.json"),
                   "--alarm-db", str(root / "data/reward_watch.alarms.sqlite3"),
                   "--runtime", str(root / "data/reward_watch.runtime.jsonl"), "--", *child_command]
        if env.get("BARK_SERVER"):
            from campus_watch.notifier import _server_base
            server = _server_base(env["BARK_SERVER"].strip())
            command[command.index("--"):command.index("--")] = ["--bark-server", server]
        if env.get("BARK_LINK_HOSTS"):
            command[command.index("--"):command.index("--")] = ["--link-hosts", env["BARK_LINK_HOSTS"]]
        launch_requested_at = time.time()
        if os.name == "nt":
            # The process provider does not inherit the caller's job tree or
            # environment. Device Key is already protected by CurrentUser DPAPI.
            if fresh:
                raise ValueError("Independent startup cannot carry transient category parameters; use normal initial parameter acquisition.")
            from campus_watch.windows_launch import spawn_independent
            process = spawn_independent(command, cwd=root, script=root / "scripts/SpawnIndependentWatch.ps1")
        else:
            process = spawn_background(command, cwd=root, env=env)
        # Launching is not running; status file and OS process must be verified.
        print(json.dumps({"status": "launching", "pid": process.pid, "launch_requested_at": launch_requested_at,
                          "state_file": str(root / "data/reward_watch.status.json")}, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "not_launched", "error_type": type(exc).__name__}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
