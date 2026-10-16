"""Durable exit-alarm recovery, independent of monitor startup and shutdown."""

from contextlib import closing
import json
import os
from pathlib import Path
import re
import sqlite3
import time

from .supervisor import AlarmOutbox, AlarmSender, HEALTH_REASONS, SCHEMA, _guard_lock, _read_json


RECOVERY_SCHEMA = "campus-watch-alarm-recovery-v1"


def recovery_path(alarm_db, event_id):
    if type(event_id) is not str or re.fullmatch(r"[0-9a-f]{64}", event_id) is None:
        raise ValueError("invalid exit event identifier")
    return Path(alarm_db).resolve().with_suffix(".retries") / (event_id + ".json")


def validate_recovery_event(alarm_db, guard_state, event_id):
    """Only explicit, identity-matched exits may be recovered; never scan history."""
    status_path = recovery_path(alarm_db, event_id)
    path = Path(alarm_db).resolve()
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT * FROM guard_alarm_outbox WHERE event_id=?", (event_id,)).fetchone()
    if row is None or row["reason_code"] in HEALTH_REASONS:
        raise ValueError("exit alarm is not recoverable")
    event = dict(row)
    for snapshot, schema in ((_read_json(guard_state), SCHEMA), (_read_json(status_path), RECOVERY_SCHEMA)):
        if (snapshot.get("schema") == schema
                and snapshot.get("alarm_event_id") == event_id
                and snapshot.get("child_pid") == event["child_pid"]
                and snapshot.get("child_started_at") == event["child_started_at"]):
            return event
    raise ValueError("exit alarm identity is not associated with this instance")


class AlarmRecovery:
    def __init__(self, alarm_db, event_id, *, sender=None, clock=time.time, sleeper=time.sleep,
                 process_started_at=None):
        self.path = recovery_path(alarm_db, event_id)
        self.alarm_db, self.event_id = Path(alarm_db), event_id
        self.sender = sender
        self.clock, self.sleeper = clock, sleeper
        self.recovery_started_at = clock()
        self.started_at = self.recovery_started_at if process_started_at is None else process_started_at
        self.outbox = None

    def publish(self, event):
        value = {"schema": RECOVERY_SCHEMA, "pid": os.getpid(), "started_at": self.started_at,
                 "updated_at": self.clock(), "recovery_started_at": self.recovery_started_at, "alarm_event_id": self.event_id,
                 "child_pid": event["child_pid"], "child_started_at": event["child_started_at"],
                 "state": event["state"], "attempts": event["attempts"],
                 "next_attempt_at": event["next_attempt_at"], "notified_at": event["notified_at"],
                 "last_error": event["last_error"], "last_error_reason": event["last_error_reason"],
                 "http_status": event["http_status"], "bark_code": event["bark_code"]}
        if self.outbox is not None:
            value["alarm_requests"] = self.outbox.request_count
        try:
            temporary = self.path.with_suffix(".json.tmp")
            temporary.write_text(json.dumps(value, allow_nan=False), encoding="utf-8")
            temporary.replace(self.path)
        except OSError:
            # The outbox remains authoritative if a status viewer holds a file.
            pass

    def run(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # One worker per event; different incidents can recover concurrently.
        try:
            with _guard_lock(self.path.with_suffix(".lock")):
                with closing(AlarmOutbox(self.alarm_db, clock=self.clock, sleeper=self.sleeper)) as outbox:
                    self.outbox = outbox
                    event = outbox.event(self.event_id)
                    if event["reason_code"] in HEALTH_REASONS:
                        raise ValueError("live health warnings cannot become exit recovery")
                    if event["state"] != "sent" and self.sender is None:
                        self.sender = AlarmSender()
                    while True:
                        self.publish(event)
                        if event["state"] == "sent":
                            return 0
                        if event["next_attempt_at"] <= self.clock() or event["state"] == "exhausted":
                            try:
                                event = outbox.retry_once(self.event_id, self.sender, progress=self.publish)
                            except OSError:
                                # Another event may own the short POST lock.
                                self.sleeper(1)
                                continue
                            self.publish(event)
                            if event["state"] == "sent":
                                return 0
                        self.sleeper(min(1, max(0.05, event["next_attempt_at"] - self.clock())))
                        event = outbox.event(self.event_id)
        except OSError:
            return 3
