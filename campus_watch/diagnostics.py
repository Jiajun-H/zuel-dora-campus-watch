"""Read-only, body-free local tracing of a post's recorded visibility path."""

import json
from contextlib import closing
from pathlib import Path
import sqlite3

from .http import sanitize_response_metadata


_REASONS = frozenset({
    "baseline_snapshot", "complete_visibility_difference", "degraded_window_coverage",
    "previous_coverage_partial", "observation_gap", "outside_previous_visible_window",
    "observed_before_window_reference", "observed_before_absence_reference",
    "category_unverified_or_not_reward", "pinned_visibility_unverified",
    "coverage_or_category_unverified", "legacy_visibility_unverified", "category_evidence_lost",
    "deep_first_seen_candidate", "deep_candidate_entered_main_window", "deep_candidate_expired",
    "deep_candidate_rebaseline", "deep_candidate_disabled", "deep_candidate_observation_gap",
    "deep_candidate_contract_lost", "deep_candidate_limit_reached", "scan_uncommitted", "not_persisted",
    "deep_candidate_coverage_interrupted",
    "observed_before_deep_candidate_policy",
    "page_limit_visibility_coverage_not_completed", "visibility_anchors_missing_at_empty_tail",
    "repeated_cursor_no_visibility_state_committed", "ordinary_ids_repeated_across_pages",
    "SourceProtocolError", "TimeoutError", "HttpRequestError", "ValueError", "IntegrityError", "WatchStopped",
})


def _integer(value, *, maximum=2**63 - 1):
    return value if type(value) is int and 0 <= value <= maximum else None


def _reason(value):
    return value if type(value) is str and value in _REASONS else "unrecognized_reason"


def diagnose_post(database, post_id):
    """Trace ten minutes before and one hour after first observation."""
    if type(post_id) is not int or not 1 <= post_id <= 2**63 - 1:
        raise ValueError("diagnose requires a valid --post-id")
    path = Path(database).resolve()
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as con:
        con.row_factory = sqlite3.Row
        scope = dict(con.execute("SELECT key,value FROM app_state WHERE key IN ('source_school','source_category')"))
        if scope != {"source_school": "58", "source_category": "14"}:
            raise ValueError("diagnose requires an isolated school58 reward database")
        row = con.execute(
            "SELECT post_id,created_at,first_seen_at,status,attempts,notification_eligible,visibility_reason,"
            "confirmation_observed_at,send_started_at,notified_at FROM posts WHERE post_id=?", (post_id,),
        ).fetchone()
        committed = row is not None
        if committed:
            post = {k: _integer(row[k]) for k in ("post_id", "created_at", "first_seen_at", "attempts",
                                                "notification_eligible", "confirmation_observed_at", "send_started_at", "notified_at")}
            post["status"] = row["status"] if row["status"] in {"baseline", "older_discovered", "pending", "sent"} else "unknown"
            post["visibility_reason"] = _reason(row["visibility_reason"])
        else:
            partial = con.execute(
                "SELECT created_at,observed_at,reason FROM partial_diagnostics WHERE post_id=? ORDER BY observed_at,run_id LIMIT 1",
                (post_id,),
            ).fetchone()
            if partial is None:
                return {"status": "not_observed", "post_id": post_id,
                        "message": "ID无本地观察记录；不能据此断言平台不可见。"}
            post = {"post_id": post_id, "created_at": _integer(partial["created_at"]),
                    "first_seen_at": _integer(partial["observed_at"]), "status": "uncommitted",
                    "visibility_reason": _reason(partial["reason"]), "notification_eligible": 0,
                    "attempts": 0, "confirmation_observed_at": None, "send_started_at": None, "notified_at": None}
        first = post["first_seen_at"]
        if first is None:
            raise ValueError("invalid local observation time")
        runs, observations, events = [], [], []
        for run in con.execute(
            "SELECT id,started_at,status,observation_summary FROM runs WHERE started_at BETWEEN ? AND ? ORDER BY id LIMIT 240",
            (first - 600, first + 3600),
        ):
            summary = json.loads(run["observation_summary"])
            pages = summary.get("page_observations", [])
            available = isinstance(pages, list) and bool(pages)
            runs.append({"run_id": run["id"], "started_at": run["started_at"],
                         "status": run["status"] if run["status"] in {"ok", "baseline", "rebaselined", "partial", "incomplete", "failed", "running", "interrupted"} else "unknown",
                         "page_diagnostics_available": available})
            if available:
                for page in pages[:5]:
                    items = page.get("posts", [])
                    target = next((p for p in items[:200] if p.get("post_id") == post_id), None)
                    record = {"run_id": run["id"], "page_number": _integer(page.get("page_number"), maximum=5),
                              "cursor": page.get("cursor") if type(page.get("cursor")) is int and -1 <= page["cursor"] <= 2**63-1 else None,
                              "main_window": page.get("main_window") is True,
                              "target_returned": True if target else None if (
                                  page.get("truncated") is True or page.get("fetch_status") != "ok") else False,
                              "fetch_status": "ok" if page.get("fetch_status") == "ok" else "unverified",
                              "truncated": page.get("truncated") is True,
                              "response": sanitize_response_metadata(page.get("response"))}
                    for key in ("started_at", "finished_at"):
                        value = page.get(key)
                        record[key] = value if type(value) in (int, float) and 0 < value < 253402300799 else None
                    if target:
                        record["observation_committed"] = target.get("observation_committed") is True
                        record["visibility_reason"] = _reason(target.get("visibility_reason"))
                        record["notification_eligible"] = target.get("notification_eligible") == 1
                    observations.append(record)
            for event in summary.get("deep_candidate_events", [])[:200]:
                if event.get("post_id") == post_id:
                    events.append({"run_id": run["id"], "reason": _reason(event.get("reason"))})
        returned = [p for p in observations if p["target_returned"] and p["finished_at"] is not None]
        first_return = min((p["finished_at"] for p in returned), default=None)
        first_main = min((p["finished_at"] for p in returned if p["main_window"]), default=None)
        first_deep = min((p["finished_at"] for p in returned if not p["main_window"]), default=None)
        return {"status": "ok" if committed else "observation_uncommitted", "post": post,
                "first_recorded_page_return_at": first_return,
                "first_recorded_main_window_at": first_main, "first_recorded_deep_page_at": first_deep,
                "runs": runs, "page_observations": observations, "candidate_events": events,
                "query_run_limit_reached": len(runs) == 240,
                "limits": ["仅记录保留期内已请求页面，不证明平台首次公开或审核时刻。",
                           "缺少旧版本或过期诊断时，不能把记录缺失当作接口未返回。",
                           "Bark确认落库不是手机精确收件时间；缓存头只提供线索。"]}
