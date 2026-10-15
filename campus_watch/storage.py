"""Atomic metadata/outbox persistence and process-wide, crash-released run lock."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable

from .models import ForumPost, RewardPost


class StoreError(RuntimeError):
    pass


class StoreBusy(StoreError):
    pass


class SQLiteStore:
    def __init__(self, database_path: str | Path, *, target_category: int | None = None,
                 discovery_policy: str | None = None) -> None:
        if target_category is not None and (type(target_category) is not int or target_category not in (0, 14)):
            raise StoreError("unsupported database category")
        self.target_category = target_category or 0
        self.discovery_policy = discovery_policy or ("visibility" if self.target_category == 14 else "legacy")
        if self.discovery_policy not in ("visibility", "legacy") or (self.target_category == 0 and self.discovery_policy != "legacy"):
            raise StoreError("unsupported database discovery policy")
        self.database_path = (
            ":memory:" if str(database_path) == ":memory:"
            else str(Path(database_path).resolve())
        )
        self._locked = False
        if self.target_category == 14 and self.database_path != ":memory:" and Path(self.database_path).exists():
            self._preflight_scope()
        if self.database_path != ":memory:":
            Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.database_path, timeout=0.25)
        self._connection.row_factory = sqlite3.Row
        try:
            version = self._read_schema_version()
            tables = self._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            allowed_versions = ((5,) if self.discovery_policy == "visibility" else (4,)) if self.target_category == 14 else (2, 3)
            if (tables and version not in allowed_versions) or (not tables and version not in (0, *allowed_versions)):
                raise StoreError("incompatible database; preserve it and use a new --db")
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            if version == 2:
                self._migrate_v2()
            if not tables:
                with self.run_lock(recover_runs=False):
                    schema = """
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY, started_at INTEGER NOT NULL,
                    finished_at INTEGER, status TEXT NOT NULL,
                    pages INTEGER NOT NULL DEFAULT 0,
                    fetched_count INTEGER NOT NULL DEFAULT 0,
                    new_count INTEGER NOT NULL DEFAULT 0,
                    delivered_count INTEGER NOT NULL DEFAULT 0,
                    pending_count INTEGER NOT NULL DEFAULT 0,
                    suppressed_count INTEGER NOT NULL DEFAULT 0,
                    error_type TEXT, warnings TEXT NOT NULL DEFAULT '[]',
                    coverage TEXT NOT NULL DEFAULT 'none',
                    confirmed_new_count INTEGER NOT NULL DEFAULT 0,
                    blocked_unconfirmed_count INTEGER NOT NULL DEFAULT 0,
                    deferred_missing_summary_count INTEGER NOT NULL DEFAULT 0,
                    baseline_expanded_count INTEGER NOT NULL DEFAULT 0,
                    missing_anchor_count INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS posts (
                    post_id INTEGER PRIMARY KEY, created_at INTEGER NOT NULL,
                    top INTEGER NOT NULL CHECK(top IN (0,1)),
                    source_school INTEGER NOT NULL CHECK(source_school=58),
                    first_seen_at INTEGER NOT NULL, batch_id INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK(status IN
                        ('baseline','older_discovered','pending','sent')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT, notified_at INTEGER,
                    summary TEXT NOT NULL DEFAULT '', url TEXT,
                    notification_eligible INTEGER NOT NULL DEFAULT 0
                        CHECK(notification_eligible IN (0,1)),
                    confirmation_since INTEGER, confirmation_observed_at INTEGER,
                    notification_reason TEXT NOT NULL DEFAULT 'legacy_unverified',
                    FOREIGN KEY(batch_id) REFERENCES runs(id)
                );
                CREATE TABLE IF NOT EXISTS app_state (
                    key TEXT PRIMARY KEY, value TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS pending_order
                    ON posts(status,created_at,post_id);
                PRAGMA user_version=3;
                COMMIT;
                """
                    if self.target_category == 14:
                        schema = schema.replace("PRAGMA user_version=3;", self._reward_schema(visibility=self.discovery_policy == "visibility"))
                    self._connection.executescript(schema)
            self._connection.commit()
        except Exception:
            self._connection.close()
            raise

    def __enter__(self) -> "SQLiteStore":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def close(self) -> None:
        self._connection.close()

    def _preflight_scope(self) -> None:
        """Refuse another category before changing journal mode or history."""
        connection = sqlite3.connect(Path(self.database_path).as_uri() + "?mode=ro&immutable=1", uri=True)
        try:
            tables = {r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not tables:
                if Path(self.database_path).stat().st_size:
                    raise StoreError("existing database scope cannot be verified without changing it")
                return
            row = connection.execute("SELECT value FROM app_state WHERE key='source_category'").fetchone() if "app_state" in tables else None
            existing = int(row[0]) if row else 0
            school = connection.execute("SELECT value FROM app_state WHERE key='source_school'").fetchone() if row else None
            if existing != self.target_category or (row and (school is None or school[0] != "58")):
                raise StoreError("database scope mismatch; keep this database and use a separate category database")
        finally:
            connection.close()

    @staticmethod
    def _reward_schema(*, visibility: bool = False) -> str:
        schema = """
        ALTER TABLE posts ADD COLUMN source_category INTEGER;
        ALTER TABLE posts ADD COLUMN category_evidence TEXT NOT NULL DEFAULT 'unknown';
        ALTER TABLE posts ADD COLUMN body_text TEXT NOT NULL DEFAULT '';
        ALTER TABLE posts ADD COLUMN body_status TEXT NOT NULL DEFAULT 'missing';
        ALTER TABLE posts ADD COLUMN body_reason TEXT NOT NULL DEFAULT 'not_configured';
        ALTER TABLE posts ADD COLUMN body_source TEXT;
        ALTER TABLE posts ADD COLUMN body_encoding TEXT;
        ALTER TABLE posts ADD COLUMN body_evidence TEXT;
        ALTER TABLE posts ADD COLUMN body_truncated INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE posts ADD COLUMN body_deadline INTEGER;
        ALTER TABLE posts ADD COLUMN delivery_mode TEXT NOT NULL DEFAULT '';
        INSERT INTO app_state(key,value) VALUES('source_school','58'),('source_category','14');
        PRAGMA user_version=4;
        """
        if visibility:
            schema += "\n".join(SQLiteStore._visibility_schema_statements())
        return schema

    @staticmethod
    def _visibility_schema_statements() -> tuple[str, ...]:
        return (
            "ALTER TABLE posts ADD COLUMN visibility_state TEXT NOT NULL DEFAULT 'observed' CHECK(visibility_state IN ('baseline','observed','newly_observed_confirmed','uncertain_first_seen'));",
            "ALTER TABLE posts ADD COLUMN visibility_reason TEXT NOT NULL DEFAULT 'legacy_visibility_unverified';",
            "ALTER TABLE posts ADD COLUMN visibility_previous_run_id INTEGER REFERENCES runs(id);",
            "ALTER TABLE posts ADD COLUMN visibility_confirmed_run_id INTEGER REFERENCES runs(id);",
            "ALTER TABLE posts ADD COLUMN visibility_confirmed_at INTEGER;",
            "ALTER TABLE posts ADD COLUMN last_seen_at INTEGER;",
            "ALTER TABLE posts ADD COLUMN send_started_at INTEGER;",
            "CREATE TABLE partial_diagnostics (post_id INTEGER NOT NULL,run_id INTEGER NOT NULL,observed_at INTEGER NOT NULL,reason TEXT NOT NULL,created_at INTEGER NOT NULL,top INTEGER NOT NULL,source_category INTEGER,category_evidence TEXT NOT NULL,body_status TEXT NOT NULL,PRIMARY KEY(post_id,run_id),FOREIGN KEY(run_id) REFERENCES runs(id));",
            "CREATE TABLE visibility_snapshots (run_id INTEGER PRIMARY KEY,observed_at INTEGER NOT NULL,pages INTEGER NOT NULL,terminal INTEGER NOT NULL CHECK(terminal IN (0,1)),ordinary_order TEXT NOT NULL,absence_reference_run_id INTEGER,FOREIGN KEY(run_id) REFERENCES runs(id),FOREIGN KEY(absence_reference_run_id) REFERENCES runs(id));",
            "ALTER TABLE runs ADD COLUMN observation_summary TEXT NOT NULL DEFAULT '{}';",
            "INSERT INTO app_state(key,value) VALUES('discovery_policy','visibility');",
            "PRAGMA user_version=5;",
        )

    @classmethod
    def migrate_visibility_database(cls, database_path: str | Path, backup_path: str | Path) -> dict:
        """Explicit reward-only migration with an immutable pre-change backup.

        Opening/status never invokes this method. A backup is never overwritten;
        after any later sends it must not be restored over current outbox state.
        """
        source_path = Path(database_path).resolve()
        backup = Path(backup_path).resolve()
        if not source_path.is_file() or backup == source_path or backup.exists():
            raise StoreError("migration requires an existing source and a new distinct backup path")
        audit = sqlite3.connect(source_path.as_uri() + "?mode=ro", uri=True)
        try:
            tables = {r[0] for r in audit.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if "app_state" not in tables:
                raise StoreError("only a scoped reward database can be migrated")
            scope = dict(audit.execute("SELECT key,value FROM app_state WHERE key IN ('source_school','source_category')"))
            if scope != {"source_school": "58", "source_category": "14"}:
                raise StoreError("database scope mismatch; preserve the original database")
        finally:
            audit.close()
        holder = object.__new__(cls)
        holder.database_path = str(source_path)
        holder.target_category = 14
        holder.discovery_policy = "visibility"
        holder._locked = False
        holder._connection = sqlite3.connect(str(source_path), timeout=0.25)
        holder._connection.row_factory = sqlite3.Row
        try:
            with holder.run_lock(recover_runs=False):
                version = holder._connection.execute("PRAGMA user_version").fetchone()[0]
                scope = {r[0]: r[1] for r in holder._connection.execute(
                    "SELECT key,value FROM app_state WHERE key IN ('source_school','source_category')"
                )}
                if version not in (4, 5) or scope != {"source_school": "58", "source_category": "14"}:
                    raise StoreError("only a verified reward schema4 database can be migrated")
                if version == 5:
                    return {"schema_before": 5, "schema_after": 5, "changed": False, "backup_validated": False}
                counts = {table: holder._connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("posts", "runs")}
                backup.parent.mkdir(parents=True, exist_ok=True)
                # Reserve the new target without a check/open overwrite race.
                with backup.open("xb"):
                    pass
                destination = sqlite3.connect(str(backup))
                try:
                    holder._connection.backup(destination)
                    valid = destination.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
                    valid = valid and destination.execute("PRAGMA user_version").fetchone()[0] == 4
                    valid = valid and all(destination.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == count for table, count in counts.items())
                    if not valid:
                        raise StoreError("migration backup validation failed")
                finally:
                    destination.close()
                holder._migrate_reward_v4()
                return {"schema_before": 4, "schema_after": 5, "changed": True,
                        "backup_validated": True, "posts_preserved": counts["posts"], "runs_preserved": counts["runs"]}
        finally:
            holder._connection.close()

    def _migrate_reward_v4(self) -> None:
        self._require_lock()
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            if self._connection.execute("PRAGMA user_version").fetchone()[0] != 4:
                raise StoreError("reward schema changed before migration")
            for statement in self._visibility_schema_statements():
                self._connection.execute(statement)
            self._connection.execute(
                "UPDATE posts SET visibility_state=CASE WHEN status='baseline' THEN 'baseline' ELSE 'observed' END,"
                "visibility_reason='legacy_visibility_unverified',last_seen_at=first_seen_at"
            )
            self._connection.execute(
                "UPDATE posts SET notification_eligible=0,notification_reason='legacy_visibility_unverified' WHERE status='pending'"
            )
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

    def _read_schema_version(self) -> int:
        # After TerminateProcess, Windows can briefly retain a WAL mapping.
        # Retry only this opening read and this specific truncate-recovery
        # error, before any application mutation or notification is possible.
        for attempt in range(4):
            try:
                return self._connection.execute("PRAGMA user_version").fetchone()[0]
            except sqlite3.OperationalError as exc:
                if os.name != "nt" or getattr(exc, "sqlite_errorname", "") != "SQLITE_IOERR_TRUNCATE" or attempt == 3:
                    raise
                self._connection.close()
                time.sleep(0.05 * (2 ** attempt))
                self._connection = sqlite3.connect(self.database_path, timeout=0.25)
                self._connection.row_factory = sqlite3.Row
        raise StoreError("database schema could not be read")

    def _migrate_v2(self) -> None:
        """Upgrade atomically under the same lock; old pending stay unverified."""
        with self.run_lock(recover_runs=False):
            if self._connection.execute("PRAGMA user_version").fetchone()[0] == 3:
                return
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                additions = {
                    "posts": {
                        "summary": "TEXT NOT NULL DEFAULT ''", "url": "TEXT",
                        "notification_eligible": "INTEGER NOT NULL DEFAULT 0 CHECK(notification_eligible IN (0,1))",
                        "confirmation_since": "INTEGER", "confirmation_observed_at": "INTEGER",
                        "notification_reason": "TEXT NOT NULL DEFAULT 'legacy_unverified'",
                    },
                    "runs": {
                        name: "INTEGER NOT NULL DEFAULT 0" for name in (
                            "confirmed_new_count", "blocked_unconfirmed_count",
                            "deferred_missing_summary_count", "baseline_expanded_count",
                            "missing_anchor_count",
                        )
                    },
                }
                for table, columns in additions.items():
                    existing = {row[1] for row in self._connection.execute(f"PRAGMA table_info({table})")}
                    for name, declaration in columns.items():
                        if name not in existing:
                            self._connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")
                self._connection.execute("PRAGMA user_version=3")
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise

    @contextmanager
    def run_lock(self, *, recover_runs: bool = True):
        """One process owns fetch + commit + send. OS releases on process death.

        The lock file persists intentionally; its presence does not mean locked.
        Use a local disk and the same resolved database path for all processes.
        """
        if self._locked:
            raise StoreBusy("database operation already active")
        handle = None
        acquired = False
        try:
            if self.database_path != ":memory:":
                handle = open(self.database_path + ".run.lock", "a+b")
                handle.seek(0, os.SEEK_END)
                if handle.tell() == 0:
                    handle.write(b"0")
                    handle.flush()
                handle.seek(0)
                try:
                    if os.name == "nt":
                        import msvcrt
                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    raise StoreBusy("another process owns this database") from None
                acquired = True
            self._locked = True
            if recover_runs:
                with self._connection:
                    self._connection.execute(
                    "UPDATE runs SET status='interrupted', finished_at=?, "
                    "error_type='ProcessInterrupted' WHERE status='running'",
                    (int(time.time()),),
                )
            yield
        finally:
            self._locked = False
            if handle is not None:
                try:
                    if acquired:
                        handle.seek(0)
                        if os.name == "nt":
                            import msvcrt
                            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                        else:
                            import fcntl
                            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                finally:
                    handle.close()

    def _require_lock(self) -> None:
        if not self._locked:
            raise StoreError("mutating operations require the run lock")

    def _state(self, key: str) -> str | None:
        row = self._connection.execute(
            "SELECT value FROM app_state WHERE key=?", (key,)
        ).fetchone()
        return row[0] if row else None

    def _set_state(self, key: str, value: str) -> None:
        self._connection.execute(
            "INSERT INTO app_state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value)
        )

    @property
    def baseline_complete(self) -> bool:
        if self.discovery_policy == "visibility":
            return self.visibility_snapshot is not None
        return self._state("baseline_at") is not None

    @property
    def baseline_at(self) -> int:
        return int(self._state("baseline_at") or 0)

    @property
    def old_window_seconds(self) -> int:
        return int(self._state("old_window_seconds") or 0)

    @property
    def baseline_expansion_pending(self) -> bool:
        # Missing marker means a legacy database: preserve its existing policy.
        return self._state("baseline_expansion_complete") == "0"

    @property
    def confirmation_boundary(self) -> int:
        recorded = self._state("last_confirmed_at")
        if recorded is not None:
            return int(recorded)
        run_id = self._state("confirmed_run_id")
        row = self._connection.execute("SELECT started_at FROM runs WHERE id=?", (run_id,)).fetchone()
        return int(row[0]) if row else self.baseline_at

    @property
    def frontier(self) -> set[int]:
        return set(json.loads(self._state("frontier") or "[]"))

    def known_ids(self) -> set[int]:
        return {r[0] for r in self._connection.execute("SELECT post_id FROM posts")}

    @property
    def visibility_snapshot(self) -> dict | None:
        if self.discovery_policy != "visibility":
            return None
        value = self._state("visibility_snapshot")
        return json.loads(value) if value is not None else None

    @property
    def snapshot(self) -> dict | None:
        return self.visibility_snapshot

    def visibility_counts(self) -> dict[str, int]:
        if self.discovery_policy != "visibility":
            return {}
        return {row[0]: row[1] for row in self._connection.execute(
            "SELECT visibility_state,COUNT(*) FROM posts GROUP BY visibility_state"
        )}

    def visibility_counts_for_ids(self, post_ids: Iterable[int]) -> dict[str, int]:
        values = list(set(post_ids))
        if not values:
            return {}
        marks = ",".join("?" for _ in values)
        return dict(self._connection.execute(
            f"SELECT visibility_state,COUNT(*) FROM posts WHERE post_id IN ({marks}) GROUP BY visibility_state", values))

    def visibility_policy_blocked_count(self, policy: str) -> int:
        if self.discovery_policy != "visibility" or policy != "hold":
            return 0
        return self._connection.execute(
            "SELECT COUNT(*) FROM posts WHERE status='pending' AND notification_eligible=1 "
            "AND visibility_state='uncertain_first_seen'"
        ).fetchone()[0]

    @property
    def partial_diagnostic_count(self) -> int:
        if self.discovery_policy != "visibility":
            return 0
        return self._connection.execute("SELECT COUNT(*) FROM partial_diagnostics").fetchone()[0]

    def previously_observed_before_reference(self, reference_run_id: int) -> set[int]:
        """A failed earlier observation cannot become a later first-public claim."""
        return {r[0] for r in self._connection.execute(
            "SELECT DISTINCT p.post_id FROM partial_diagnostics p JOIN visibility_snapshots s "
            "ON s.run_id=? WHERE p.observed_at<=s.observed_at", (reference_run_id,))}

    @property
    def deep_window_candidates(self) -> dict[int, dict]:
        """Short-lived qualifications, separate from permanent ever-seen IDs."""
        raw = json.loads(self._state("deep_window_candidates_v1") or "{}")
        if not isinstance(raw, dict) or len(raw) > 1000:
            raise StoreError("invalid deep candidate registry")
        result = {}
        fields = {"first_run_id", "first_seen_at", "expires_at", "first_page"}
        for key, item in raw.items():
            if (not isinstance(key, str) or not re.fullmatch(r"[1-9][0-9]{0,18}", key)
                    or int(key) > 2**63 - 1 or not isinstance(item, dict)
                    or set(item) != fields or any(type(v) is not int for v in item.values())
                    or item["first_run_id"] < 1 or item["first_seen_at"] < 1
                    or not 0 < item["expires_at"] - item["first_seen_at"] <= 3600
                    or not 1 <= item["first_page"] <= 5):
                raise StoreError("invalid deep candidate entry")
            result[int(key)] = item
        return result

    @staticmethod
    def _visibility_reason(value: object) -> str:
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,95}", value):
            raise StoreError("visibility reason must be a safe diagnostic identifier")
        return value

    def record_partial_observation(self, posts: Iterable[ForumPost], run_id: int,
                                   now: int, reason: str) -> int:
        """Keep first-observed audit metadata without poisoning ever-seen IDs."""
        self._require_lock()
        if self.discovery_policy != "visibility" or type(now) is not int or now <= 0:
            raise StoreError("partial observation requires visibility policy and a valid time")
        reason = self._visibility_reason(reason)
        unknown = {p.post_id: p for p in posts if p.post_id not in self.known_ids()}
        with self._connection:
            for post in unknown.values():
                self._connection.execute(
                    "INSERT INTO partial_diagnostics(post_id,run_id,observed_at,reason,created_at,top,source_category,category_evidence,body_status) VALUES(?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(post_id,run_id) DO UPDATE SET observed_at=MIN(observed_at,excluded.observed_at)",
                    (post.post_id, run_id, now, reason, post.created_at, post.top,
                     getattr(post, "source_category", None), getattr(post, "category_evidence", "unknown"),
                     getattr(getattr(post, "body", None), "status", "missing")),
                )
        return len(unknown)

    @staticmethod
    def _reward_category_ok(post: ForumPost) -> bool:
        return getattr(post, "source_category", None) == 14 and getattr(post, "category_evidence", "unknown") in (
            "server_filter", "response_field", "fixture_scope"
        ) and (post.top == 0 or getattr(post, "category_evidence", "unknown") == "response_field")

    def _insert_reward_body(self, post: ForumPost, *, eligible: bool,
                            observed_at: int, body_wait_seconds: int) -> None:
        body = getattr(post, "body", None)
        available = eligible and body is not None and body.status in ("full", "preview") and bool(body.text and body.evidence_id)
        self._connection.execute(
            "UPDATE posts SET source_category=?,category_evidence=?,body_text=?,body_status=?,"
            "body_reason=?,body_source=?,body_encoding=?,body_evidence=?,body_truncated=?,body_deadline=? WHERE post_id=?",
            (getattr(post, "source_category", None), getattr(post, "category_evidence", "unknown"),
             body.text if available else "", body.status if available else (body.status if body and body.status not in ("full", "preview") else "missing"),
             body.reason if body else "not_configured", body.source_field if body else None,
             body.encoding if body else "unknown", body.evidence_id if body else None,
             int(body.truncated) if available else 0, observed_at + body_wait_seconds, post.post_id),
        )

    def _refresh_existing_reward(self, post: ForumPost, observed_at: int) -> None:
        """Only enrich/revoke an existing qualification; never promote it."""
        self._connection.execute("UPDATE posts SET top=?,last_seen_at=? WHERE post_id=?", (post.top, observed_at, post.post_id))
        if not self._reward_category_ok(post):
            self._connection.execute(
                "UPDATE posts SET notification_eligible=0,notification_reason='category_evidence_lost' WHERE post_id=? AND status='pending'",
                (post.post_id,),
            )
            return
        self._connection.execute(
            "UPDATE posts SET summary=CASE WHEN summary='' THEN ? ELSE summary END,url=CASE WHEN url IS NULL THEN ? ELSE url END "
            "WHERE post_id=? AND status='pending' AND notification_eligible=1 AND attempts=0 AND delivery_mode='' AND send_started_at IS NULL",
            (post.summary, post.url, post.post_id),
        )
        body = getattr(post, "body", None)
        if body is not None and body.status in ("full", "preview"):
            self._connection.execute(
                "UPDATE posts SET body_text=?,body_status=?,body_reason=?,body_source=?,body_encoding=?,body_evidence=?,body_truncated=? "
                "WHERE post_id=? AND status='pending' AND notification_eligible=1 AND attempts=0 AND delivery_mode='' AND send_started_at IS NULL "
                "AND (body_text='' OR (body_status='preview' AND ?='full'))",
                (body.text, body.status, body.reason, body.source_field, body.encoding, body.evidence_id,
                 int(body.truncated), post.post_id, body.status),
            )

    def commit_visibility_observation(
        self, posts: Iterable[ForumPost], frontier: set[int], run_id: int, *, now: int,
        observed_completed_at: int, baseline: bool = False, snapshot_order: list[int],
        snapshot_terminal: bool, snapshot_pages: int, confirmed_ids: set[int],
        uncertain_ids: set[int], uncertainty_reasons: dict[int, str],
        degraded_ids: set[int] = frozenset(), body_wait_seconds: int = 120,
        absence_reference_run_id: int | None = None,
        deferred_ids: set[int] = frozenset(), promoted_ids: set[int] = frozenset(),
        candidate_ttl_seconds: int = 0, candidate_continuous: bool = False,
        candidate_pages: dict[int, int] | None = None,
        candidate_cancel_reason: str = "deep_candidate_observation_gap",
    ) -> tuple[list[ForumPost], int, int]:
        """Apply a completed visibility proof supplied by the scanner.

        No creation timestamp or numeric ID comparison grants eligibility.
        Existing history never gains one, except an explicitly registered,
        unexpired deep candidate entering a supported ordinary main window.
        """
        self._require_lock()
        if self.discovery_policy != "visibility" or self.target_category != 14:
            raise StoreError("visibility commit requires a visibility reward database")
        if type(now) is not int or type(observed_completed_at) is not int or now <= 0 or observed_completed_at < now:
            raise StoreError("invalid visibility observation time")
        if type(snapshot_terminal) is not bool or type(snapshot_pages) is not int or snapshot_pages < 1:
            raise StoreError("invalid visibility snapshot coverage")
        if type(body_wait_seconds) is not int or body_wait_seconds < 0:
            raise StoreError("invalid body wait bound")
        projected = list(posts)
        ids = {post.post_id for post in projected}
        ordinary = {post.post_id for post in projected if post.top == 0}
        if len(ids) != len(projected) or len(snapshot_order) != len(set(snapshot_order)) or not set(snapshot_order) <= ordinary:
            raise StoreError("visibility snapshot must contain unique ordinary observed IDs")
        if not frontier <= ordinary or not confirmed_ids <= ids or not uncertain_ids <= ids or not set(degraded_ids) <= ids:
            raise StoreError("visibility proof contains unobserved IDs")
        if confirmed_ids & uncertain_ids or confirmed_ids & set(degraded_ids):
            raise StoreError("visibility proof classifications conflict")
        if (type(candidate_ttl_seconds) is not int or not 0 <= candidate_ttl_seconds <= 3600
                or type(candidate_continuous) is not bool
                or not set(deferred_ids) <= ordinary - set(snapshot_order)
                or not set(promoted_ids) <= set(snapshot_order) - confirmed_ids
                or set(deferred_ids) & (confirmed_ids | set(degraded_ids))):
            raise StoreError("invalid deep candidate decision")
        candidate_pages = candidate_pages or {}
        if any(type(candidate_pages.get(i)) is not int or not 1 <= candidate_pages[i] <= 5 for i in deferred_ids):
            raise StoreError("deep candidate requires an observed page")
        safe_reasons = {post_id: self._visibility_reason(reason) for post_id, reason in uncertainty_reasons.items()}
        if candidate_cancel_reason not in ("deep_candidate_observation_gap", "deep_candidate_coverage_interrupted"):
            raise StoreError("invalid deep candidate cancellation reason")
        previous = self.visibility_snapshot
        if not baseline and previous is None:
            raise StoreError("visibility baseline proof is required")
        previous_run = previous["run_id"] if previous else None
        previous_at = previous["observed_at"] if previous else None
        if absence_reference_run_id == run_id and not snapshot_terminal:
            raise StoreError("a current absence reference requires a terminal observation")
        reference_to_validate = (previous.get("absence_reference_run_id") if previous else None) if absence_reference_run_id == run_id else absence_reference_run_id
        if reference_to_validate is not None:
            reference = self._connection.execute(
                "SELECT terminal FROM visibility_snapshots WHERE run_id=?", (reference_to_validate,)
            ).fetchone()
            if not reference or reference[0] != 1:
                raise StoreError("absence reference must be a persisted terminal observation")
        if confirmed_ids and reference_to_validate is None:
            raise StoreError("confirmed visibility IDs require an earlier terminal absence reference")
        effective_reference = run_id if snapshot_terminal else absence_reference_run_id
        new_posts = []
        suppressed = expanded = 0
        registry = self.deep_window_candidates
        epoch_value = self._state("deep_candidate_policy_v1_since_run")
        if epoch_value is not None and not re.fullmatch(r"[1-9][0-9]*", epoch_value):
            raise StoreError("invalid deep candidate policy epoch")
        candidate_epoch = int(epoch_value) if epoch_value is not None else run_id
        outcome = {"created_count": 0, "promoted_count": 0, "expired_count": 0,
                   "cancelled_count": 0, "events": []}
        def event(post_id, reason):
            if len(outcome["events"]) < 200:
                outcome["events"].append({"post_id": post_id, "reason": reason})
        def retire(post_id, reason, kind):
            registry.pop(post_id, None)
            self._connection.execute(
                "UPDATE posts SET visibility_reason=?,notification_reason=? WHERE post_id=? "
                "AND status='older_discovered' AND notification_eligible=0",
                (reason, reason, post_id),
            )
            outcome[kind] += 1
            event(post_id, reason)
        with self._connection:
            for post_id, item in list(registry.items()):
                if baseline or not candidate_continuous or not candidate_ttl_seconds:
                    reason = ("deep_candidate_rebaseline" if baseline else
                              "deep_candidate_disabled" if not candidate_ttl_seconds else candidate_cancel_reason)
                    retire(post_id, reason, "cancelled_count")
                elif observed_completed_at >= item["expires_at"]:
                    retire(post_id, "deep_candidate_expired", "expired_count")
            for post in projected:
                existing = self._connection.execute(
                    "SELECT status,attempts,notification_eligible,delivery_mode,send_started_at,visibility_reason,batch_id "
                    "FROM posts WHERE post_id=?", (post.post_id,)).fetchone()
                if existing:
                    body_status = getattr(getattr(post, "body", None), "status", "missing")
                    if post.post_id in registry and (post.top != 0 or not self._reward_category_ok(post)
                            or body_status in ("invalid", "unknown_format", "unverified")):
                        retire(post.post_id, "deep_candidate_contract_lost", "cancelled_count")
                    entry = registry.get(post.post_id)
                    if (post.post_id in promoted_ids and entry is not None
                            and existing["status"] == "older_discovered" and not existing["notification_eligible"]
                            and existing["attempts"] == 0 and existing["delivery_mode"] == ""
                            and existing["send_started_at"] is None and existing["batch_id"] == entry["first_run_id"]
                            and existing["visibility_reason"] == "deep_first_seen_candidate"):
                        self._connection.execute(
                            "UPDATE posts SET status='pending',notification_eligible=1,"
                            "notification_reason='deep_candidate_entered_main_window',"
                            "visibility_reason='deep_candidate_entered_main_window',visibility_previous_run_id=?,"
                            "confirmation_since=?,confirmation_observed_at=?,summary=?,url=? WHERE post_id=?",
                            (previous_run, previous_at, observed_completed_at, post.summary, post.url, post.post_id),
                        )
                        self._insert_reward_body(post, eligible=True, observed_at=observed_completed_at,
                                                 body_wait_seconds=body_wait_seconds)
                        registry.pop(post.post_id)
                        outcome["promoted_count"] += 1
                        event(post.post_id, "deep_candidate_entered_main_window")
                        new_posts.append(post)
                    self._refresh_existing_reward(post, observed_completed_at)
                    continue
                category_ok = self._reward_category_ok(post)
                # Pins are known silently even if a reliable category field is
                # present: repinning is not evidence of first ordinary visibility.
                confirmed = not baseline and post.post_id in confirmed_ids and category_ok and post.top == 0
                degraded = not baseline and post.post_id in degraded_ids and category_ok and post.top == 0
                eligible = confirmed or degraded
                state = "baseline" if baseline else "newly_observed_confirmed" if confirmed else "uncertain_first_seen"
                reason = "baseline_snapshot" if baseline else "complete_visibility_difference" if confirmed else "degraded_window_coverage" if degraded else safe_reasons.get(post.post_id, "coverage_or_category_unverified")
                if not baseline and (not category_ok or post.top != 0):
                    reason = "pinned_visibility_unverified" if post.top else "category_unverified_or_not_reward"
                status = "baseline" if baseline else "pending" if eligible else "older_discovered"
                partial_origin = self._connection.execute(
                    "SELECT MIN(observed_at),MIN(run_id) FROM partial_diagnostics WHERE post_id=?", (post.post_id,)
                ).fetchone()
                first_partial, first_partial_run = partial_origin
                first_seen = min(first_partial, observed_completed_at) if first_partial is not None else observed_completed_at
                if (post.post_id in deferred_ids and candidate_continuous and candidate_ttl_seconds
                        and not baseline and not eligible and category_ok and post.top == 0
                        and getattr(getattr(post, "body", None), "status", "missing") not in ("invalid", "unknown_format", "unverified")
                        and first_seen + candidate_ttl_seconds > observed_completed_at):
                    if first_partial_run is not None and first_partial_run < candidate_epoch:
                        reason = "observed_before_deep_candidate_policy"
                        event(post.post_id, reason)
                    elif len(registry) < 1000:
                        registry[post.post_id] = {"first_run_id": run_id, "first_seen_at": first_seen,
                                                 "expires_at": first_seen + candidate_ttl_seconds,
                                                 "first_page": candidate_pages[post.post_id]}
                        reason = "deep_first_seen_candidate"
                        outcome["created_count"] += 1
                        event(post.post_id, reason)
                    else:
                        reason = "deep_candidate_limit_reached"
                        event(post.post_id, reason)
                self._connection.execute(
                    "INSERT INTO posts(post_id,created_at,top,source_school,first_seen_at,batch_id,status,summary,url,notification_eligible,"
                    "confirmation_since,confirmation_observed_at,notification_reason,visibility_state,visibility_reason,visibility_previous_run_id,"
                    "visibility_confirmed_run_id,visibility_confirmed_at,last_seen_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (post.post_id, post.created_at, post.top, post.source_school, first_seen, run_id, status,
                     post.summary if eligible else "", post.url if eligible else None, int(eligible), previous_at,
                     observed_completed_at, reason, state, reason, previous_run, run_id if confirmed else None,
                     observed_completed_at if confirmed else None, observed_completed_at),
                )
                self._insert_reward_body(post, eligible=eligible, observed_at=observed_completed_at, body_wait_seconds=body_wait_seconds)
                if eligible:
                    new_posts.append(post)
                elif not baseline:
                    suppressed += 1
            self._set_state("deep_window_candidates_v1", json.dumps(registry))
            if candidate_ttl_seconds and self._state("deep_candidate_policy_v1_since_run") is None:
                self._set_state("deep_candidate_policy_v1_since_run", str(run_id))
            elif not candidate_ttl_seconds:
                self._connection.execute("DELETE FROM app_state WHERE key='deep_candidate_policy_v1_since_run'")
            if baseline:
                self._set_state("baseline_at", str(now))
                self._set_state("old_window_seconds", "0")
                self._set_state("baseline_scope", "terminal_visibility_snapshot" if snapshot_terminal else "bounded_visibility_snapshot")
                self._set_state("baseline_expansion_complete", "1")
            self._set_state("frontier", json.dumps(sorted(frontier)))
            self._set_state("confirmed_run_id", str(run_id))
            self._set_state("last_confirmed_at", str(now))
            self._connection.execute(
                "INSERT INTO visibility_snapshots(run_id,observed_at,pages,terminal,ordinary_order,absence_reference_run_id) VALUES(?,?,?,?,?,?)",
                (run_id, observed_completed_at, snapshot_pages, int(snapshot_terminal), json.dumps(snapshot_order), effective_reference),
            )
            self._set_state("visibility_snapshot", json.dumps({
                "order": snapshot_order, "terminal": snapshot_terminal, "pages": snapshot_pages,
                "run_id": run_id, "observed_at": observed_completed_at,
                "absence_reference_run_id": effective_reference,
            }))
        self.last_candidate_outcome = outcome
        return new_posts, suppressed, expanded

    def commit_observation(
        self, posts: Iterable[ForumPost], frontier: set[int], run_id: int,
        *, now: int, baseline: bool = False, old_window_seconds: int = 86400,
        silent_baseline_ids: set[int] | None = None,
        observed_completed_at: int | None = None,
        body_wait_seconds: int = 120,
    ) -> tuple[list[ForumPost], int, int]:
        self._require_lock()
        if self.discovery_policy == "visibility":
            raise StoreError("visibility databases require an explicit visibility observation proof")
        new_posts = []
        suppressed = 0
        expanded = 0
        silent_ids = silent_baseline_ids or set()
        expansion_pending = self.baseline_expansion_pending
        boundary = self.confirmation_boundary
        completed_at = now if observed_completed_at is None else observed_completed_at
        cutoff = now - old_window_seconds if baseline else (
            self.baseline_at - self.old_window_seconds
        )
        with self._connection:
            for post in sorted(posts, key=lambda p: (p.created_at, p.post_id)):
                status = "baseline" if baseline or post.post_id in silent_ids else (
                    "pending" if post.created_at >= cutoff else "older_discovered"
                )
                eligible = status == "pending" and boundary <= post.created_at <= completed_at
                category_ok = getattr(post, "source_category", None) == 14 and getattr(
                    post, "category_evidence", "unknown"
                ) in ("server_filter", "response_field", "fixture_scope")
                # A pinned section may be global even on a filtered list. Until
                # a reliable per-post field verifies it, do not notify a pin.
                if self.target_category == 14 and post.top == 1 and getattr(post, "category_evidence", "unknown") != "response_field":
                    category_ok = False
                if self.target_category == 14:
                    eligible = eligible and category_ok
                reason = "confirmed_interval" if eligible else (
                    "future_timestamp" if status == "pending" and post.created_at > completed_at
                    else "before_confirmed_interval" if status == "pending" else status
                )
                if self.target_category == 14 and status == "pending" and not category_ok:
                    reason = "category_unverified_or_not_reward"
                result = self._connection.execute(
                    "INSERT OR IGNORE INTO posts "
                    "(post_id,created_at,top,source_school,first_seen_at,batch_id,status,"
                    "summary,url,notification_eligible,confirmation_since,confirmation_observed_at,notification_reason) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (post.post_id, post.created_at, post.top, post.source_school,
                     completed_at, run_id, status, post.summary if eligible else "",
                     post.url if eligible else None, int(eligible), boundary, completed_at, reason),
                )
                if result.rowcount:
                    if self.target_category == 14:
                        body = getattr(post, "body", None)
                        available = eligible and body is not None and body.status in ("full", "preview") and bool(body.text and body.evidence_id)
                        self._connection.execute(
                            "UPDATE posts SET source_category=?,category_evidence=?,body_text=?,body_status=?,"
                            "body_reason=?,body_source=?,body_encoding=?,body_evidence=?,body_truncated=?,body_deadline=? WHERE post_id=?",
                            (getattr(post, "source_category", None), getattr(post, "category_evidence", "unknown"),
                             body.text if available else "", body.status if body else "missing",
                             body.reason if body else "not_configured", body.source_field if body else None,
                             body.encoding if body else "unknown", body.evidence_id if body else None,
                             int(body.truncated) if body else 0, completed_at + body_wait_seconds, post.post_id),
                        )
                    if status == "pending":
                        new_posts.append(post)
                    elif status == "older_discovered":
                        suppressed += 1
                    elif not baseline:
                        expanded += 1
                else:
                    self._connection.execute(
                        "UPDATE posts SET top=? WHERE post_id=?", (post.top, post.post_id)
                    )
                    # Freeze optional links with the reward payload too. A send
                    # can succeed before attempts/sent bookkeeping is committed.
                    snapshot_filter = " AND delivery_mode=''" if self.target_category == 14 else ""
                    self._connection.execute(
                        "UPDATE posts SET summary=CASE WHEN summary='' THEN ? ELSE summary END, "
                        "url=CASE WHEN url IS NULL THEN ? ELSE url END "
                        "WHERE post_id=? AND status='pending' AND notification_eligible=1 AND attempts=0"
                        + snapshot_filter,
                        (post.summary, post.url, post.post_id),
                    )
                    if self.target_category == 14:
                        if not category_ok:
                            self._connection.execute(
                                "UPDATE posts SET notification_eligible=0,notification_reason='category_evidence_lost' "
                                "WHERE post_id=? AND status='pending'", (post.post_id,)
                            )
                        body = getattr(post, "body", None)
                        if body is not None and body.status in ("full", "preview"):
                            self._connection.execute(
                                "UPDATE posts SET body_text=?,body_status=?,body_reason=?,body_source=?,body_encoding=?,"
                                "body_evidence=?,body_truncated=? WHERE post_id=? AND status='pending' AND "
                                "notification_eligible=1 AND (body_text='' OR (body_status='preview' AND ?='full')) "
                                "AND attempts=0 AND delivery_mode=''",
                                (body.text, body.status, body.reason, body.source_field, body.encoding,
                                 body.evidence_id, int(body.truncated), post.post_id, body.status),
                            )
            if baseline:
                self._set_state("baseline_at", str(now))
                self._set_state("old_window_seconds", str(old_window_seconds))
                self._set_state("baseline_scope", "first_page_only")
                self._set_state("baseline_expansion_complete", "0")
            elif expansion_pending:
                self._set_state("baseline_expansion_complete", "1")
                if expanded:
                    self._set_state("baseline_scope", "first_page_plus_initial_bounded_lookback")
            self._set_state("frontier", json.dumps(sorted(frontier)))
            self._set_state("confirmed_run_id", str(run_id))
            self._set_state("last_confirmed_at", str(now))
        return new_posts, suppressed, expanded

    def pending_posts(self, limit: int = 30, *, confirmed_only: bool = False,
                      require_summary: bool = False, body_policy: str = "metadata",
                      now: int | None = None, visibility_policy: str | None = None) -> list[ForumPost]:
        filters = " AND notification_eligible=1" if confirmed_only else ""
        if visibility_policy not in (None, "hold", "notify-window"):
            raise StoreError("unsupported visibility notification policy")
        if self.discovery_policy == "visibility" and visibility_policy == "hold":
            filters += " AND visibility_state='newly_observed_confirmed'"
        if require_summary:
            filters += " AND summary<>''"
        extra_fields = ""
        if self.target_category == 14:
            filters += " AND source_category=14 AND category_evidence IN ('server_filter','response_field','fixture_scope')"
            if body_policy == "wait":
                filters += f" AND (body_text<>'' OR body_deadline<={int(time.time()) if now is None else int(now)} OR delivery_mode='metadata')"
            elif body_policy == "require":
                filters += " AND body_text<>'' AND body_status IN ('full','preview') AND body_evidence IS NOT NULL AND body_encoding<>'unknown' AND delivery_mode<>'metadata'"
            elif body_policy != "metadata":
                raise StoreError("unsupported reward body policy")
            extra_fields = ",source_category,category_evidence,body_text,body_status,body_reason,body_source,body_encoding,body_evidence,body_truncated,delivery_mode"
            if self.discovery_policy == "visibility":
                extra_fields += ",visibility_state,visibility_reason,first_seen_at AS observed_at,visibility_confirmed_at AS confirmed_at,send_started_at"
        rows = self._connection.execute(
            "SELECT post_id,created_at,top,source_school,summary,url" + extra_fields + " FROM posts "
            "WHERE status='pending'" + filters + " ORDER BY created_at,post_id LIMIT ?", (limit,)
        ).fetchall()
        if self.target_category != 14:
            return [ForumPost(**dict(row)) for row in rows]
        from .body import BodyResult
        posts = []
        for row in rows:
            values = dict(row)
            body = BodyResult(
                text=values.pop("body_text"), status=values.pop("body_status"),
                reason=values.pop("body_reason"), source_field=values.pop("body_source"),
                encoding=values.pop("body_encoding") or "unknown", evidence_id=values.pop("body_evidence"),
                truncated=bool(values.pop("body_truncated")),
            )
            posts.append(RewardPost(**values, body=body))
        return posts

    def freeze_delivery(self, post_id: int, mode: str) -> str:
        self._require_lock()
        if self.target_category != 14 or mode not in ("full", "preview", "metadata"):
            raise StoreError("invalid reward notification snapshot")
        with self._connection:
            self._connection.execute(
                "UPDATE posts SET delivery_mode=? WHERE post_id=? AND status='pending' AND notification_eligible=1 "
                "AND source_category=14 AND category_evidence<>'unknown' AND delivery_mode=''",
                (mode, post_id),
            )
        row = self._connection.execute("SELECT delivery_mode FROM posts WHERE post_id=?", (post_id,)).fetchone()
        if not row or not row[0]:
            raise StoreError("reward candidate cannot be frozen")
        return str(row[0])

    def mark_delivery_started(self, post_id: int, at: int) -> None:
        self._require_lock()
        if self.discovery_policy != "visibility" or type(at) is not int or at <= 0:
            raise StoreError("send initiation requires visibility policy and a valid absolute time")
        with self._connection:
            changed = self._connection.execute(
                "UPDATE posts SET send_started_at=COALESCE(send_started_at,?) WHERE post_id=? AND status='pending' AND notification_eligible=1",
                (at, post_id),
            )
            if not changed.rowcount:
                raise StoreError("only an eligible pending notification can start delivery")

    @property
    def pending_count(self) -> int:
        return self._connection.execute(
            "SELECT COUNT(*) FROM posts WHERE status='pending'"
        ).fetchone()[0]

    def delivery_counts(self) -> dict[str, int]:
        if self.target_category == 14:
            row = self._connection.execute(
                "SELECT COALESCE(SUM(notification_eligible=1),0),COALESCE(SUM(notification_eligible=0),0),"
                "COALESCE(SUM(notification_eligible=1 AND body_text=''),0) FROM posts WHERE status='pending'"
            ).fetchone()
            result = dict(zip(("eligible_pending_count", "blocked_unconfirmed_count", "deferred_missing_summary_count"), row))
            if self.discovery_policy == "visibility":
                result["blocked_unconfirmed_count"] = self._connection.execute(
                    "SELECT COUNT(*) FROM posts WHERE visibility_state='uncertain_first_seen' AND notification_eligible=0"
                ).fetchone()[0]
            return result
        row = self._connection.execute(
            "SELECT COALESCE(SUM(notification_eligible=1),0),"
            "COALESCE(SUM(notification_eligible=0),0),"
            "COALESCE(SUM(notification_eligible=1 AND summary=''),0) "
            "FROM posts WHERE status='pending'"
        ).fetchone()
        return dict(zip(("eligible_pending_count", "blocked_unconfirmed_count", "deferred_missing_summary_count"), row))

    def body_state_counts(self) -> dict[str, int]:
        if self.target_category != 14:
            return {}
        return {row[0]: row[1] for row in self._connection.execute(
            "SELECT body_status,COUNT(*) FROM posts GROUP BY body_status"
        )}

    def confirmed_count_for_ids(self, ids: Iterable[int]) -> int:
        values = list(ids)
        if not values:
            return 0
        return sum(self._connection.execute(
            "SELECT notification_eligible FROM posts WHERE post_id=?", (value,)
        ).fetchone()[0] for value in values)

    def mark_delivered(self, post_id: int) -> None:
        self._require_lock()
        with self._connection:
            self._connection.execute(
                "UPDATE posts SET status='sent',attempts=attempts+1,last_error=NULL, "
                "notified_at=? WHERE post_id=? AND status='pending'",
                (int(time.time()), post_id),
            )

    def mark_delivery_failed(self, post_id: int, error_type: str) -> None:
        self._require_lock()
        with self._connection:
            self._connection.execute(
                "UPDATE posts SET attempts=attempts+1,last_error=? "
                "WHERE post_id=? AND status='pending'", (error_type, post_id),
            )

    def start_run(self, now: int) -> int:
        self._require_lock()
        with self._connection:
            cursor = self._connection.execute(
                "INSERT INTO runs(started_at,status) VALUES(?,'running')", (now,)
            )
        return int(cursor.lastrowid)

    def finish_run(self, run_id: int, summary: dict) -> None:
        self._require_lock()
        with self._connection:
            self._connection.execute(
                "UPDATE runs SET finished_at=?,status=?,pages=?,fetched_count=?,"
                "new_count=?,delivered_count=?,pending_count=?,suppressed_count=?,"
                "error_type=?,warnings=?,coverage=?,confirmed_new_count=?,blocked_unconfirmed_count=?,"
                "deferred_missing_summary_count=?,baseline_expanded_count=?,missing_anchor_count=? WHERE id=?",
                (int(time.time()), summary["status"], summary["pages"],
                 summary["fetched_count"], summary["new_count"],
                 summary["delivered_count"], summary["pending_count"],
                 summary["suppressed_count"], summary["error_type"],
                 json.dumps(summary["warnings"]), summary["coverage"],
                 summary.get("confirmed_new_count", 0), summary.get("blocked_unconfirmed_count", 0),
                 summary.get("deferred_missing_summary_count", 0), summary.get("baseline_expanded_count", 0),
                 summary.get("missing_anchor_count", 0), run_id),
            )
            if self.discovery_policy == "visibility":
                self._connection.execute("UPDATE runs SET observation_summary=? WHERE id=?",
                                         (json.dumps(summary, ensure_ascii=False), run_id))
                current_started = self._connection.execute("SELECT started_at FROM runs WHERE id=?", (run_id,)).fetchone()[0]
                # Retire only new passive detail, never permanent IDs, runs,
                # absence references or sent/attempt state. Fixed seven days.
                self._connection.execute(
                    "UPDATE runs SET observation_summary=json_remove(observation_summary, '$.page_observations', '$.deep_candidate_events', '$.latency_events') "
                    "WHERE started_at<? AND (observation_summary LIKE '%\"page_observations\"%' "
                    "OR observation_summary LIKE '%\"deep_candidate_events\"%' OR observation_summary LIKE '%\"latency_events\"%')",
                    (current_started - 7 * 86400,),
                )

    def status(self) -> dict:
        return {
            "discovery_policy": self.discovery_policy,
            "baseline_complete": self.baseline_complete,
            "source_scope": {"school_id": 58, "to_id": self.target_category},
            "baseline_at": self.baseline_at or None,
            "baseline_scope": self._state("baseline_scope"),
            "old_window_seconds": self.old_window_seconds,
            "baseline_expansion_complete": self._state("baseline_expansion_complete"),
            "frontier": sorted(self.frontier),
            "confirmation_boundary": self.confirmation_boundary,
            **self.delivery_counts(),
            "body_status_counts": self.body_state_counts(),
            "visibility_state_counts": self.visibility_counts(),
            "visibility_snapshot": self.visibility_snapshot,
            "partial_diagnostic_count": self.partial_diagnostic_count,
            "counts": {r[0]: r[1] for r in self._connection.execute(
                "SELECT status,COUNT(*) FROM posts GROUP BY status"
            )},
            "last_run": dict(row) if (row := self._connection.execute(
                "SELECT * FROM runs ORDER BY id DESC LIMIT 1"
            ).fetchone()) else None,
        }
