"""Bounded ID observation, atomic confirmed frontiers, and retryable delivery."""

from __future__ import annotations

import time
from contextlib import nullcontext
from dataclasses import asdict, dataclass, field, replace
from typing import Callable

from .adapters import ForumAdapter
from .models import ForumPost, SourceProtocolError
from .notifier import BarkNotifier, RewardBarkNotifier, Notifier
from .storage import SQLiteStore, StoreBusy
from .http import sanitize_response_metadata


class IncompleteScan(SourceProtocolError):
    pass


@dataclass(slots=True)
class PollSummary:
    status: str
    pages: int = 0
    fetched_count: int = 0
    new_count: int = 0
    delivered_count: int = 0
    retry_failures: int = 0
    pending_count: int = 0
    suppressed_count: int = 0
    baseline_expanded_count: int = 0
    confirmed_new_count: int = 0
    eligible_pending_count: int = 0
    blocked_unconfirmed_count: int = 0
    deferred_missing_summary_count: int = 0
    body_status_counts: dict[str, int] = field(default_factory=dict)
    error_type: str | None = None
    warnings: list[str] = field(default_factory=list)
    coverage: str = "none"
    missing_anchor_count: int = 0
    previous_anchor_count: int = 0
    required_anchor_count: int = 0
    ordinary_anchor_count: int = 0
    missing_anchor_ids: list[int] = field(default_factory=list)
    anchor_quorum_used: bool = False
    new_posts: list[dict] = field(default_factory=list)
    uncertain_first_seen_count: int = 0
    confirmed_visible_count: int = 0
    degraded_eligible_count: int = 0
    partial_candidate_count: int = 0
    body_ready_count: int = 0
    text_delivered_count: int = 0
    metadata_delivered_count: int = 0
    coverage_gap: bool = False
    body_observed_count: int = 0
    body_parsed_count: int = 0
    body_invalid_count: int = 0
    body_missing_count: int = 0
    notification_policy: str = "legacy"
    window_new_count: int = 0
    window_delivered_count: int = 0
    policy_blocked_pending_count: int = 0
    deep_candidate_count: int = 0
    deep_candidate_created_count: int = 0
    deep_candidate_promoted_count: int = 0
    deep_candidate_expired_count: int = 0
    deep_candidate_cancelled_count: int = 0
    deep_candidate_events: list[dict] = field(default_factory=list)
    page_observations: list[dict] = field(default_factory=list)
    latency_events: list[dict] = field(default_factory=list)
    freshness: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


class Monitor:
    def __init__(
        self, adapter: ForumAdapter | None, store: SQLiteStore,
        notifier: Notifier | None = None, *, max_pages_per_poll: int = 3,
        lookback_pages: int = 1, old_window_seconds: int = 86400,
        max_alerts: int = 30, clock: Callable[[], float] = time.time,
        confirmed_notifications: bool | None = None,
        missing_content_policy: str | None = None, body_wait_seconds: int = 120,
        baseline_pages: int = 1, coverage_pages: int | None = None,
        max_observation_gap_seconds: int = 900, uncertain_policy: str = "hold",
        deep_candidate_ttl_seconds: int = 900, diagnostic_clock: Callable[[], float] = time.time,
    ) -> None:
        if not 1 <= max_pages_per_poll <= 5:
            raise ValueError("max_pages_per_poll must be 1..5")
        if not 0 <= lookback_pages <= 2:
            raise ValueError("lookback_pages must be 0..2")
        if not 0 <= old_window_seconds <= 30 * 86400 or not 1 <= max_alerts <= 100:
            raise ValueError("old window or notification budget is invalid")
        self.adapter, self.store, self.notifier = adapter, store, notifier
        self.visibility_mode = getattr(store, "discovery_policy", "legacy") == "visibility"
        if missing_content_policy is None:
            missing_content_policy = "require" if self.visibility_mode else "metadata"
        if missing_content_policy not in ("metadata", "wait", "require") or type(body_wait_seconds) is not int or not 1 <= body_wait_seconds <= 3600:
            raise ValueError("body fallback policy or waiting bound is invalid")
        if type(baseline_pages) is not int or not 1 <= baseline_pages <= 5:
            raise ValueError("baseline_pages must be 1..5")
        if coverage_pages is not None and (type(coverage_pages) is not int or not 1 <= coverage_pages <= 5):
            raise ValueError("coverage_pages must be 1..5")
        if type(max_observation_gap_seconds) is not int or not 1 <= max_observation_gap_seconds <= 86400:
            raise ValueError("observation gap bound must be 1..86400 seconds")
        if uncertain_policy not in ("hold", "notify-window"):
            raise ValueError("uncertain notification policy must be hold or notify-window")
        if type(deep_candidate_ttl_seconds) is not int or not 0 <= deep_candidate_ttl_seconds <= 3600:
            raise ValueError("deep candidate lifetime must be 0..3600 seconds")
        self.deep_candidate_ttl_seconds, self.diagnostic_clock = deep_candidate_ttl_seconds, diagnostic_clock
        self.baseline_pages, self.coverage_pages = baseline_pages, coverage_pages
        self.max_observation_gap_seconds, self.uncertain_policy = max_observation_gap_seconds, uncertain_policy
        self.body_policy, self.body_wait_seconds = missing_content_policy, body_wait_seconds
        self.reward_mode = store.target_category == 14
        if self.reward_mode and isinstance(notifier, BarkNotifier) and not isinstance(notifier, RewardBarkNotifier):
            raise ValueError("reward monitoring requires the reward notification backend")
        self.max_pages = max_pages_per_poll
        self.lookback_pages = lookback_pages
        self.old_window_seconds = old_window_seconds
        self.max_alerts, self.clock = max_alerts, clock
        self.confirmed_notifications = (
            isinstance(notifier, BarkNotifier) if confirmed_notifications is None
            else confirmed_notifications
        )
        if self.reward_mode:
            self.confirmed_notifications = True

    def run_once(self, command: str = "check", *, _lock_held: bool = False) -> PollSummary:
        if command not in ("init", "check", "notify", "rebaseline"):
            raise ValueError("unsupported monitor command")
        if command == "rebaseline" and not self.visibility_mode:
            raise ValueError("rebaseline is only available for visibility monitoring")
        if _lock_held and not self.store._locked:
            raise ValueError("watch execution requires the existing run lock")
        result = PollSummary(status="failed")
        if self.visibility_mode:
            result.notification_policy = self.uncertain_policy
        try:
            with nullcontext() if _lock_held else self.store.run_lock():
                now = int(self.clock())
                run_id = self.store.start_run(now)
                observation_committed = False
                self._visibility_committed = False
                self._partial_posts: dict[int, ForumPost] = {}
                self._post_pages: dict[int, int] = {}
                self._diagnostic_window_pages = 0
                try:
                    if self.store.baseline_complete and command == "check" and now < self.store.confirmation_boundary:
                        raise SourceProtocolError("local_clock_moved_before_confirmed_boundary")
                    if self.visibility_mode and command in ("init", "rebaseline", "check"):
                        self._run_visibility(command, result, run_id, now)
                        observation_committed = result.status in ("baseline", "rebaselined", "ok", "partial")
                    elif command == "init":
                        if self.store.baseline_complete:
                            result.status = "already_initialized"
                            result.warnings.append("baseline_preserved_use_check")
                        else:
                            posts, frontier = self._baseline(result)
                            self.store.commit_observation(
                                posts, frontier, run_id, now=now, baseline=True,
                                old_window_seconds=self.old_window_seconds,
                                body_wait_seconds=self.body_wait_seconds,
                            )
                            observation_committed = True
                            result.status = "baseline"
                            result.coverage = "first_page_only"
                            result.warnings.append("baseline_covers_first_page_only")
                    elif not self.store.baseline_complete:
                        raise SourceProtocolError("baseline_required_run_init")
                    elif command == "notify":
                        result.status = "ok"
                        self._deliver_pending(result)
                    else:
                        posts, frontier, silent_ids = self._scan(result)
                        observed_at = int(self.clock())
                        if observed_at < now:
                            raise SourceProtocolError("local_clock_moved_back_during_scan")
                        new, suppressed, expanded = self.store.commit_observation(
                            posts, frontier, run_id, now=now, silent_baseline_ids=silent_ids,
                            observed_completed_at=observed_at,
                            body_wait_seconds=self.body_wait_seconds,
                        )
                        observation_committed = True
                        result.new_count, result.suppressed_count = len(new), suppressed
                        result.baseline_expanded_count = expanded
                        result.confirmed_new_count = self.store.confirmed_count_for_ids(
                            post.post_id for post in new
                        )
                        result.new_posts = [{
                            "post_id": post.post_id, "created_at": post.created_at,
                            "top": post.top, "source_school": post.source_school,
                        } for post in new]
                        if suppressed:
                            result.warnings.append("older_first_seen_ids_suppressed")
                        if expanded:
                            result.warnings.append("initial_lookback_history_added_to_silent_baseline")
                        result.status = "ok"
                        self._deliver_pending(result)
                except IncompleteScan as exc:
                    result.status = "incomplete"
                    result.coverage = "uncommitted"
                    result.error_type = type(exc).__name__
                    result.warnings.append(str(exc))
                    self._record_partial(result, run_id, now, str(exc))
                except Exception as exc:
                    result.status = "failed"
                    result.error_type = type(exc).__name__
                    if not self._visibility_committed:
                        self._record_partial(result, run_id, now, type(exc).__name__)
                    if observation_committed or self._visibility_committed:
                        result.warnings.append("observation_committed_delivery_state_uncertain")
                    elif command == "notify":
                        result.warnings.append("delivery_state_uncertain_pending_preserved")
                    else:
                        result.coverage = "uncommitted"
                        result.warnings.append("operation_stopped_state_not_advanced")
                result.pending_count = self.store.pending_count
                result.body_status_counts = self.store.body_state_counts()
                for name, count in self.store.delivery_counts().items():
                    setattr(result, name, count)
                if self.visibility_mode:
                    result.deep_candidate_count = len(self.store.deep_window_candidates)
                    self._annotate_page_observations(result)
                self.store.finish_run(run_id, result.to_dict())
        except StoreBusy:
            result.status = "busy"
            result.error_type = "StoreBusy"
            result.warnings.append("another_process_active_retry_manually")
        except Exception as exc:
            result.status = "failed"
            result.error_type = type(exc).__name__
            result.warnings.append("run_finalization_failed_actual_summary_retained")
        return result

    def _record_partial(self, result: PollSummary, run_id: int, now: int, reason: str) -> None:
        """Diagnostics from an unfinished scan never become seen IDs or outbox."""
        if self.visibility_mode and self._partial_posts:
            result.partial_candidate_count = self.store.record_partial_observation(
                self._partial_posts.values(), run_id, now=max(now, int(self.clock())), reason=reason,
            )
            result.coverage_gap = True
            result.warnings.append("partial_observation_metadata_only_not_committed")

    def _window_pages(self) -> int:
        snapshot = self.store.visibility_snapshot
        configured = self.coverage_pages if self.coverage_pages is not None else (
            int(snapshot["pages"]) if snapshot is not None else self.baseline_pages
        )
        if configured > self.max_pages:
            raise IncompleteScan("visibility_window_exceeds_request_budget")
        return configured

    def _run_visibility(self, command: str, result: PollSummary, run_id: int, now: int) -> None:
        snapshot = self.store.visibility_snapshot
        if command == "init" and self.store.baseline_complete:
            result.status = "already_initialized"
            result.warnings.append("baseline_preserved_use_check")
            if snapshot is None:
                result.warnings.append("visibility_snapshot_missing_run_explicit_rebaseline")
            return
        baseline = command in ("init", "rebaseline")
        if not baseline and (not self.store.baseline_complete or snapshot is None):
            raise SourceProtocolError("visibility_baseline_required_run_init_or_explicit_rebaseline")
        requested_pages = self.baseline_pages if baseline else self._window_pages()
        if requested_pages > self.max_pages:
            raise IncompleteScan("visibility_baseline_exceeds_request_budget")
        posts, frontier, order, terminal, pages = self._visibility_scan(
            result, requested_pages, snapshot=None if baseline else snapshot,
        )
        completed_at = int(self.clock())
        if completed_at < now:
            raise SourceProtocolError("local_clock_moved_back_during_scan")
        result.body_observed_count = len(posts)
        result.body_parsed_count = sum(getattr(getattr(p, "body", None), "status", None) in ("full", "preview") for p in posts)
        result.body_invalid_count = sum(getattr(getattr(p, "body", None), "status", None) in ("invalid", "unknown_format", "unverified") for p in posts)
        result.body_missing_count = result.body_observed_count - result.body_parsed_count - result.body_invalid_count
        confirmed_ids: set[int] = set()
        uncertain_ids: set[int] = set()
        degraded_ids: set[int] = set()
        reasons: dict[int, str] = {}
        deferred_ids: set[int] = set()
        promoted_ids: set[int] = set()
        continuous = False
        candidate_supported = False
        absence_reference = run_id if terminal else None
        if not baseline:
            known = self.store.known_ids()
            unknown = {post.post_id for post in posts if post.post_id not in known}
            prior_order = list(snapshot["order"])
            continuous = (now >= int(snapshot["observed_at"]) and
                          completed_at - int(snapshot["observed_at"]) <= self.max_observation_gap_seconds)
            if not continuous:
                result.coverage_gap = True
                result.warnings.append("observation_gap_exceeds_confirmation_bound")
            # Exhausting the preceding visible history proves an unseen ID was
            # absent then. Publication timestamp and ID magnitude are irrelevant.
            prior_reference = snapshot.get("absence_reference_run_id") or (
                snapshot.get("run_id") if snapshot["terminal"] else None
            )
            # A missing old anchor breaks the chain of exhaustive coverage.
            # Stable window overlap supports only an uncertain observation,
            # even if the preceding snapshot had reached an empty tail.
            if result.anchor_quorum_used:
                prior_reference = None
            if prior_reference is not None and continuous:
                uncertain_ids = unknown & self.store.previously_observed_before_reference(prior_reference)
                confirmed_ids = unknown - uncertain_ids
                reasons = {post_id: "observed_before_absence_reference" for post_id in uncertain_ids}
                absence_reference = prior_reference
            else:
                uncertain_ids = unknown
                default_reason = "observation_gap" if not continuous else "previous_coverage_partial"
                reasons = {post_id: default_reason for post_id in uncertain_ids}
                ordinary_order = [post.post_id for post in posts if post.top == 0]
                current_anchors = [post_id for post_id in ordinary_order if post_id in set(prior_order)]
                expected_anchors = [post_id for post_id in prior_order if post_id in set(current_anchors)]
                anchor_order_stable = current_anchors == expected_anchors
                if not anchor_order_stable:
                    result.warnings.append("known_anchor_order_changed_partial_proof")
                # A known former tail that is now pinned cannot enlarge the
                # ordinary window. Shrink to the deepest still-ordinary anchor.
                old_tail = current_anchors[-1] if anchor_order_stable and current_anchors else None
                within_old_tail: set[int] = set()
                if old_tail in ordinary_order:
                    within_old_tail = set(ordinary_order[:ordinary_order.index(old_tail) + 1])
                for post_id in uncertain_ids - within_old_tail:
                    reasons[post_id] = "outside_previous_visible_window"
                if (
                    self.uncertain_policy == "notify-window" and continuous
                    and prior_reference is None and anchor_order_stable
                    and len(current_anchors) >= result.required_anchor_count
                    and len(current_anchors) >= 2 and old_tail in ordinary_order
                ):
                    previously_observed = self.store.previously_observed_before_reference(snapshot["run_id"])
                    degraded_ids = (uncertain_ids & within_old_tail & set(order)) - previously_observed
                    for post_id in uncertain_ids & previously_observed:
                        reasons[post_id] = "observed_before_window_reference"
                    for post_id in degraded_ids:
                        reasons[post_id] = "bounded_window_first_seen_unconfirmed"
                if uncertain_ids:
                    result.warnings.append("first_seen_visibility_uncertain_not_new_publication_proof")
            # Existing deep candidates remain explicitly uncertain even when
            # this round also has an exhaustive reference for other unknowns.
            # Every successful link in their lifetime must remain supported.
            ordinary_order = [p.post_id for p in posts if p.top == 0]
            anchors = [i for i in ordinary_order if i in set(prior_order)]
            expected = [i for i in prior_order if i in set(anchors)]
            candidate_supported = (
                self.uncertain_policy == "notify-window" and continuous
                and len(anchors) >= max(2, (3 * len(prior_order) + 3) // 4)
                and anchors == expected and bool(set(anchors) & set(order))
                and requested_pages == int(snapshot["pages"])
            )
            if candidate_supported and self.deep_candidate_ttl_seconds:
                boundary = set(ordinary_order[:ordinary_order.index(anchors[-1]) + 1])
                active = self.store.deep_window_candidates
                promoted_ids = set(active) & boundary & set(order)
                promoted_ids = {i for i in promoted_ids if completed_at < active[i]["expires_at"]}
                if prior_reference is None:
                    previously_observed = self.store.previously_observed_before_reference(snapshot["run_id"])
                    deferred_ids = (uncertain_ids - set(order) - previously_observed)
                    deferred_ids &= {p.post_id for p in posts if p.top == 0}
        new, suppressed, expanded = self.store.commit_visibility_observation(
            posts, frontier, run_id, now=now, observed_completed_at=completed_at,
            baseline=baseline, snapshot_order=order, snapshot_terminal=terminal,
            snapshot_pages=pages, confirmed_ids=confirmed_ids, uncertain_ids=uncertain_ids,
            uncertainty_reasons=reasons, degraded_ids=degraded_ids,
            body_wait_seconds=self.body_wait_seconds,
            absence_reference_run_id=absence_reference,
            deferred_ids=deferred_ids, promoted_ids=promoted_ids,
            candidate_ttl_seconds=self.deep_candidate_ttl_seconds if self.uncertain_policy == "notify-window" else 0,
            candidate_continuous=candidate_supported, candidate_pages=self._post_pages,
            candidate_cancel_reason="deep_candidate_coverage_interrupted" if continuous else "deep_candidate_observation_gap",
        )
        self._visibility_committed = True
        outcome = self.store.last_candidate_outcome
        for name in ("created", "promoted", "expired", "cancelled"):
            setattr(result, f"deep_candidate_{name}_count", outcome[f"{name}_count"])
        result.deep_candidate_events = outcome["events"]
        result.coverage = ("bounded_window_anchor_quorum" if result.anchor_quorum_used else
                           "terminal_visible_traversal" if terminal else "bounded_visible_window")
        if baseline:
            result.status = "rebaselined" if command == "rebaseline" else "baseline"
            result.warnings.append("baseline_is_silent_existing_delivery_states_preserved")
            if not terminal:
                result.warnings.append("baseline_window_partial_first_seen_requires_uncertainty_policy")
            return
        result.new_count, result.suppressed_count, result.baseline_expanded_count = len(new), suppressed, expanded
        actual_states = self.store.visibility_counts_for_ids(unknown)
        result.confirmed_visible_count = actual_states.get("newly_observed_confirmed", 0)
        result.uncertain_first_seen_count = actual_states.get("uncertain_first_seen", 0)
        result.degraded_eligible_count = self.store.confirmed_count_for_ids(degraded_ids | promoted_ids)
        result.window_new_count = result.degraded_eligible_count
        result.confirmed_new_count = self.store.confirmed_count_for_ids(confirmed_ids)
        result.new_posts = [{"post_id": post.post_id, "created_at": post.created_at,
                             "top": post.top, "source_school": post.source_school} for post in new]
        result.status = "ok"
        self._deliver_pending(result)

    def _visibility_scan(
        self, result: PollSummary, window_pages: int, *, snapshot: dict | None,
    ) -> tuple[list[ForumPost], set[int], list[int], bool, int]:
        """Collect a bounded snapshot with no timestamp or ID ordering gate.

        Prefer finding all previous anchors. Daily window mode can tolerate
        missing IDs using stable ordinary overlap at the existing page limit.
        A newly pinned old ID never lowers that overlap's fixed denominator.
        IDs are used only as source-provided cursors.
        Extra continuity/lookback pages are recorded as observations but do not
        silently enlarge the persisted window and next run's request budget.
        """
        required = set(snapshot["order"]) if snapshot is not None else set()
        self._diagnostic_window_pages = window_pages
        seen_anchors: set[int] = set()
        seen_cursors: set[int] = set()
        seen_ordinary: set[int] = set()
        collected: dict[int, ForumPost] = {}
        window_order: list[int] = []
        first_frontier: set[int] = set()
        cursor = -1
        overlap_page: int | None = None
        quorum_page: int | None = None

        def stable_quorum() -> bool:
            prior = list(snapshot["order"]) if snapshot is not None else []
            ordinary_order = [post.post_id for post in collected.values() if post.top == 0]
            ordinary_anchors = [post_id for post_id in ordinary_order if post_id in required]
            expected = [post_id for post_id in prior if post_id in set(ordinary_anchors)]
            result.previous_anchor_count = len(prior)
            result.required_anchor_count = max(2, (3 * len(prior) + 3) // 4) if prior else 0
            result.ordinary_anchor_count = len(ordinary_anchors)
            result.missing_anchor_count = len(required - seen_anchors)
            result.missing_anchor_ids = [post_id for post_id in prior if post_id not in seen_anchors][:100]
            split = (len(prior) + 1) // 2
            supporting = set(ordinary_anchors)
            return (
                self.uncertain_policy == "notify-window" and bool(prior)
                and len(ordinary_anchors) >= result.required_anchor_count
                and ordinary_anchors == expected
                and bool(supporting & set(prior[:split]))
                and bool(supporting & set(prior[split:]))
                and bool(supporting & set(window_order))
            )

        def accept_quorum(index: int) -> bool:
            if (stable_quorum() and quorum_page is not None
                    and index >= window_pages and index >= quorum_page + self.lookback_pages):
                result.anchor_quorum_used = True
                result.warnings.append("stable_window_overlap_missing_old_anchors")
                return True
            return False

        for index in range(1, self.max_pages + 1):
            if cursor in seen_cursors:
                raise IncompleteScan("repeated_cursor_no_visibility_state_committed")
            seen_cursors.add(cursor)
            page = self._fetch(cursor, result)
            ordinary = [post for post in page.posts if post.top == 0]
            if not page.posts:
                if page.next_cursor is not None:
                    raise IncompleteScan("empty_page_has_unverified_cursor")
                stable_quorum()
                if required - seen_anchors:
                    if accept_quorum(index):
                        # Do not create an exhaustive absence reference across
                        # an incomplete anchor link, including at an empty tail.
                        return list(collected.values()), first_frontier, window_order, False, min(index, window_pages)
                    raise IncompleteScan("visibility_anchors_missing_at_empty_tail")
                # The remote empty tail is evidence for this completed scan;
                # no claim about deleted/unapproved/private posts follows.
                return list(collected.values()), first_frontier, window_order, True, min(index, window_pages)
            if not ordinary:
                raise IncompleteScan("pinned_only_page_cursor_unknown")
            if index == 1:
                first_frontier = {post.post_id for post in ordinary}
            ordinary_ids = [post.post_id for post in ordinary]
            if len(ordinary_ids) != len(set(ordinary_ids)) or set(ordinary_ids) & seen_ordinary:
                raise IncompleteScan("ordinary_ids_repeated_across_pages")
            if page.next_cursor != ordinary[-1].post_id:
                raise IncompleteScan("cursor_missing_or_not_last_ordinary_id")
            for post in page.posts:
                self._post_pages.setdefault(post.post_id, index)
                previous = collected.get(post.post_id)
                if previous is not None and previous != post:
                    raise IncompleteScan("post_information_changed_during_visibility_scan")
                collected[post.post_id] = post
            self._partial_posts = dict(collected)
            seen_ordinary.update(ordinary_ids)
            if index <= window_pages:
                window_order.extend(ordinary_ids)
            seen_anchors.update(set(collected) & required)
            if stable_quorum() and quorum_page is None:
                quorum_page = index
            if seen_anchors == required and overlap_page is None:
                overlap_page = index
            if snapshot is None:
                if index >= window_pages:
                    return list(collected.values()), first_frontier, window_order, False, window_pages
            elif (
                index >= window_pages and overlap_page is not None
                and index >= overlap_page + self.lookback_pages
            ):
                return list(collected.values()), first_frontier, window_order, False, window_pages
            elif index == self.max_pages and accept_quorum(index):
                return list(collected.values()), first_frontier, window_order, False, window_pages
            cursor = page.next_cursor
        raise IncompleteScan("page_limit_visibility_coverage_not_completed")

    def _fetch(self, cursor: int, result: PollSummary):
        if self.adapter is None:
            raise SourceProtocolError("source_not_configured")
        scope = getattr(self.adapter, "source_scope", None)
        if scope is not None and scope.to_id != self.store.target_category:
            raise SourceProtocolError("source_and_database_category_mismatch")
        result.pages += 1
        started_at = self.diagnostic_clock()
        diagnostic = {"page_number": result.pages, "cursor": cursor,
                      "started_at": started_at, "main_window": result.pages <= self._diagnostic_window_pages,
                      "posts": [], "ordinary_order": [], "truncated": False}
        key = getattr(self.adapter, "diagnostic_fresh_key", None)
        if type(key) is int and 0 < key < 2**63:
            diagnostic["request_fresh_key"] = key
        try:
            page = self.adapter.fetch_page(cursor)
            if self.visibility_mode:
                diagnostic["posts"] = [{
                    "post_id": p.post_id, "created_at": p.created_at, "top": p.top,
                    "source_category": getattr(p, "source_category", None),
                    "category_evidence": getattr(p, "category_evidence", "unknown"),
                    "body_status": getattr(getattr(p, "body", None), "status", "missing"),
                } for p in page.posts[:200]]
                diagnostic["ordinary_order"] = [p.post_id for p in page.posts[:200] if p.top == 0]
                diagnostic["returned_count"] = len(page.posts)
                diagnostic["truncated"] = len(page.posts) > 200
                diagnostic["fetch_status"] = "ok"
        except Exception as exc:
            diagnostic["fetch_status"] = "failed"
            diagnostic["error_type"] = type(exc).__name__
            raise
        finally:
            if self.visibility_mode:
                diagnostic["finished_at"] = self.diagnostic_clock()
                diagnostic["response"] = sanitize_response_metadata(getattr(self.adapter, "last_response_metadata", {}))
                result.page_observations.append(diagnostic)
        result.fetched_count += len(page.posts)
        return page

    def _annotate_page_observations(self, result: PollSummary) -> None:
        """Add safe decisions after commit/delivery; failed scans stay distinct."""
        for page in result.page_observations:
            for item in page["posts"]:
                row = self.store._connection.execute(
                    "SELECT status,notification_eligible,visibility_reason,first_seen_at,send_started_at,notified_at "
                    "FROM posts WHERE post_id=?", (item["post_id"],)).fetchone()
                item["observation_committed"] = self._visibility_committed
                if row:
                    item.update(dict(row))
                else:
                    item["visibility_reason"] = "scan_uncommitted" if not self._visibility_committed else "not_persisted"
        for post in result.new_posts[:100]:
            row = self.store._connection.execute(
                "SELECT post_id,created_at,first_seen_at,confirmation_observed_at,send_started_at,notified_at,"
                "notification_reason FROM posts WHERE post_id=?", (post["post_id"],)).fetchone()
            if row:
                event = dict(row)
                event["creation_to_first_observation_seconds"] = row["first_seen_at"] - row["created_at"]
                event["creation_to_qualification_seconds"] = row["confirmation_observed_at"] - row["created_at"]
                event["observation_to_bark_confirmation_seconds"] = (
                    row["notified_at"] - row["first_seen_at"] if row["notified_at"] is not None else None)
                event["creation_to_qualification_over_60_seconds"] = event["creation_to_qualification_seconds"] > 60
                event["qualification_to_send_started_seconds"] = (
                    row["send_started_at"] - row["confirmation_observed_at"] if row["send_started_at"] is not None else None)
                event["send_started_to_bark_confirmation_seconds"] = (
                    row["notified_at"] - row["send_started_at"]
                    if row["notified_at"] is not None and row["send_started_at"] is not None else None)
                event["platform_publication_latency_verified"] = False
                result.latency_events.append(event)

    def _baseline(self, result: PollSummary) -> tuple[list[ForumPost], set[int]]:
        page = self._fetch(-1, result)
        frontier = {post.post_id for post in page.posts if post.top == 0}
        if not page.posts or not frontier:
            raise IncompleteScan("empty_or_pinned_only_baseline_not_initialized")
        return list(page.posts), frontier

    def _scan(self, result: PollSummary) -> tuple[list[ForumPost], set[int], set[int]]:
        required = self.store.frontier
        result.missing_anchor_count = len(required)
        if not required:
            raise IncompleteScan("confirmed_frontier_missing_manual_review_required")
        seen_anchors: set[int] = set()
        collected: dict[int, ForumPost] = {}
        seen_cursors: set[int] = set()
        seen_ordinary: set[int] = set()
        cursor = -1
        first_frontier: set[int] = set()
        overlap_page: int | None = None
        lookback_only_ids: set[int] = set()

        def completed():
            silent_ids = {
                post_id for post_id in lookback_only_ids
                if self.store.baseline_expansion_pending
                and collected[post_id].created_at < self.store.baseline_at
            }
            return list(collected.values()), first_frontier, silent_ids

        for index in range(1, self.max_pages + 1):
            if cursor in seen_cursors:
                raise IncompleteScan("repeated_cursor_no_state_committed")
            seen_cursors.add(cursor)
            page = self._fetch(cursor, result)
            ordinary = [post for post in page.posts if post.top == 0]
            if index == 1:
                first_frontier = {post.post_id for post in ordinary}
            if not page.posts:
                if overlap_page is not None:
                    result.warnings.append("empty_lookback_tail_not_proof_of_full_history")
                    result.coverage = "confirmed_frontier_with_bounded_lookback"
                    return completed()
                raise IncompleteScan("empty_page_before_frontier_manual_review_required")
            if not ordinary:
                raise IncompleteScan("pinned_only_page_cursor_unknown")
            for post in page.posts:
                previous = collected.get(post.post_id)
                if previous and previous.created_at != post.created_at:
                    raise SourceProtocolError("post_metadata_changed_during_scan")
                if previous and previous.top != post.top:
                    raise IncompleteScan("pin_changed_during_scan_retry_manually")
                if previous and (previous.summary, previous.url) != (post.summary, post.url):
                    raise SourceProtocolError("notification_content_changed_during_scan")
                if previous and self.reward_mode and (
                    getattr(previous, "source_category", None), getattr(previous, "category_evidence", "unknown")
                ) != (getattr(post, "source_category", None), getattr(post, "category_evidence", "unknown")):
                    raise SourceProtocolError("category_changed_during_scan")
                if previous and self.reward_mode and getattr(previous, "body", None) != getattr(post, "body", None):
                    raise SourceProtocolError("body_changed_during_scan")
                collected[post.post_id] = post
            ids = {post.post_id for post in ordinary}
            if ids & seen_ordinary:
                raise IncompleteScan("ordinary_ids_repeated_across_pages")
            seen_ordinary.update(ids)
            if overlap_page is not None and index > overlap_page:
                lookback_only_ids.update(ids)
            seen_anchors.update(ids & required)
            result.missing_anchor_count = len(required - seen_anchors)
            if any(a.created_at < b.created_at for a, b in zip(ordinary, ordinary[1:])):
                if "ordinary_time_order_unstable" not in result.warnings:
                    result.warnings.append("ordinary_time_order_unstable")
            if seen_anchors == required and overlap_page is None:
                overlap_page = index
            if overlap_page is not None and index >= overlap_page + self.lookback_pages:
                result.coverage = "confirmed_frontier_with_bounded_lookback"
                return completed()
            if page.next_cursor is None or page.next_cursor != ordinary[-1].post_id:
                raise IncompleteScan("cursor_missing_or_not_last_ordinary_id")
            cursor = page.next_cursor
        raise IncompleteScan("page_limit_frontier_or_lookback_not_reached_increase_budget_manually")

    def _deliver_pending(self, result: PollSummary) -> None:
        counts = self.store.delivery_counts()
        if self.visibility_mode:
            result.body_ready_count = len(self.store.pending_posts(
                1000000, confirmed_only=True, body_policy="require", visibility_policy=self.uncertain_policy))
            result.policy_blocked_pending_count = self.store.visibility_policy_blocked_count(self.uncertain_policy)
            if result.policy_blocked_pending_count:
                result.warnings.append("window_pending_held_by_strict_policy")
                result.status = "partial"
            if self.body_policy == "require" and counts["deferred_missing_summary_count"]:
                result.warnings.append("body_required_pending_preserved_no_metadata_fallback")
                result.status = "partial"
        if self.confirmed_notifications and counts["blocked_unconfirmed_count"]:
            result.warnings.append("unconfirmed_candidates_blocked_no_notification")
        if self.notifier is None:
            if self.store.pending_count:
                result.warnings.append("notifier_unconfigured_pending_preserved")
            return
        if self.confirmed_notifications and not self.reward_mode and counts["deferred_missing_summary_count"]:
            result.status = "partial"
            result.warnings.append("confirmed_posts_missing_summary_pending_preserved")
        for post in self.store.pending_posts(
            self.max_alerts, confirmed_only=self.confirmed_notifications,
            require_summary=self.confirmed_notifications and not self.reward_mode,
            body_policy=self.body_policy, now=int(self.clock()) if self.reward_mode else None,
            visibility_policy=self.uncertain_policy if self.visibility_mode else None,
        ):
            if self.reward_mode:
                body = post.body
                available = body is not None and body.status in ("full", "preview") and bool(body.text and body.evidence_id)
                mode = self.store.freeze_delivery(post.post_id, body.status if available else "metadata")
                post = replace(post, delivery_mode=mode)
                if self.visibility_mode:
                    self.store.mark_delivery_started(post.post_id, int(self.clock()))
            try:
                self.notifier.send(post)
            except Exception as exc:
                self.store.mark_delivery_failed(post.post_id, type(exc).__name__)
                result.retry_failures += 1
                result.status = "partial"
                result.warnings.append("notification_failed_pending_preserved")
                break
            else:
                self.store.mark_delivered(post.post_id)
                result.delivered_count += 1
                if self.visibility_mode and post.visibility_state == "uncertain_first_seen":
                    result.window_delivered_count += 1
                if self.reward_mode:
                    if post.delivery_mode == "metadata":
                        result.metadata_delivered_count += 1
                    else:
                        result.text_delivered_count += 1
        if len(self.store.pending_posts(1, confirmed_only=self.confirmed_notifications,
                                       require_summary=self.confirmed_notifications and not self.reward_mode,
                                       body_policy=self.body_policy, now=int(self.clock()) if self.reward_mode else None,
                                       visibility_policy=self.uncertain_policy if self.visibility_mode else None)) and result.retry_failures == 0:
            result.warnings.append("notification_budget_reached_pending_preserved")
        if self.reward_mode and self.body_policy == "wait" and counts["deferred_missing_summary_count"]:
            if self.store.pending_count:
                result.warnings.append("body_wait_is_bounded_deadline_fallback_on_next_notify")
