"""Opt-in fresh-key policy and round lease for production and shadow callers.

All I/O is injected. A future integration must pass the *same* RequestLedger
used for list requests. Shadow mode passes its isolated ledger and accounts for
production separately; it does not claim an atomic shared production budget.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
import math
import time

from .watch import BEIJING, RequestBudgetExceeded, parse_fresh_key


def remaining_rounds_after_current(now, poll_seconds=60):
    """Count scheduled starts before the next Beijing midnight, excluding now."""
    local = datetime.fromtimestamp(now, BEIJING)
    midnight = datetime.combine(local.date() + timedelta(days=1), datetime.min.time(), BEIJING).timestamp()
    return max(0, math.ceil((midnight - now) / poll_seconds) - 1)


@dataclass(frozen=True)
class RefreshPolicy:
    mode: str = "process"
    min_interval_seconds: int = 180
    age_threshold_seconds: int = 120
    daily_fresh_limit: int = 0
    daily_get_limit: int = 4320
    poll_seconds: int = 60
    max_list_pages: int = 3

    def __post_init__(self):
        if self.mode not in ("process", "fixed", "age", "adaptive"):
            raise ValueError("unsupported refresh policy")
        for name, lower, upper in (("min_interval_seconds", 60, 86400),
                                   ("age_threshold_seconds", 0, 86400),
                                   ("daily_fresh_limit", 0, 1440),
                                   ("daily_get_limit", 1, 4320),
                                   ("poll_seconds", 60, 86400), ("max_list_pages", 1, 3)):
            value = getattr(self, name)
            if type(value) is not int or not lower <= value <= upper:
                raise ValueError("invalid refresh policy bound")


@dataclass(frozen=True)
class BudgetView:
    used_gets: int
    used_fresh: int
    remaining_rounds_after_current: int
    required_current_pages: int = 3
    external_gets: int = 0

    def __post_init__(self):
        if any(type(x) is not int or x < 0 for x in (
                self.used_gets, self.used_fresh, self.remaining_rounds_after_current,
                self.required_current_pages, self.external_gets)):
            raise ValueError("invalid budget view")

    def required_list_reserve(self, policy):
        # Reserve *all* remaining max-page rounds, including their anchor page.
        return self.required_current_pages + self.remaining_rounds_after_current * policy.max_list_pages


def decide(policy, budget, *, now, last_attempt, headers, has_key=True):
    reserve = budget.required_list_reserve(policy)
    spare = policy.daily_get_limit - budget.used_gets - budget.external_gets - reserve
    result = {"refresh": False, "reason": "process_mode", "required_list_reserve": reserve,
              "spare_gets_after_list_reserve": spare}
    if policy.mode == "process" and has_key:
        return result
    if budget.used_fresh >= policy.daily_fresh_limit:
        return dict(result, reason="daily_fresh_cap")
    if spare < 1:
        return dict(result, reason="preserve_required_pagination")
    if last_attempt is not None and now - last_attempt < policy.min_interval_seconds:
        return dict(result, reason="cooldown")
    if not has_key:
        return dict(result, refresh=True, reason="initial_parameter_required")
    if policy.mode == "fixed":
        return dict(result, refresh=True, reason="fixed_interval")
    age = headers.get("age")
    status = headers.get("eo-cache-status")
    if type(status) is list:
        # Production passive transport stores normalized states as a list;
        # standalone shadow records use one scalar. Ambiguous lists fail shut.
        status = status[0] if len(status) == 1 and type(status[0]) is str else "unknown"
    if type(age) is not int or age < 0:
        return dict(result, reason="age_unknown")
    if status is not None and status not in ("HIT", "STALE", "EXPIRED", "REVALIDATED"):
        return dict(result, reason="not_cached_evidence")
    threshold = policy.age_threshold_seconds
    if policy.mode == "adaptive":
        remaining_cap = policy.daily_fresh_limit - budget.used_fresh
        rounds = budget.remaining_rounds_after_current + 1
        # Spend more slowly when fresh slots are scarce. This is an explicit,
        # configurable heuristic, not a fitted latency guarantee.
        allowed = min(spare, remaining_cap)
        pace = max(policy.min_interval_seconds, math.ceil(rounds / allowed) * policy.poll_seconds)
        if last_attempt is not None and now - last_attempt < pace:
            return dict(result, reason="adaptive_budget_pacing", adaptive_interval_seconds=pace)
    return dict(result, refresh=age >= threshold,
                reason="cached_age_threshold" if age >= threshold else "age_below_threshold")


def reserve_fresh(ledger, policy, *, list_reserve, external_gets=0):
    """Atomic cap + reserve on the supplied existing GET ledger, before I/O.

    The production adapter can subsequently reserve its normal list requests.
    Other users of this ledger cannot race a fresh reservation past its caps.
    external_gets is only a conservative snapshot, not an atomic external lock.
    """
    if any(type(v) is not int or v < 0 for v in (list_reserve, external_gets)):
        raise ValueError("invalid reserve")
    ledger.reserve_fresh(daily_fresh_limit=policy.daily_fresh_limit,
                         list_reserve=list_reserve, external_gets=external_gets,
                         total_limit=policy.daily_get_limit)


class RoundKeys:
    """Only acquire a new parameter at a round boundary; failure never retries.

    Exceptions propagate to the caller's existing stop handling. An old key is
    retained in RAM but cannot be silently used to continue a failed refresh.
    """
    def __init__(self, key, policy, ledger, fetch_parameter, *, clock=time.time):
        self.key, self.policy, self.ledger = key, policy, ledger
        self.fetch_parameter, self.clock = fetch_parameter, clock
        self._active = False
        self._last_attempt_precise = None
        self.last_decision = None
        self.last_refreshed = False
        self.last_key_changed = False

    def note_acquisition(self, at):
        self._last_attempt_precise = at

    @contextmanager
    def round(self, budget, previous_home_headers=None):
        if self._active:
            raise RuntimeError("round_already_active")
        self._active = True
        self.last_refreshed = False
        self.last_key_changed = False
        try:
            row = self.ledger.con.execute("SELECT MAX(at) FROM attempts WHERE kind='fresh_get'").fetchone()
            latest = row[0]
            if latest is not None:
                # Ledger timestamps are whole seconds. Preserve subsecond
                # cooldown in this controller; after restart use the safe end
                # of the recorded second, never refresh a fraction too early.
                latest = (self._last_attempt_precise if self._last_attempt_precise is not None
                          and int(self._last_attempt_precise) == latest else latest + 1)
            self.last_decision = decide(self.policy, budget, now=self.clock(), last_attempt=latest,
                                        headers=previous_home_headers or {}, has_key=bool(self.key))
            self.last_decision["elapsed_since_last_attempt_seconds"] = (
                max(0, self.clock() - latest) if latest is not None else None)
            if self.last_decision["refresh"]:
                try:
                    reserve_fresh(self.ledger, self.policy,
                                  list_reserve=self.last_decision["required_list_reserve"],
                                  external_gets=budget.external_gets)
                except RequestBudgetExceeded:
                    # Another ledger client may have used the previously free
                    # slot. Preserve all list pages and continue with old key.
                    self.last_decision.update(refresh=False, reason="budget_changed_preserve_pagination")
                    if not self.key:
                        raise
                    yield self.key
                    return
                self._last_attempt_precise = self.clock()
                try:
                    fresh = parse_fresh_key(self.fetch_parameter())
                except Exception:
                    self.last_decision["reason"] = "fresh_request_failed_stop"
                    raise
                self.last_refreshed = True
                self.last_key_changed = fresh != self.key
                self.key = fresh  # Update only after successful exact parsing.
                self.last_decision["reason"] = "refreshed" if self.last_key_changed else "same_key_returned"
            if not self.key:
                raise RequestBudgetExceeded("initial_parameter_unavailable")
            yield self.key
        finally:
            self._active = False
