"""Explicit opt-in configuration for the first production refresh trial.

This loader reads a small public JSON file. It does not open a ledger or post
database, read credentials, or make requests. No path is auto-discovered.
"""

import json
from pathlib import Path


MAX_CONFIG_BYTES = 8192
ALLOWED_FIELDS = frozenset({"mode", "min_interval_seconds", "daily_fresh_limit"})


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate freshness configuration field")
        value[key] = item
    return value


def load_refresh_policy(path=None, *, daily_get_limit=4320, poll_seconds=60, max_list_pages=3):
    """Return None for unchanged process behavior, otherwise RefreshPolicy.

    Production v1 exposes only process and fixed modes. Accounting and scan
    settings come from the actual watch command, never from this file.
    """
    if path is None:
        return None
    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_CONFIG_BYTES + 1)
    if len(raw) > MAX_CONFIG_BYTES:
        raise ValueError("freshness configuration is too large")
    value = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=_unique_object)
    if type(value) is not dict or not value or set(value) - ALLOWED_FIELDS:
        raise ValueError("invalid freshness configuration fields")
    if value.get("mode") not in ("process", "fixed"):
        raise ValueError("production freshness mode must be process or fixed")
    interval = value.get("min_interval_seconds", 120)
    cap = value.get("daily_fresh_limit", 720)
    if type(interval) is not int or not 120 <= interval <= 86400:
        raise ValueError("freshness interval must be 120 through 86400 seconds")
    if type(cap) is not int or not 1 <= cap <= 720:
        raise ValueError("freshness daily cap must be 1 through 720")
    if value["mode"] == "process":
        return None
    from .freshness import RefreshPolicy
    return RefreshPolicy(mode="fixed", min_interval_seconds=interval, daily_fresh_limit=cap,
                         daily_get_limit=daily_get_limit, poll_seconds=poll_seconds,
                         max_list_pages=max_list_pages)
