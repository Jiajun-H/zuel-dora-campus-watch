"""Minimal, fail-closed adapters for the observed school 58 list format.

The cursor is the last ordinary post ID in the returned order. This is an
observed pagination convention, not a promise of a stable remote snapshot.
Reward requests use their own explicit scope and category-bound fresh key.
Content is opt-in through an exact, evidence-backed body specification;
legacy summary mapping remains available for the isolated all-post reader.
"""

from __future__ import annotations

import json
import math
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .content import ContentMapping, normalize_summary, validate_post_url
from .body import BodySpec, extract_body
from .body_cipher import CipherParameters
from .http import get_json
from .models import ForumPost, PostPage, RewardPost, SourceProtocolError


DORA_ENDPOINT = "https://cdn5.dolacc.com/index.php/api/Postlist/index"
SCHOOL_ID = 58
MAX_POST_ID = (1 << 63) - 1
Clock = int | float | Callable[[], int | float] | None


@dataclass(frozen=True, slots=True)
class SourceScope:
    """An explicit list-request context, separate from a post's own category.

    A category-14 server filter is evidence of membership when no verified
    per-post classification field is available. Category 0 has no such effect.
    Offline fixtures must declare this context themselves.
    """

    school_id: int = SCHOOL_ID
    to_id: int = 14

    def __post_init__(self) -> None:
        if type(self.school_id) is not int or self.school_id != SCHOOL_ID:
            raise ValueError("Only the verified school scope is supported.")
        if type(self.to_id) is not int or self.to_id not in (0, 14):
            raise ValueError("Only all-post or reward list scopes are supported.")


@dataclass(frozen=True, slots=True)
class CategoryMapping:
    """Exact, independently verified category field; never discover aliases."""

    field: str
    evidence_id: str
    require_dora_topic: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.field, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", self.field):
            raise ValueError("Category field must be one exact top-level field.")
        if not isinstance(self.evidence_id, str) or not self.evidence_id.strip():
            raise ValueError("Category mapping requires explicit verification evidence.")
        if type(self.require_dora_topic) is not bool:
            raise ValueError("Strict category contract must be an explicit boolean.")
        if self.require_dora_topic and self.field != "topic_id":
            raise ValueError("Strict Dora category contract uses only the observed topic_id field.")


def verified_reward_category(
    evidence_id: str = "local-51-53-20261012-category",
) -> CategoryMapping:
    """The category contract verified in the supplied local list/detail captures.

    Ten ordinary list rows and one matching detail expose integer topic_id=14,
    integer topic.id=14 and exact topic.name='悬赏'. No pinned row was captured.
    This function validates structures only; it performs no network request.
    """
    return CategoryMapping("topic_id", evidence_id, require_dora_topic=True)


def _scope(value: SourceScope | Mapping[str, object] | None) -> SourceScope | None:
    if value is None or isinstance(value, SourceScope):
        return value
    if not isinstance(value, Mapping) or set(value) != {"school_id", "to_id"}:
        raise SourceProtocolError("Source scope requires explicit school_id and to_id.")
    try:
        return SourceScope(value["school_id"], value["to_id"])
    except ValueError:
        raise SourceProtocolError("Source scope is unsupported.") from None


def fresh_key_request_params(
    *, target_category: int = 14, cursor: int = -1, page_size: int = 10
) -> dict[str, int | str]:
    """Describe observed fresh-key parameters; perform no network operation."""
    scope = SourceScope(to_id=target_category)
    cursor = _cursor(cursor)
    if type(page_size) is not int or not 0 < page_size <= 100:
        raise ValueError("Fresh-key page size must be a bounded integer.")
    return {"toId": scope.to_id, "scId": scope.school_id, "pageSize": page_size,
            "lastindex": cursor, "keyword": ""}


def _category(
    item: dict, scope: SourceScope | None, mapping: CategoryMapping | None
) -> tuple[int | None, str]:
    # A configured reliable response field is stronger than request context.
    # Missing or malformed classification must not inherit category 14.
    if mapping is not None:
        if mapping.require_dora_topic:
            topic = item.get("topic")
            if (
                type(item.get("topic_id")) is not int or item["topic_id"] != 14
                or not isinstance(topic, dict)
                or type(topic.get("id")) is not int or topic["id"] != 14
                or type(topic.get("name")) is not str or topic["name"] != "悬赏"
                or _integer(item.get("top"), "pinned flag") != 0
            ):
                return None, "unknown"
            return 14, "response_field"
        try:
            category = _integer(item.get(mapping.field), "response category")
        except SourceProtocolError:
            return None, "unknown"
        if not 0 <= category <= MAX_POST_ID:
            return None, "unknown"
        return category, "response_field"
    if scope is not None and scope.to_id == 14:
        return 14, "server_filter"
    return None, "unknown"


class ForumAdapter(Protocol):
    def fetch_page(self, cursor: int = -1) -> PostPage:
        """Fetch one page; the caller bounds and validates traversal."""


def _integer(value: object, field: str) -> int:
    """Normalize integers without accepting booleans, floats, or coercions."""
    if type(value) is int:
        return value
    if isinstance(value, str) and value and value.isascii() and value.isdecimal():
        if value == "0" or value[0] != "0":
            # Limit digit count before int() and before SQLite storage.
            if len(value) <= 19:
                return int(value)
    raise SourceProtocolError(f"Invalid {field}; expected a canonical decimal integer.")


def _now(clock: Clock) -> float:
    value = time.time() if clock is None else clock() if callable(clock) else clock
    if type(value) not in (int, float):
        raise SourceProtocolError("Invalid parser clock.")
    try:
        valid = math.isfinite(value) and value > 0
    except (OverflowError, TypeError, ValueError):
        valid = False
    if not valid:
        raise SourceProtocolError("Invalid parser clock.")
    return float(value)


def _cursor(value: object) -> int:
    if type(value) is not int or (value != -1 and not 0 < value <= MAX_POST_ID):
        raise SourceProtocolError("Cursor must be -1 or a positive stored post ID.")
    return value


def parse_page(
    payload: object, *, now: Clock = None, content_mapping: ContentMapping | None = None,
    source_scope: SourceScope | Mapping[str, object] | None = None,
    target_category: int | None = None, category_mapping: CategoryMapping | None = None,
    body_spec: BodySpec | None = None,
    cipher_parameters: CipherParameters | None = None,
) -> PostPage:
    """Validate the entire page before exposing its minimal metadata.

The supplied list has integer IDs/timestamps and decimal-string pinned flags.
Both exact integer encodings remain supported for legacy metadata; the strict
category contract additionally requires the observed integer topic IDs.
Missing or malformed core metadata stops the complete page.
"""
    if not isinstance(payload, dict):
        raise SourceProtocolError("Forum response must be a JSON object.")
    if _integer(payload.get("code"), "business status") != 1:
        raise SourceProtocolError("Forum business status is not successful; stop this check.")
    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("posts"), list):
        raise SourceProtocolError("Forum response data.posts must be an array.")
    if content_mapping is not None and not isinstance(content_mapping, ContentMapping):
        raise SourceProtocolError("Content mapping must be explicitly configured.")
    scope = _scope(source_scope)
    if target_category is not None:
        SourceScope(to_id=target_category)
        if scope is not None and scope.to_id != target_category:
            raise SourceProtocolError("Source scope does not match the requested category.")
    if category_mapping is not None and not isinstance(category_mapping, CategoryMapping):
        raise SourceProtocolError("Category mapping must be explicitly configured.")
    if body_spec is not None and not isinstance(body_spec, BodySpec):
        raise SourceProtocolError("Body format must be explicitly configured.")
    if cipher_parameters is not None and not isinstance(cipher_parameters, CipherParameters):
        raise SourceProtocolError("Body cipher parameters must be explicitly configured.")
    reward_projection = scope is not None or target_category == 14 or category_mapping is not None or body_spec is not None

    clock = _now(now)
    result: list[ForumPost] = []
    seen: dict[int, ForumPost] = {}
    last_ordinary: int | None = None
    for item in data["posts"]:
        if not isinstance(item, dict):
            raise SourceProtocolError("Every forum post must be a JSON object.")
        post_id = _integer(item.get("id"), "post ID")
        created_at = _integer(item.get("createtime"), "creation timestamp")
        top = _integer(item.get("top"), "pinned flag")
        if not 0 < post_id <= MAX_POST_ID:
            raise SourceProtocolError("Post ID is outside the SQLite integer range.")
        if not 0 < created_at <= clock + 300:
            raise SourceProtocolError("Creation timestamp is invalid or too far in the future.")
        if top not in (0, 1):
            raise SourceProtocolError("Pinned flag must be 0 or 1.")
        if top == 0:
            last_ordinary = post_id
        summary = ""
        url = None
        if content_mapping is not None:
            summary_field = content_mapping.summary_field
            url_field = content_mapping.url_field
            if summary_field is not None and summary_field in item:
                if not isinstance(item[summary_field], str):
                    raise SourceProtocolError("Configured summary field must contain text.")
                summary = normalize_summary(item[summary_field])
            if url_field is not None and url_field in item:
                if item[url_field] is not None and not isinstance(item[url_field], str):
                    raise SourceProtocolError("Configured post link field must contain text or null.")
                url = validate_post_url(item[url_field], content_mapping.allowed_link_hosts)
        if reward_projection:
            category, evidence = _category(item, scope, category_mapping)
            post = RewardPost(post_id, created_at, top, SCHOOL_ID, summary=summary, url=url,
                              source_category=category, category_evidence=evidence,
                              body=extract_body(item, body_spec, cipher_parameters=cipher_parameters))
        else:
            post = ForumPost(post_id, created_at, top, SCHOOL_ID, summary=summary, url=url)
        previous = seen.get(post_id)
        if previous is not None:
            if previous != post:
                raise SourceProtocolError("Repeated post ID has conflicting metadata.")
            continue
        seen[post_id] = post
        result.append(post)

    return PostPage(tuple(result), last_ordinary)


class DoraForumAdapter:
    """One authorized list request at a time, without token or cookie access."""

    def __init__(
        self,
        fresh_key: str,
        permission_confirmed: bool = False,
        timeout_seconds: float = 10,
        *,
        now: Clock = None,
        content_mapping: ContentMapping | None = None,
        target_category: int = 14,
        fresh_key_category: int | None = None,
        category_mapping: CategoryMapping | None = None,
        body_spec: BodySpec | None = None,
        cipher_parameters: CipherParameters | None = None,
        passive_diagnostics: bool = False,
    ) -> None:
        if not isinstance(fresh_key, str) or not fresh_key.strip():
            raise ValueError("A fresh key must be explicitly supplied.")
        if type(permission_confirmed) is not bool:
            raise ValueError("Access permission must be explicitly confirmed as a boolean.")
        if content_mapping is not None and not isinstance(content_mapping, ContentMapping):
            raise ValueError("Content mapping must be explicitly configured.")
        self.source_scope = SourceScope(to_id=target_category)
        if fresh_key_category is not None:
            SourceScope(to_id=fresh_key_category)
        if category_mapping is not None and not isinstance(category_mapping, CategoryMapping):
            raise ValueError("Category mapping must be explicitly configured.")
        if (
            self.source_scope.to_id == 14 and category_mapping is not None
            and not category_mapping.require_dora_topic
        ):
            raise ValueError("Live reward requests require the verified Dora category contract.")
        if body_spec is not None and not isinstance(body_spec, BodySpec):
            raise ValueError("Body format must be explicitly configured.")
        if cipher_parameters is not None and not isinstance(cipher_parameters, CipherParameters):
            raise ValueError("Body cipher parameters must be explicitly configured.")
        if type(passive_diagnostics) is not bool:
            raise ValueError("Passive diagnostics must be explicitly configured as a boolean.")
        self._fresh_key = fresh_key
        self._permission_confirmed = permission_confirmed
        self._timeout_seconds = timeout_seconds
        self._now = now
        self._content_mapping = content_mapping
        self._fresh_key_category = fresh_key_category
        self._category_mapping = category_mapping if category_mapping is not None else (
            verified_reward_category() if self.source_scope.to_id == 14 else None
        )
        self._body_spec = body_spec
        self._cipher_parameters = cipher_parameters
        self._passive_diagnostics = passive_diagnostics
        self.last_response_metadata: dict = {}

    def fetch_page(self, cursor: int = -1) -> PostPage:
        # Never carry a prior page's metadata over a failed/unauthorized request.
        self.last_response_metadata = {}
        if not self._permission_confirmed:
            raise SourceProtocolError("Forum access requires confirmed platform permission.")
        cursor = _cursor(cursor)
        if self._fresh_key_category != self.source_scope.to_id and not (
            self.source_scope.to_id == 0 and self._fresh_key_category is None
        ):
            raise SourceProtocolError("Fresh key must be explicitly bound to this category.")
        request_options = {
            "params": {"toId": self.source_scope.to_id, "scId": SCHOOL_ID, "lastindex": cursor, "freshKey": self._fresh_key},
            "timeout_seconds": self._timeout_seconds,
        }
        if self._passive_diagnostics:
            request_options["response_metadata"] = self.last_response_metadata
        payload = get_json(DORA_ENDPOINT, **request_options)
        # Preserve the legacy all-post projection for explicit category 0 calls.
        return parse_page(payload, now=self._now, content_mapping=self._content_mapping,
                          source_scope=self.source_scope if self.source_scope.to_id == 14 else None,
                          category_mapping=self._category_mapping, body_spec=self._body_spec,
                          cipher_parameters=self._cipher_parameters)


class FixtureForumAdapter:
    """Replay strict, keyed pages without making a network request."""

    def __init__(
        self,
        pages: Mapping[str | int, dict],
        *,
        now: Clock = None,
        allowed_link_hosts: tuple[str, ...] = (),
        content_mapping: ContentMapping | None = None,
        source_scope: SourceScope | Mapping[str, object] | None = None,
        target_category: int | None = None,
        category_mapping: CategoryMapping | None = None,
        body_spec: BodySpec | None = None,
        cipher_parameters: CipherParameters | None = None,
    ) -> None:
        if not isinstance(pages, Mapping):
            raise SourceProtocolError("Fixture pages must be a cursor mapping.")
        if content_mapping is not None and not isinstance(content_mapping, ContentMapping):
            raise ValueError("Content mapping must be explicitly configured.")
        self.source_scope = _scope(source_scope)
        if target_category is not None:
            SourceScope(to_id=target_category)
            if self.source_scope is not None and self.source_scope.to_id != target_category:
                raise SourceProtocolError("Fixture scope does not match the requested category.")
        if category_mapping is not None and not isinstance(category_mapping, CategoryMapping):
            raise ValueError("Category mapping must be explicitly configured.")
        if body_spec is not None and not isinstance(body_spec, BodySpec):
            raise ValueError("Body format must be explicitly configured.")
        if cipher_parameters is not None and not isinstance(cipher_parameters, CipherParameters):
            raise ValueError("Body cipher parameters must be explicitly configured.")
        self._target_category = target_category
        self._category_mapping = category_mapping
        self._body_spec = body_spec
        self._cipher_parameters = cipher_parameters
        self._pages: dict[str, dict] = {}
        for key, payload in pages.items():
            if type(key) is int:
                normalized = str(_cursor(key))
            elif isinstance(key, str):
                normalized = str(-1 if key == "-1" else _cursor(_integer(key, "fixture cursor")))
            else:
                raise SourceProtocolError("Fixture cursor key must be an integer or decimal string.")
            if normalized in self._pages:
                raise SourceProtocolError("Fixture has duplicate cursor keys.")
            self._pages[normalized] = payload
        self._now = now
        if content_mapping is not None and allowed_link_hosts:
            raise ValueError("Specify link hosts in the content mapping or fixture options, not both.")
        self._content_mapping = content_mapping if content_mapping is not None else ContentMapping("summary", "url", allowed_link_hosts)
        self.requests: list[int] = []

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        *,
        now: Clock = None,
        allowed_link_hosts: tuple[str, ...] = (),
        content_mapping: ContentMapping | None = None,
        target_category: int | None = None,
        category_mapping: CategoryMapping | None = None,
        body_spec: BodySpec | None = None,
        cipher_parameters: CipherParameters | None = None,
    ) -> FixtureForumAdapter:
        problem = False
        value: object = None
        try:
            value = json.loads(Path(path).read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeError, ValueError):
            problem = True
        if problem:
            raise SourceProtocolError("Fixture file could not be read as JSON.")
        if not isinstance(value, dict) or not isinstance(value.get("pages"), dict):
            raise SourceProtocolError("Fixture file must contain a pages object.")
        return cls(value["pages"], now=now, allowed_link_hosts=allowed_link_hosts,
                   content_mapping=content_mapping, source_scope=value.get("source_scope"),
                   target_category=target_category, category_mapping=category_mapping,
                   body_spec=body_spec, cipher_parameters=cipher_parameters)

    def fetch_page(self, cursor: int = -1) -> PostPage:
        cursor = _cursor(cursor)
        self.requests.append(cursor)
        if str(cursor) not in self._pages:
            raise SourceProtocolError("Fixture has no page for the requested cursor.")
        return parse_page(self._pages[str(cursor)], now=self._now, content_mapping=self._content_mapping,
                          source_scope=self.source_scope, target_category=self._target_category,
                          category_mapping=self._category_mapping, body_spec=self._body_spec,
                          cipher_parameters=self._cipher_parameters)
