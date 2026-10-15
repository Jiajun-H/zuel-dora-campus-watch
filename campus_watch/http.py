"""Single-attempt JSON HTTP with bounded reads and sanitized failures."""

from __future__ import annotations

import http.client
import hashlib
import json
import math
import re
import sys
from collections.abc import Mapping
from datetime import timezone
from email.utils import format_datetime, parsedate_to_datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class HttpRequestError(RuntimeError):
    """Safe to report: contains no request URL, credential, or response body."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_CACHE_FLAGS = frozenset({
    "public", "private", "no-cache", "no-store", "must-revalidate",
    "proxy-revalidate", "no-transform", "immutable", "only-if-cached",
    "must-understand",
})
_CACHE_NUMBERS = frozenset({
    "max-age", "s-maxage", "min-fresh", "max-stale",
    "stale-while-revalidate", "stale-if-error",
})
_MAX_HEADER_NUMBER = (1 << 31) - 1
_CACHE_STATES = frozenset({
    "HIT", "MISS", "BYPASS", "STALE", "EXPIRED", "REVALIDATED",
    "DYNAMIC", "UPDATING", "PASS", "ERROR",
})
_HTTP_DATE = re.compile(
    r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), [0-9]{2} "
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) [0-9]{4} "
    r"[0-9]{2}:[0-9]{2}:[0-9]{2} GMT"
    r"|(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), "
    r"[0-9]{2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-[0-9]{2} "
    r"[0-9]{2}:[0-9]{2}:[0-9]{2} GMT"
    r"|(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) "
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) [ 0-9][0-9] "
    r"[0-9]{2}:[0-9]{2}:[0-9]{2} [0-9]{4}"
)


def _header_number(value: str) -> int | None:
    if not value or len(value) > 10 or not value.isascii() or not value.isdecimal():
        return None
    number = int(value)
    return number if number <= _MAX_HEADER_NUMBER else None


def _http_date(value: str) -> str | None:
    if _HTTP_DATE.fullmatch(value) is None:
        return None
    try:
        stamp = parsedate_to_datetime(value)
        if value[:3] != ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")[stamp.weekday()]:
            return None
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        return format_datetime(stamp.astimezone(timezone.utc), usegmt=True)
    except (ValueError, TypeError, OverflowError):
        return None


def _cache_control(value: str) -> dict:
    """Retain only standardized flags/numbers, never directive arguments."""
    result: dict[str, bool | int] = {}
    conflicts: set[str] = set()
    # Commas inside quoted field-name lists are not directive boundaries.
    parts = []
    start = 0
    quoted = escaped = False
    for index, char in enumerate(value):
        if escaped:
            escaped = False
        elif char == "\\" and quoted:
            escaped = True
        elif char == '"':
            quoted = not quoted
        elif char == "," and not quoted:
            parts.append(value[start:index])
            start = index + 1
    if quoted or escaped:
        return {}
    parts.append(value[start:])
    for part in parts:
        name, equal, argument = part.strip().partition("=")
        name = name.lower()
        candidate: bool | int | None = None
        if name in _CACHE_FLAGS and (not equal or name in {"private", "no-cache"}):
            candidate = True
        elif name == "max-stale" and not equal:
            candidate = True
        elif name in _CACHE_NUMBERS and equal:
            candidate = _header_number(argument.strip())
        if name in _CACHE_FLAGS | _CACHE_NUMBERS and candidate is None:
            # A malformed duplicate makes that directive ambiguous; do not
            # retain an earlier or later valid value as cache evidence.
            result.pop(name, None)
            conflicts.add(name)
        if candidate is not None and name not in conflicts:
            if name in result and (type(result[name]) is not type(candidate) or result[name] != candidate):
                result.pop(name)
                conflicts.add(name)
            else:
                result[name] = candidate
    return result


def _capture_response_metadata(target: dict | None, status: object, getter) -> None:
    """Best-effort whitelist only; diagnostics cannot change request behavior.

    Deliberately neither inspect arbitrary headers nor keep their raw values.
    A hostile/malformed diagnostic header must not raise a transport error.
    """
    if type(target) is not dict:
        return
    if type(status) is int and 100 <= status <= 599:
        target["http_status"] = status
    safe: dict = {}
    for name in ("Date", "Age", "Cache-Control", "Expires", "X-Cache", "Via", "ETag"):
        try:
            value = getter(name)
            if type(value) is not str or not value.strip():
                continue
            # Presence alone is sufficient when a value cannot safely be hashed.
            if name in {"Via", "ETag"}:
                evidence = {"present": True}
                if len(value) <= 4096:
                    evidence["sha256"] = hashlib.sha256(value.encode("utf-8")).hexdigest()
                safe[name.lower()] = evidence
                continue
            if len(value) > 4096 or any(ord(char) < 32 or ord(char) == 127 for char in value):
                continue
            value = value.strip()
            if name in {"Date", "Expires"}:
                normalized = _http_date(value)
            elif name == "Age":
                normalized = _header_number(value)
            elif name == "Cache-Control":
                normalized = _cache_control(value) or None
            else:
                states = []
                for token in re.findall(r"[A-Za-z][A-Za-z0-9_-]*", value.upper()):
                    if token in {"TCP_HIT", "TCP_MISS", "TCP_REFRESH_HIT"}:
                        token = "MISS" if token == "TCP_MISS" else "HIT"
                    if token in _CACHE_STATES and token not in states:
                        states.append(token)
                normalized = states or None
            if normalized is not None:
                safe[name.lower()] = normalized
        except Exception:
            # Never export the original exception or let optional diagnostics
            # interfere with the existing authorization/HTTP/parser contract.
            continue
    if safe:
        target["headers"] = safe


def sanitize_response_metadata(metadata: object) -> dict:
    """Copy already-normalized evidence into a strictly bounded public shape.

    The monitor can apply this at the persistence boundary even to fixture or
    wrapper adapters. Raw header strings, arbitrary nested values and unknown
    fields are never accepted as normalized diagnostics.
    """
    if type(metadata) is not dict:
        return {}
    result = {}
    status = metadata.get("http_status")
    if type(status) is int and 100 <= status <= 599:
        result["http_status"] = status
    headers = metadata.get("headers")
    if type(headers) is not dict:
        return result
    safe = {}
    for name in ("date", "age", "cache-control", "expires", "x-cache", "via", "etag"):
        value = headers.get(name)
        normalized = None
        if name in {"date", "expires"} and type(value) is str and len(value) <= 128:
            normalized = _http_date(value)
        elif name == "age" and type(value) is int and 0 <= value <= _MAX_HEADER_NUMBER:
            normalized = value
        elif name == "cache-control" and type(value) is dict:
            directives = {}
            for directive in _CACHE_FLAGS | _CACHE_NUMBERS:
                item = value.get(directive)
                if (directive in _CACHE_FLAGS or directive == "max-stale") and item is True:
                    directives[directive] = True
                elif directive in _CACHE_NUMBERS and type(item) is int and 0 <= item <= _MAX_HEADER_NUMBER:
                    directives[directive] = item
            normalized = directives or None
        elif name == "x-cache" and type(value) is list and len(value) <= len(_CACHE_STATES):
            states = []
            for item in value:
                if type(item) is str and item in _CACHE_STATES and item not in states:
                    states.append(item)
            normalized = states or None
        elif name in {"via", "etag"} and type(value) is dict and value.get("present") is True:
            evidence = {"present": True}
            digest = value.get("sha256")
            if type(digest) is str and re.fullmatch(r"[0-9a-fA-F]{64}", digest):
                evidence["sha256"] = digest.lower()
            normalized = evidence
        if normalized is not None:
            safe[name] = normalized
    if safe:
        result["headers"] = safe
    return result


def _reject_constant(value: str) -> None:
    raise ValueError("Nonstandard JSON number.")


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object key.")
        result[key] = value
    return result


def get_json(
    url: str,
    *,
    params: Mapping | None = None,
    payload: dict | None = None,
    timeout_seconds: float = 10,
    max_bytes: int = 2_000_000,
    response_metadata: dict | None = None,
) -> dict:
    """Make exactly one request; reject redirects instead of forwarding secrets.

    This transport grants no permission to contact a platform. Callers must gate
    real endpoints. HTTP, network, and parsing failures never include the URL,
    query parameters, payload, server error text, or underlying exception.
    Optional response metadata contains only normalized cache-header evidence;
    it neither adds requests nor changes the request's headers or cache policy.
    """
    if type(response_metadata) is dict:
        response_metadata.clear()
    valid_timeout = False
    if type(timeout_seconds) in (int, float):
        try:
            valid_timeout = math.isfinite(timeout_seconds) and timeout_seconds > 0
        except (OverflowError, ValueError):
            pass
    if not valid_timeout:
        raise HttpRequestError("HTTP timeout must be finite and positive.")
    if type(max_bytes) is not int or not 1 <= max_bytes < sys.maxsize:
        raise HttpRequestError("HTTP response limit must be a positive integer.")
    if payload is not None and not isinstance(payload, dict):
        raise HttpRequestError("HTTP JSON payload must be an object.")

    request = None
    problem = False
    try:
        parsed = urlsplit(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username is not None or parsed.password is not None:
            problem = True
        else:
            # Access .port here so malformed ports fail before opening a socket.
            parsed.port
            query = parsed.query
            if params is not None:
                if not isinstance(params, Mapping):
                    problem = True
                else:
                    encoded = urlencode(params)
                    query = "&".join(part for part in (query, encoded) if part)
            if not problem:
                target = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))
                body = None if payload is None else json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
                headers = {"Accept": "application/json", "User-Agent": "CampusWatch/0.2"}
                if body is not None:
                    headers["Content-Type"] = "application/json; charset=utf-8"
                request = Request(target, data=body, headers=headers, method="GET" if body is None else "POST")
    except (ValueError, TypeError, UnicodeError, AttributeError, RecursionError, OverflowError):
        problem = True
    if problem or request is None:
        raise HttpRequestError("Invalid HTTP request configuration.")

    failure = None
    raw = b""
    try:
        with build_opener(_NoRedirect()).open(request, timeout=float(timeout_seconds)) as response:
            status = response.status
            if type(response_metadata) is dict:
                _capture_response_metadata(response_metadata, status, lambda name: response.getheader(name))
            if not 200 <= status < 300:
                failure = f"HTTP status {status}; request stopped without retry."
            else:
                declared_length = response.getheader("Content-Length")
                length = None
                if declared_length is not None:
                    declared_length = declared_length.strip()
                    if not declared_length.isascii() or not declared_length.isdecimal():
                        failure = "HTTP response had an invalid declared length."
                    else:
                        length = int(declared_length)
                        if length > max_bytes:
                            failure = "HTTP response exceeded the configured size limit."
                if failure is None:
                    raw = response.read(max_bytes + 1)
                    if length is not None and len(raw) != length:
                        failure = "HTTP response ended before its declared length."
    except HTTPError as exc:
        code = exc.code
        if type(response_metadata) is dict:
            _capture_response_metadata(response_metadata, code, lambda name: exc.headers.get(name) if exc.headers is not None else None)
        exc.close()
        if code in (401, 403, 429):
            failure = f"HTTP {code} refused or limited access; stop without retry."
        elif code in (301, 302, 303, 307, 308):
            failure = "HTTP redirect refused; request stopped without retry."
        else:
            failure = f"HTTP status {code}; request stopped without retry."
    except (URLError, TimeoutError, OSError, http.client.HTTPException, ValueError, UnicodeError):
        failure = "HTTP transport failed; request stopped without retry."
    # Raise outside the except suite: no original URL-bearing exception is
    # retained as __cause__ or __context__ on the user-visible error.
    if failure is not None:
        raise HttpRequestError(failure)
    if len(raw) > max_bytes:
        raise HttpRequestError("HTTP response exceeded the configured size limit.")

    value: object = None
    problem = False
    try:
        value = json.loads(raw.decode("utf-8-sig"), parse_constant=_reject_constant, object_pairs_hook=_unique_object)
    except (UnicodeError, ValueError, RecursionError):
        problem = True
    if problem:
        raise HttpRequestError("HTTP response was not valid unambiguous JSON.")
    if not isinstance(value, dict):
        raise HttpRequestError("HTTP JSON response must be an object.")
    return value
