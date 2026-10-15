"""Explicit summary projection and local URL checks, with no network access."""

from __future__ import annotations

import html
import re
import unicodedata
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import parse_qsl, unquote, urlsplit


_FIELD = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}\Z")
_HOST = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?\Z")
_MALFORMED_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_ENCODED_ESCAPE = re.compile(r"%[0-9A-Fa-f]{2}")
_ID_QUERY_KEYS = frozenset({"id", "pid", "postid", "post_id", "scid", "schoolid"})
_BLOCK_TAGS = frozenset({"p", "div", "br", "li", "ul", "ol", "section", "article", "h1", "h2", "h3", "h4", "h5", "h6", "table", "tr", "td", "blockquote", "pre"})


def _canonical_host(value: object) -> str | None:
    if not isinstance(value, str) or value != value.strip() or not value.isascii():
        return None
    host = value.lower()
    if not _HOST.fullmatch(host) or ".." in host:
        return None
    if any(not label or len(label) > 63 or label.startswith("-") or label.endswith("-") for label in host.split(".")):
        return None
    return host


@dataclass(frozen=True, slots=True)
class ContentMapping:
    """Exact top-level fields; no alias discovery or detail-page requests."""

    summary_field: str | None = None
    url_field: str | None = None
    allowed_link_hosts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field in (self.summary_field, self.url_field):
            if field is not None and (not isinstance(field, str) or not _FIELD.fullmatch(field)):
                raise ValueError("Content field must be a simple ASCII identifier.")
        if not isinstance(self.allowed_link_hosts, (tuple, list)):
            raise ValueError("Allowed link hosts must be an explicit hostname sequence.")
        hosts = []
        for value in self.allowed_link_hosts:
            host = _canonical_host(value)
            if host is None:
                raise ValueError("Allowed link host must be a bare ASCII hostname.")
            if host not in hosts:
                hosts.append(host)
        object.__setattr__(self, "allowed_link_hosts", tuple(hosts))


class _SummaryParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden: str | None = None

    def handle_starttag(self, tag: str, attrs) -> None:
        if self.hidden is not None:
            return
        if tag in ("script", "style", "template"):
            self.hidden = tag
        elif tag in _BLOCK_TAGS:
            self.parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if self.hidden is not None:
            if tag == self.hidden:
                self.hidden = None
            return
        if tag in _BLOCK_TAGS:
            self.parts.append(" ")

    def handle_data(self, data: str) -> None:
        if self.hidden is None:
            self.parts.append(data)


def normalize_summary(value: str, limit: int = 240) -> str:
    """Strip markup/control characters and bound plain text to 240 code points."""
    if not isinstance(value, str):
        raise ValueError("Summary must be text.")
    if type(limit) is not int or not 1 <= limit <= 240:
        raise ValueError("Summary limit must be an integer between 1 and 240.")
    parser = _SummaryParser()
    normalized = unicodedata.normalize("NFKC", value)
    parser.feed(unicodedata.normalize("NFKC", html.unescape(normalized)))
    parser.close()
    cleaned = "".join(
        " " if char.isspace() else "" if unicodedata.category(char) in ("Cc", "Cf", "Cs") else char
        for char in "".join(parser.parts)
    )
    return " ".join(cleaned.split())[:limit]


def _unsafe_url_chars(value: str) -> bool:
    return any(char.isspace() or char == "\\" or unicodedata.category(char) in ("Cc", "Cf", "Cs") for char in value)


def _has_dot_segments(value: str) -> bool:
    # Matrix-parameter suffixes can be removed by a server before routing.
    # Treat these as dot segments too rather than relying on one URL parser.
    return any(segment.partition(";")[0] in (".", "..") for segment in urlsplit(value).path.split("/"))


def validate_post_url(value: object, allowed_hosts: tuple[str, ...]) -> str | None:
    """Accept only supplied HTTPS links on exact explicit hosts; never resolve.

Query parameters are limited to positive numeric post/school IDs. Unknown
parameters, including credential or identity parameters, fail closed. This
checks a URL's shape, not whether the remote page exists or is a post page.
"""
    if not isinstance(value, str) or not value or "#" in value or _unsafe_url_chars(value):
        return None
    if value != unicodedata.normalize("NFKC", value) or _MALFORMED_ESCAPE.search(value):
        return None
    if not isinstance(allowed_hosts, (tuple, list)):
        return None
    hosts = {_canonical_host(host) for host in allowed_hosts}
    hosts.discard(None)
    if not hosts:
        return None
    try:
        # Inspect raw and encoded routing paths locally. Reject nested encoding
        # beyond the inspection bound instead of assuming a server decodes once.
        decoded = value
        for _ in range(4):
            if _unsafe_url_chars(decoded) or decoded != unicodedata.normalize("NFKC", decoded) or _MALFORMED_ESCAPE.search(decoded) or _has_dot_segments(decoded):
                return None
            next_decoded = unquote(decoded, errors="strict")
            if next_decoded == decoded:
                break
            decoded = next_decoded
        else:
            if _ENCODED_ESCAPE.search(decoded):
                return None
            if _unsafe_url_chars(decoded) or decoded != unicodedata.normalize("NFKC", decoded) or _MALFORMED_ESCAPE.search(decoded) or _has_dot_segments(decoded):
                return None
        parsed = urlsplit(value)
        if parsed.scheme != "https" or parsed.username is not None or parsed.password is not None or parsed.fragment:
            return None
        if parsed.port not in (None, 443):
            return None
        host = _canonical_host(parsed.hostname)
        if host is None or host not in hosts:
            return None
        pairs = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True, max_num_fields=8)
        keys = set()
        for key, item in pairs:
            normalized = key.lower()
            if normalized not in _ID_QUERY_KEYS or normalized in keys:
                return None
            if not item.isascii() or not item.isdecimal() or item.startswith("0") or len(item) > 19:
                return None
            keys.add(normalized)
    except (ValueError, TypeError, UnicodeError):
        return None
    return value
