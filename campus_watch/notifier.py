"""Bark delivery with bounded summaries, validated links and safe failures."""

from __future__ import annotations

import ipaddress
import math
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Protocol
from urllib.parse import unquote, urlsplit, urlunsplit

from .content import normalize_summary, validate_post_url
from .http import HttpRequestError, get_json
from .models import ForumPost, RewardPost


BEIJING = timezone(timedelta(hours=8))


class Notifier(Protocol):
    def send(self, post: ForumPost) -> None:
        """Deliver once, or raise an exception so the caller keeps it pending."""


class PushDeliveryError(RuntimeError):
    """Safe delivery failure; never contains credentials or the response body."""


class MissingSummary(PushDeliveryError):
    """A required post summary is absent; the caller must keep work pending."""


def _published_time(post: ForumPost) -> str:
    return datetime.fromtimestamp(post.created_at, BEIJING).isoformat(timespec="seconds")


def _server_base(server: str) -> str:
    """Accept a deployment prefix, never credentials, queries or HTTP URLs."""
    error = "BARK_SERVER must be an HTTPS base URL without credentials, query or fragment"
    if (
        not isinstance(server, str)
        or not server
        or any(ord(char) <= 32 or ord(char) >= 127 for char in server)
        or any(char in server for char in "\\?#")
    ):
        raise ValueError(error)
    try:
        parsed = urlsplit(server)
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.netloc.endswith(":")
            or (parsed.port is not None and not 1 <= parsed.port <= 65535)
        ):
            raise ValueError(error)
        hostname = parsed.hostname
        try:
            ipaddress.ip_address(hostname)
        except ValueError:
            if len(hostname) > 253 or any(
                not re.fullmatch(r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?", label)
                for label in hostname.split(".")
            ):
                raise ValueError(error)
        path = unquote(parsed.path)
        if (
            any(char in path for char in "\\\r\n\t")
            or any(segment in (".", "..") for segment in path.split("/"))
            or re.search(r"%(?![0-9a-fA-F]{2})", parsed.path)
        ):
            raise ValueError(error)
    except ValueError:
        pass
    else:
        return urlunsplit(("https", parsed.netloc, parsed.path.rstrip("/"), "", ""))
    raise ValueError(error)


@dataclass(frozen=True, slots=True)
class BarkNotifier:
    """Send JSON to ``/push`` with the device key outside the URL.

    A stable string ``id`` reduces duplicate notification cards on compatible
    Bark versions. It cannot guarantee exactly-once delivery: a server may have
    accepted a push before a timeout or lost response. Persistence belongs to
    the caller, which must retain pending work after any error.
    """

    device_key: str = field(repr=False)
    server: str = field(default="https://api.day.app", repr=False)
    timeout_seconds: float = 10.0
    require_summary: bool = field(default=False, repr=False)
    allowed_link_hosts: tuple[str, ...] = field(default=(), repr=False)
    simulation: bool = field(default=False, repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.device_key, str)
            or not self.device_key
            or len(self.device_key) > 512
            or any(ord(char) <= 32 or ord(char) >= 127 for char in self.device_key)
        ):
            raise ValueError("BARK_KEY must be a non-empty device key without whitespace")
        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not math.isfinite(self.timeout_seconds)
            or self.timeout_seconds <= 0
        ):
            raise ValueError("Bark timeout_seconds must be a positive finite number")
        if type(self.require_summary) is not bool:
            raise ValueError("Bark require_summary must be a boolean")
        if type(self.simulation) is not bool:
            raise ValueError("Bark simulation must be a boolean")
        if not isinstance(self.allowed_link_hosts, tuple) or any(
            not isinstance(host, str) or not host.strip() for host in self.allowed_link_hosts
        ):
            raise ValueError("Bark allowed_link_hosts must be a tuple of hostnames")
        object.__setattr__(self, "server", _server_base(self.server))
        object.__setattr__(
            self,
            "allowed_link_hosts",
            tuple(dict.fromkeys(host.strip().lower() for host in self.allowed_link_hosts)),
        )

    @classmethod
    def from_env(
        cls,
        *,
        timeout_seconds: float = 10.0,
        require_summary: bool = True,
        allowed_link_hosts: tuple[str, ...] = (),
    ) -> BarkNotifier:
        """Read the device key only from BARK_KEY; caller hosts override env."""
        key = os.environ.get("BARK_KEY", "").strip()
        server = os.environ.get("BARK_SERVER", "https://api.day.app").strip()
        if not allowed_link_hosts:
            allowed_link_hosts = tuple(
                host.strip()
                for host in os.environ.get("BARK_LINK_HOSTS", "").split(",")
                if host.strip()
            )
        return cls(
            device_key=key,
            server=server,
            timeout_seconds=timeout_seconds,
            require_summary=require_summary,
            allowed_link_hosts=allowed_link_hosts,
        )

    def _send_payload(self, payload: dict[str, str]) -> None:
        if self.simulation:
            payload = {**payload, "title": "【模拟】中南大校圈集成验收"}
        failed = False
        try:
            result = get_json(
                f"{self.server}/push",
                payload=payload,
                timeout_seconds=self.timeout_seconds,
            )
        except (HttpRequestError, OSError):
            failed = True
        if failed:
            raise PushDeliveryError("Bark request failed; notification remains pending")
        if not isinstance(result, dict) or type(result.get("code")) not in (int, str):
            raise PushDeliveryError("Bark response is invalid; notification remains pending")
        if result["code"] not in (200, "200"):
            raise PushDeliveryError("Bark rejected the notification; notification remains pending")

    def send(self, post: ForumPost) -> None:
        invalid_summary = False
        summary = ""
        try:
            summary = normalize_summary(post.summary, limit=240)
        except (TypeError, ValueError):
            invalid_summary = True
        if invalid_summary:
            raise PushDeliveryError("Post summary is invalid; notification remains pending")
        if self.require_summary and not summary:
            raise MissingSummary("Post summary is required; notification remains pending")

        safe_url = None
        if post.url is not None:
            safe_url = validate_post_url(post.url, self.allowed_link_hosts)
            if safe_url is None:
                raise PushDeliveryError("Post link is not allowed; notification remains pending")

        body = f"帖子 ID：{post.post_id}\n发布时间：{_published_time(post)}"
        if summary:
            body = f"发布时间：{_published_time(post)}\n摘要：{summary}\n帖子 ID：{post.post_id}"
        payload = {
            "device_key": self.device_key,
            "title": "校圈新帖",
            "body": body,
            "id": f"campus-{post.source_school}-{post.post_id}",
        }
        if safe_url is not None:
            payload["url"] = safe_url
        self._send_payload(payload)

    def send_test(self) -> None:
        """Send one opt-in connectivity message containing no forum metadata."""
        self._send_payload(
            {
                "device_key": self.device_key,
                "title": "Bark 通知测试",
                "body": "通知通道测试（不含论坛内容）。",
            }
        )


class ConsoleNotifier:
    """Print metadata for synthetic demo runs only."""

    def send(self, post: ForumPost) -> None:
        print(f"[模拟通知] 帖子 ID：{post.post_id}；发布时间：{_published_time(post)}")


class RewardBarkNotifier(BarkNotifier):
    """Category-gated delivery of verified body/preview or explicit fallback."""

    def send(self, post: ForumPost) -> None:
        if not isinstance(post, RewardPost) or post.source_category != 14 or post.category_evidence not in (
            "server_filter", "response_field", "fixture_scope"
        ) or post.source_school != 58:
            raise PushDeliveryError("Reward category is unverified; notification remains pending")
        if post.top == 1 and post.category_evidence != "response_field":
            raise PushDeliveryError("Pinned post category needs response verification; notification remains pending")
        body = post.body
        verified_text = body is not None and body.status in ("full", "preview") and bool(
            body.text and body.evidence_id and body.encoding != "unknown"
        )
        mode = post.delivery_mode or (body.status if verified_text else "metadata")
        if mode in ("full", "preview") and not verified_text:
            raise PushDeliveryError("Frozen body is unavailable; notification remains pending")
        stamp = _published_time(post)
        if mode == "metadata":
            content = "正文不可用：未取得可验证文字；本条仅显示发布时间和帖子 ID。"
        else:
            content = body.text
            if body.truncated:
                content += "\n（已按长度截断）"
        payload = {
            "device_key": self.device_key, "title": "【悬赏】",
            "body": f"{content}\n\n发布时间：{stamp}\n帖子 ID：{post.post_id}",
            "id": f"campus-58-14-{post.post_id}",
            "group": "中南大悬赏",
        }
        if post.url is not None:
            safe_url = validate_post_url(post.url, self.allowed_link_hosts)
            if safe_url is None:
                raise PushDeliveryError("Post link is not allowed; notification remains pending")
            payload["url"] = safe_url
        self._send_payload(payload)
