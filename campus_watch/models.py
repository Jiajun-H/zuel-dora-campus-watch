"""Public metadata and a bounded notification payload, without author data."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from .content import normalize_summary

BEIJING = timezone(timedelta(hours=8))


class SourceProtocolError(RuntimeError):
    """A source cannot safely supply a bounded page."""


@dataclass(frozen=True, slots=True)
class ForumPost:
    post_id: int
    created_at: int
    top: int = 0
    source_school: int = 58
    summary: str = field(default="", repr=False)
    url: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        for name in ("post_id", "created_at", "top", "source_school"):
            if type(getattr(self, name)) is not int:
                raise ValueError(f"{name} must be an integer")
        if self.post_id <= 0 or self.created_at <= 0:
            raise ValueError("post ID and timestamp must be positive")
        if self.post_id > 2**63 - 1 or self.created_at > 253402271999:
            raise ValueError("post metadata is outside the supported range")
        if self.top not in (0, 1) or self.source_school != 58:
            raise ValueError("unsupported pin flag or school")
        object.__setattr__(self, "summary", normalize_summary(self.summary))
        if self.url is not None and (not isinstance(self.url, str) or not self.url):
            raise ValueError("post URL must be a non-empty string or None")

    @property
    def published_at(self) -> str:
        return datetime.fromtimestamp(self.created_at, BEIJING).isoformat()


@dataclass(frozen=True, slots=True)
class PostPage:
    posts: tuple[ForumPost, ...]
    next_cursor: int | None


@dataclass(frozen=True, slots=True)
class RewardPost(ForumPost):
    """Category provenance and body result for an explicitly scoped source."""

    source_category: int | None = None
    category_evidence: str = "unknown"
    body: object | None = field(default=None, repr=False)
    delivery_mode: str = ""
    visibility_state: str = "legacy"
    visibility_reason: str = ""
    observed_at: int | None = None
    confirmed_at: int | None = None
    send_started_at: int | None = None

    def __post_init__(self) -> None:
        super(RewardPost, self).__post_init__()
        if self.source_category is not None and (
            type(self.source_category) is not int or self.source_category < 0
        ):
            raise ValueError("invalid source category")
        if self.category_evidence not in ("unknown", "server_filter", "response_field", "fixture_scope"):
            raise ValueError("invalid category evidence")
        if self.delivery_mode not in ("", "full", "preview", "metadata"):
            raise ValueError("invalid delivery mode")
        if self.visibility_state not in ("legacy", "baseline", "observed", "newly_observed_confirmed", "uncertain_first_seen"):
            raise ValueError("invalid visibility state")
        if not isinstance(self.visibility_reason, str) or len(self.visibility_reason) > 128 or not self.visibility_reason.isascii():
            raise ValueError("invalid visibility reason")
        for value in (self.observed_at, self.confirmed_at, self.send_started_at):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError("invalid observation timestamp")
        if self.body is not None:
            from .body import BodyResult
            if not isinstance(self.body, BodyResult):
                raise ValueError("invalid body result")
