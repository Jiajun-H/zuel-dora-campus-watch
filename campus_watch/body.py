"""Evidence-configured post text projection; this module never makes requests.

An encoding choice is an assertion supplied by a reviewed mapping, not format
discovery. In particular a field called ``name`` and Base64-looking bytes alone
are not evidence of post text. Cipher parameters stay outside all projections;
AES is enabled only by an explicit, locally verified mapping. No requests occur.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
import unicodedata
import zlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from html.parser import HTMLParser

from .body_cipher import CipherParameters, decrypt_cbc


_FIELDS = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}\Z")
_EVIDENCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}\Z")
_ENCODINGS = frozenset({"unknown", "plain_utf8", "base64_utf8", "gzip_base64", "zlib_base64", "aes_cbc_base64"})
_KINDS = frozenset({"unknown", "full", "preview"})
_STATUSES = frozenset({"full", "preview", "missing", "empty", "unknown_format", "invalid", "unverified"})
_REASON = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}\Z")
_MAX_INPUT = 65_536
_MAX_DECODED_BYTES = 16_384
_BLOCK_TAGS = frozenset({"p", "div", "br", "li", "ul", "ol", "section", "article", "h1", "h2", "h3", "h4", "h5", "h6", "table", "tr", "blockquote", "pre"})
_HIDDEN_TAGS = frozenset({"script", "style", "template"})
_MOJIBAKE = ("\u00e4\u00b8", "\u00e4\u00bd", "\u00e5\u00a5", "\u00e6\u2013", "\u00e7\u0161", "\u00e8\u00af", "\u00e2\u20ac", "\u00f0\u0178")


@dataclass(frozen=True, slots=True)
class BodySpec:
    """An exact field mapping whose evidence is reviewed outside this module.

``kind`` describes source completeness. Length clipping separately sets
``truncated``; it never promotes a preview to full text. The default is closed.
"""

    field: str = "name"
    encoding: str = "unknown"
    kind: str = "unknown"
    evidence_id: str | None = None
    max_chars: int = 500

    def __post_init__(self) -> None:
        if not isinstance(self.field, str) or not _FIELDS.fullmatch(self.field):
            raise ValueError("Body field must be a simple ASCII identifier.")
        if not isinstance(self.encoding, str) or self.encoding not in _ENCODINGS:
            raise ValueError("Unsupported body encoding.")
        if not isinstance(self.kind, str) or self.kind not in _KINDS:
            raise ValueError("Unsupported body completeness kind.")
        if self.evidence_id is not None and (not isinstance(self.evidence_id, str) or not _EVIDENCE.fullmatch(self.evidence_id)):
            raise ValueError("Body evidence must be a short non-secret reference identifier.")
        if type(self.max_chars) is not int or not 1 <= self.max_chars <= 2000:
            raise ValueError("Body limit must be an integer from 1 to 2000.")


@dataclass(frozen=True, slots=True)
class BodyResult:
    text: str = field(repr=False)
    status: str
    reason: str
    source_field: str | None
    encoding: str
    truncated: bool = False
    evidence_id: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Reject corrupt restored snapshots without echoing their contents."""
        if not isinstance(self.status, str) or self.status not in _STATUSES:
            raise ValueError("Unsupported body result status.")
        if not isinstance(self.reason, str) or not _REASON.fullmatch(self.reason):
            raise ValueError("Body result reason must be a safe diagnostic identifier.")
        if self.source_field is not None and (not isinstance(self.source_field, str) or not _FIELDS.fullmatch(self.source_field)):
            raise ValueError("Invalid body result source field.")
        if not isinstance(self.encoding, str) or self.encoding not in _ENCODINGS:
            raise ValueError("Unsupported body result encoding.")
        if self.evidence_id is not None and (not isinstance(self.evidence_id, str) or not _EVIDENCE.fullmatch(self.evidence_id)):
            raise ValueError("Invalid body result evidence identifier.")
        if not isinstance(self.text, str) or len(self.text) > 2000 or type(self.truncated) is not bool:
            raise ValueError("Invalid body result text length or truncation flag.")
        available = self.status in ("full", "preview")
        if not available:
            if self.text or self.truncated:
                raise ValueError("Unavailable body result cannot contain text.")
            return
        if self.encoding == "unknown" or self.evidence_id is None or self.source_field is None or not self.text:
            raise ValueError("Available body result requires a verified source mapping.")
        if _unreadable(self.text) or _opaque_ascii(self.text) or _structured(self.text):
            raise ValueError("Available body result contains unsafe or opaque text.")
        if _clean(self.text) != self.text:
            raise ValueError("Available body result must contain cleaned text.")

    def to_report(self) -> dict[str, object]:
        """Return diagnostics without text, encoded source, or evidence values."""
        return {
            "status": self.status,
            "reason": self.reason,
            "source_field": self.source_field,
            "encoding": self.encoding,
            "truncated": self.truncated,
            "evidence_present": self.evidence_id is not None,
        }


class _TextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in _HIDDEN_TAGS:
            self.hidden.append(tag)
        elif not self.hidden and tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if not self.hidden and tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if self.hidden:
            if tag == self.hidden[-1]:
                self.hidden.pop()
            return
        if tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.hidden:
            self.parts.append(data)


def _result(spec: BodySpec | None, status: str, reason: str, *, text: str = "", truncated: bool = False) -> BodyResult:
    return BodyResult(
        text=text, status=status, reason=reason,
        source_field=spec.field if spec else None,
        encoding=spec.encoding if spec else "unknown", truncated=truncated,
        evidence_id=spec.evidence_id if spec else None,
    )


def _base64_bytes(value: str) -> bytes:
    """Use the explicitly selected standard Base64 alphabet and strict padding."""
    if not value.isascii():
        raise ValueError("Non-ASCII encoding input.")
    decoded = base64.b64decode(value, validate=True)
    if base64.b64encode(decoded).decode("ascii") != value:
        raise ValueError("Non-canonical Base64 input.")
    if len(decoded) > _MAX_DECODED_BYTES:
        raise ValueError("Body decoding size bound exceeded.")
    return decoded


def _decompress(data: bytes, *, gzip: bool) -> bytes:
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS if gzip else zlib.MAX_WBITS)
    result = decoder.decompress(data, _MAX_DECODED_BYTES + 1)
    if len(result) > _MAX_DECODED_BYTES or decoder.unconsumed_tail or not decoder.eof or decoder.unused_data:
        raise ValueError("Invalid or oversized compressed body.")
    result += decoder.flush()
    if len(result) > _MAX_DECODED_BYTES:
        raise ValueError("Body decompression size bound exceeded.")
    return result


def _opaque_ascii(value: str) -> bool:
    """Fail closed on unseparated token/ciphertext shapes; never decode them."""
    stripped = value.strip()
    if len(stripped) < 24 or not stripped.isascii():
        return False
    if re.fullmatch(r"[0-9A-Fa-f]{24,}", stripped):
        return True
    if re.fullmatch(r"[A-Za-z0-9+/_=-]+", stripped):
        return True
    return False


def _single_clean(value: str) -> str:
    value = unicodedata.normalize("NFC", value.replace("\r\n", "\n").replace("\r", "\n"))
    parser = _TextParser()
    parser.feed(value)
    parser.close()
    # Keep ZWJ for emoji sequences, ZWNJ for script spelling and variation
    # selectors (Mn). Drop bidi controls, zero-width space and binary controls.
    cleaned = "".join(
        "\n" if char == "\n" else " " if char == "\t" else
        char if char in ("\u200c", "\u200d") else
        "" if unicodedata.category(char) in ("Cc", "Cf", "Cs") else char
        for char in "".join(parser.parts)
    )
    # Strip edge whitespace without folding existing line breaks or word spaces.
    cleaned = "\n".join(line.strip() for line in cleaned.split("\n"))
    return re.sub(r"\n{3,}", "\n\n", cleaned).strip()


def _clean(value: str) -> str:
    # Make stored text safe to validate again after recovery. Nested HTML entity
    # escaping must not introduce tags on a subsequent pass.
    for _ in range(4):
        cleaned = _single_clean(value)
        if cleaned == value:
            return cleaned
        value = cleaned
    raise ValueError("Body markup normalization exceeded its bound.")


def _unreadable(value: str) -> bool:
    return "\ufffd" in value or any(unicodedata.category(char) in ("Cs", "Co") for char in value) or any(marker in value for marker in _MOJIBAKE)


def _structured(value: str) -> bool:
    if not value.lstrip().startswith(("{", "[")):
        return False
    try:
        structured = json.loads(value)
    except ValueError:
        return False
    return isinstance(structured, (dict, list))


def parse_body(value: object, spec: BodySpec | None = None, *,
               cipher_parameters: CipherParameters | None = None) -> BodyResult:
    """Parse one configured body value with status-only failures.

Tests with locally generated codec samples validate mechanisms. They do not
constitute evidence that the current forum uses a particular encoding.
"""
    if value is None:
        return _result(spec, "missing", "field_missing")
    if not isinstance(value, str):
        return _result(spec, "invalid", "field_not_text")
    if not value.strip():
        return _result(spec, "empty", "empty_source")
    if spec is None or spec.encoding == "unknown" or spec.kind == "unknown":
        return _result(spec, "unknown_format", "mapping_unconfirmed")
    if spec.evidence_id is None:
        return _result(spec, "unverified", "evidence_required")
    if len(value) > _MAX_INPUT:
        return _result(spec, "invalid", "source_size_limit")
    if spec.encoding == "aes_cbc_base64" and not isinstance(cipher_parameters, CipherParameters):
        return _result(spec, "unverified", "cipher_configuration_required")
    try:
        if spec.encoding == "plain_utf8":
            if len(value.encode("utf-8", errors="strict")) > _MAX_INPUT:
                return _result(spec, "invalid", "source_size_limit")
            decoded = value
        else:
            decoded_bytes = _base64_bytes(value)
            if spec.encoding in ("gzip_base64", "zlib_base64"):
                decoded_bytes = _decompress(decoded_bytes, gzip=spec.encoding == "gzip_base64")
            elif spec.encoding == "aes_cbc_base64":
                decoded_bytes = decrypt_cbc(decoded_bytes, cipher_parameters)
            decoded = decoded_bytes.decode("utf-8", errors="strict")
    except (ValueError, UnicodeError, binascii.Error, zlib.error):
        return _result(spec, "invalid", "decode_failed")
    if _unreadable(decoded):
        return _result(spec, "invalid", "unreadable_text")
    if _opaque_ascii(decoded):
        return _result(spec, "invalid", "opaque_token")
    if _structured(decoded):
        return _result(spec, "invalid", "structured_text")
    try:
        cleaned = _clean(decoded)
    except ValueError:
        return _result(spec, "invalid", "markup_limit")
    if not cleaned or not cleaned.strip("\u200c\u200d\ufe0e\ufe0f"):
        return _result(spec, "empty", "empty_cleaned_text")
    if _unreadable(cleaned):
        return _result(spec, "invalid", "unreadable_text")
    if _structured(cleaned):
        return _result(spec, "invalid", "structured_text")
    if _opaque_ascii(cleaned):
        return _result(spec, "invalid", "opaque_token")
    truncated = len(cleaned) > spec.max_chars
    result = cleaned[:spec.max_chars]
    if truncated:
        # Avoid leaving joiners / half of a variation sequence at an excerpt edge.
        result = result.rstrip("\u200c\u200d\ufe0e\ufe0f").rstrip()
    if not result:
        return _result(spec, "empty", "empty_excerpt")
    try:
        return _result(spec, spec.kind, "configured_text", text=result, truncated=truncated)
    except ValueError:
        return _result(spec, "invalid", "unsafe_excerpt")


def extract_body(item: Mapping[str, object], spec: BodySpec | None = None, *,
                 cipher_parameters: CipherParameters | None = None) -> BodyResult:
    """Project one exact field. Unknown fields and aliases are never guessed."""
    if not isinstance(item, Mapping):
        return _result(spec, "invalid", "post_not_mapping")
    if spec is None:
        return _result(None, "unknown_format", "mapping_unconfirmed")
    return parse_body(item.get(spec.field), spec, cipher_parameters=cipher_parameters)
