"""Local, explicitly configured AES-CBC decoding; no embedded parameters or I/O."""

from __future__ import annotations

import base64
import binascii
import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


@dataclass(frozen=True, slots=True)
class CipherParameters:
    # This object is passed separately from BodySpec and all post projections.
    key: bytes = field(repr=False)
    iv: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.key) is not bytes or len(self.key) not in (16, 24, 32):
            raise ValueError("Invalid local body cipher configuration.")
        if type(self.iv) is not bytes or len(self.iv) != 16:
            raise ValueError("Invalid local body cipher configuration.")

    @classmethod
    def from_mapping(cls, value: object) -> CipherParameters:
        """Only decode explicit configuration; errors contain no supplied values."""
        try:
            if not isinstance(value, dict) or set(value) != {"key_b64", "iv_b64"}:
                raise ValueError
            parts = []
            for name in ("key_b64", "iv_b64"):
                raw = value[name]
                if not isinstance(raw, str) or len(raw) > 64 or not raw.isascii():
                    raise ValueError
                decoded = base64.b64decode(raw, validate=True)
                if base64.b64encode(decoded).decode("ascii") != raw:
                    raise ValueError
                parts.append(decoded)
            return cls(*parts)
        except (ValueError, TypeError, KeyError, binascii.Error):
            raise ValueError("Invalid local body cipher configuration.") from None

    @classmethod
    def from_file(cls, path: str | Path) -> CipherParameters:
        try:
            with Path(path).open("rb") as handle:
                raw = handle.read(4097)
            if len(raw) > 4096:
                raise ValueError
            value = json.loads(raw.decode("utf-8-sig"))
        except (OSError, ValueError, UnicodeError):
            raise ValueError("Local body cipher configuration is unavailable.") from None
        return cls.from_mapping(value)

    @classmethod
    def from_env(cls) -> CipherParameters:
        return cls.from_mapping({
            "key_b64": os.environ.get("DORA_BODY_AES_KEY_B64"),
            "iv_b64": os.environ.get("DORA_BODY_AES_IV_B64"),
        })


def decrypt_cbc(data: bytes, parameters: CipherParameters) -> bytes:
    """Decrypt bounded blocks then validate PKCS7; this is not authentication."""
    if not isinstance(parameters, CipherParameters):
        raise ValueError("Local body cipher parameters are required.")
    if not data or len(data) > 16_384 or len(data) % 16:
        raise ValueError("Invalid body cipher block structure.")
    decryptor = Cipher(algorithms.AES(parameters.key), modes.CBC(parameters.iv)).decryptor()
    padded = decryptor.update(data) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    return unpadder.update(padded) + unpadder.finalize()
