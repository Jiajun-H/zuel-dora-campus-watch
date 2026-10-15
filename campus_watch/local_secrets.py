"""CurrentUser Windows DPAPI storage for one local Bark device key.

Only encrypted bytes are written. The launcher supplies data/bark_key.dpapi;
tests use the same filename under a temporary directory. This module performs
no network requests and never reports credentials, paths, or OS error text.
"""

from __future__ import annotations

import ctypes
import os
import re
import stat
import tempfile
from contextlib import contextmanager
from ctypes import wintypes
from pathlib import Path
from urllib.parse import urlsplit


_FILENAME = "bark_key.dpapi"
_MAGIC = b"CampusWatch-Bark-DPAPI\x00\x01\n"
_PLAIN_MAGIC = b"CampusWatch-Bark-Key\x00\x01\n"
_ENTROPY = b"CampusWatch:local-bark-key:v1"
_MAX_BYTES = 65_536
_KEY = re.compile(r"[A-Za-z0-9_-]{1,512}\Z")
_REPARSE_POINT = 0x400
_UI_FORBIDDEN = 0x1


class LocalSecretError(RuntimeError):
    """Safe credential error without key, path, or an underlying error chain."""


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_ubyte))]


def normalize_bark_key(value: str) -> str:
    """Accept a bare device key or one HTTPS api.day.app key path only."""
    invalid = not isinstance(value, str) or len(value) > 2048
    key = ""
    if not invalid:
        invalid = any(ord(char) < 32 or ord(char) >= 127 for char in value)
    if not invalid:
        candidate = value.strip()
        if _KEY.fullmatch(candidate):
            return candidate
        try:
            parsed = urlsplit(candidate)
            if (
                parsed.scheme == "https"
                and parsed.netloc.lower() == "api.day.app"
                and parsed.username is None
                and parsed.password is None
                and "?" not in candidate
                and "#" not in candidate
                and parsed.path.startswith("/")
                and parsed.path.count("/") == 1
            ):
                key = parsed.path[1:]
        except (ValueError, TypeError, UnicodeError):
            pass
        if _KEY.fullmatch(key):
            return key
    raise LocalSecretError("Bark key must be a device key or a single HTTPS api.day.app key URL.")


def _dpapi(value: bytes, *, decrypt: bool) -> bytes:
    if os.name != "nt":
        raise LocalSecretError("Local Bark key storage requires Windows CurrentUser DPAPI.")
    failed = False
    result = b""
    value_buffer = ctypes.create_string_buffer(value)
    entropy_buffer = ctypes.create_string_buffer(_ENTROPY)
    source = _DataBlob(len(value), ctypes.cast(value_buffer, ctypes.POINTER(ctypes.c_ubyte)))
    entropy = _DataBlob(len(_ENTROPY), ctypes.cast(entropy_buffer, ctypes.POINTER(ctypes.c_ubyte)))
    output = _DataBlob()
    kernel32 = None
    try:
        crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.LocalFree.argtypes = [ctypes.c_void_p]
        kernel32.LocalFree.restype = ctypes.c_void_p
        if decrypt:
            operation = crypt32.CryptUnprotectData
            operation.argtypes = [
                ctypes.POINTER(_DataBlob), ctypes.POINTER(ctypes.c_wchar_p),
                ctypes.POINTER(_DataBlob), ctypes.c_void_p, ctypes.c_void_p,
                wintypes.DWORD, ctypes.POINTER(_DataBlob),
            ]
            description = None
        else:
            operation = crypt32.CryptProtectData
            operation.argtypes = [
                ctypes.POINTER(_DataBlob), wintypes.LPCWSTR,
                ctypes.POINTER(_DataBlob), ctypes.c_void_p, ctypes.c_void_p,
                wintypes.DWORD, ctypes.POINTER(_DataBlob),
            ]
            description = "Campus Watch local Bark device key"
        operation.restype = wintypes.BOOL
        # No CRYPTPROTECT_LOCAL_MACHINE: only this Windows user's DPAPI context.
        if not operation(
            ctypes.byref(source), description, ctypes.byref(entropy),
            None, None, _UI_FORBIDDEN, ctypes.byref(output),
        ):
            failed = True
        elif not output.pbData or not 0 < output.cbData <= _MAX_BYTES:
            failed = True
        else:
            result = ctypes.string_at(output.pbData, output.cbData)
    except (OSError, AttributeError, TypeError, ValueError, OverflowError):
        failed = True
    finally:
        ctypes.memset(value_buffer, 0, ctypes.sizeof(value_buffer))
        if output.pbData and kernel32 is not None:
            if 0 < output.cbData <= _MAX_BYTES:
                ctypes.memset(output.pbData, 0, output.cbData)
            kernel32.LocalFree(ctypes.cast(output.pbData, ctypes.c_void_p))
    if failed:
        raise LocalSecretError("Windows local credential protection failed.")
    return result


def _protect(value: bytes) -> bytes:
    return _dpapi(value, decrypt=False)


def _unprotect(value: bytes) -> bytes:
    return _dpapi(value, decrypt=True)


def _has_reparse(info: os.stat_result) -> bool:
    return stat.S_ISLNK(info.st_mode) or bool(getattr(info, "st_file_attributes", 0) & _REPARSE_POINT)


def _checked_path(path: Path, *, create_parent: bool = False) -> Path:
    if not isinstance(path, Path) or path.name != _FILENAME or ".." in path.parts:
        raise LocalSecretError("Local credential path is not the supported credential file.")
    absolute = Path(os.path.abspath(path))
    for directory in reversed(absolute.parents):
        info = None
        try:
            info = directory.lstat()
        except FileNotFoundError:
            if create_parent:
                directory.mkdir(mode=0o700)
                info = directory.lstat()
        if info is not None and (_has_reparse(info) or not stat.S_ISDIR(info.st_mode)):
            raise LocalSecretError("Local credential directory is not a regular directory.")
    return absolute


class _AttributeTagInfo(ctypes.Structure):
    _fields_ = [("FileAttributes", wintypes.DWORD), ("ReparseTag", wintypes.DWORD)]


@contextmanager
def _locked_parent(path: Path, *, create_parent: bool = False):
    """Keep Windows parent directories from being renamed during the operation."""
    absolute = _checked_path(path)
    if os.name != "nt":
        yield _checked_path(path, create_parent=create_parent)
        return
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateFileW.argtypes = [
        wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
        wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
    ]
    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.GetFileInformationByHandleEx.argtypes = [
        wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD,
    ]
    kernel32.GetFileInformationByHandleEx.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handles = []
    try:
        for directory in reversed(absolute.parents):
            try:
                directory.lstat()
            except FileNotFoundError:
                if not create_parent:
                    yield None
                    return
                directory.mkdir(mode=0o700)
            # READ_ATTRIBUTES, SHARE_READ|SHARE_WRITE (no SHARE_DELETE),
            # OPEN_EXISTING, BACKUP_SEMANTICS|OPEN_REPARSE_POINT.
            handle = kernel32.CreateFileW(str(directory), 0x80, 0x3, None, 3, 0x02200000, None)
            if handle in (None, ctypes.c_void_p(-1).value):
                raise LocalSecretError("Local credential directory could not be secured.")
            handles.append(handle)
            details = _AttributeTagInfo()
            if not kernel32.GetFileInformationByHandleEx(handle, 9, ctypes.byref(details), ctypes.sizeof(details)):
                raise LocalSecretError("Local credential directory could not be inspected.")
            if details.FileAttributes & _REPARSE_POINT or not details.FileAttributes & 0x10:
                raise LocalSecretError("Local credential directory is not a regular directory.")
        yield absolute
    finally:
        for handle in reversed(handles):
            kernel32.CloseHandle(handle)


def _read_envelope(path: Path) -> tuple[bytes, tuple[int, int, int, int]] | None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if _has_reparse(info) or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise LocalSecretError("Local credential file is not a regular unlinked file.")
    if not len(_MAGIC) < info.st_size <= _MAX_BYTES:
        raise LocalSecretError("Local credential file is invalid or too large.")
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
            raise LocalSecretError("Local credential file changed during access.")
        data = stream.read(_MAX_BYTES + 1)
    after = path.lstat()
    fingerprint = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
    if _has_reparse(after) or after.st_nlink != 1 or fingerprint != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise LocalSecretError("Local credential file changed during access.")
    if len(data) != info.st_size or len(data) > _MAX_BYTES or not data.startswith(_MAGIC):
        raise LocalSecretError("Local credential file is not a supported encrypted credential.")
    return data, fingerprint


def _decode(envelope: bytes) -> str:
    cleartext = _unprotect(envelope[len(_MAGIC):])
    invalid = not cleartext.startswith(_PLAIN_MAGIC)
    key = ""
    if not invalid:
        try:
            key = cleartext[len(_PLAIN_MAGIC):].decode("ascii")
        except UnicodeError:
            invalid = True
    if invalid or not _KEY.fullmatch(key):
        raise LocalSecretError("Local credential could not be decoded for this Windows user.")
    return key


def load_bark_key(path: Path) -> str | None:
    """Load this user's key, return None if absent, or raise a safe error."""
    failed = False
    result = None
    try:
        with _locked_parent(path) as checked:
            saved = None if checked is None else _read_envelope(checked)
            if saved is not None:
                result = _decode(saved[0])
    except (LocalSecretError, OSError, ValueError, TypeError, OverflowError):
        failed = True
    if failed:
        raise LocalSecretError("Saved Bark key is unreadable or unsafe; it was not changed.")
    return result


def save_bark_key(path: Path, key: str) -> None:
    """Encrypt and atomically replace a supported key; failures keep old data."""
    # Validate before any filesystem operation, and never persist a copied URL.
    normalized = normalize_bark_key(key)
    failed = False
    temporary = None
    try:
        with _locked_parent(path, create_parent=True) as checked:
            previous = _read_envelope(checked)
            if previous is not None:
                # Do not replace unknown, corrupted, or another user's ciphertext.
                _decode(previous[0])
            envelope = _MAGIC + _protect(_PLAIN_MAGIC + normalized.encode("ascii"))
            if not len(_MAGIC) < len(envelope) <= _MAX_BYTES:
                raise LocalSecretError("Protected Bark key exceeds the local storage limit.")
            descriptor, temporary_name = tempfile.mkstemp(prefix=".bark-key-", suffix=".tmp", dir=checked.parent)
            temporary = Path(temporary_name)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(envelope)
                stream.flush()
                os.fsync(stream.fileno())
            current = _read_envelope(checked)
            if current != previous:
                raise LocalSecretError("Local credential file changed before saving.")
            os.replace(temporary, checked)
            temporary = None
    except (LocalSecretError, OSError, ValueError, TypeError, OverflowError):
        failed = True
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                # Any leftover temporary file contains ciphertext only.
                pass
    if failed:
        raise LocalSecretError("Bark key could not be saved; any previous saved key was preserved.")
