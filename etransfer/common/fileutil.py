"""Cross-platform file I/O utilities.

Provides ``pread``, ``pwrite``, and ``ftruncate`` that work on all platforms
including Windows where ``os.pread`` / ``os.pwrite`` / ``os.ftruncate`` are
not available.
"""

import os
import sys

if sys.platform == "win32":

    def pread(fd: int, length: int, offset: int) -> bytes:
        """Positional read — emulated on Windows via seek + read."""
        os.lseek(fd, offset, os.SEEK_SET)
        return os.read(fd, length)

    def pwrite(fd: int, data: bytes, offset: int) -> int:
        """Positional write — emulated on Windows via seek + write."""
        os.lseek(fd, offset, os.SEEK_SET)
        return os.write(fd, data)

    def ftruncate(fd: int, length: int) -> None:
        """Truncate file to *length* bytes — emulated on Windows via _chsize_s."""
        import ctypes

        ucrt = ctypes.cdll.msvcrt
        ret = ucrt._chsize_s(fd, ctypes.c_int64(length))
        if ret != 0:
            raise OSError(f"_chsize_s failed with errno {ret}")

else:
    pread = os.pread
    pwrite = os.pwrite
    ftruncate = os.ftruncate


def derive_sink_object_key(filename: str, user: object = None) -> str:
    """Build a sink object key with a user-specific directory prefix.

    OIDC users  -> ``<email_local_part>/<filename>``
    API token   -> ``<millis_timestamp>/<filename>``
    """
    import time

    if user is not None:
        email = getattr(user, "email", "") or ""
        if email and "@" in email:
            return f"{email.split('@')[0]}/{filename}"
        username = getattr(user, "username", "") or ""
        if username:
            return f"{username}/{filename}"
    return f"{int(time.time() * 1000)}/{filename}"
