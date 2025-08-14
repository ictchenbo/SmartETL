from io import BufferedReader, BytesIO, TextIOWrapper
from tempfile import SpooledTemporaryFile
from time import sleep
from typing import IO, TYPE_CHECKING, Any, Optional, TypeVar, cast


def convert_to_bytes(file: bytes | IO[bytes]) -> bytes:
    """Extract the bytes from `file` without preventing it from being read again later.

    As a convenience to simplify client code, also returns `file` unchanged if it is already bytes.
    """
    if isinstance(file, bytes):
        return file

    if isinstance(file, SpooledTemporaryFile):
        file.seek(0)
        f_bytes = file.read()
        file.seek(0)
        return f_bytes

    if isinstance(file, BytesIO):
        return file.getvalue()

    if isinstance(file, (TextIOWrapper, BufferedReader)):
        with open(file.name, "rb") as f:
            return f.read()

    raise ValueError("Invalid file-like object type")


def exactly_one(**kwargs: Any) -> None:
    """
    Verify arguments; exactly one of all keyword arguments must not be None.

    Example:
        >>> exactly_one(filename=filename, file=file, text=text, url=url)
    """
    if sum([(arg is not None and arg != "") for arg in kwargs.values()]) != 1:
        names = list(kwargs.keys())
        if len(names) > 1:
            message = f"Exactly one of {', '.join(names[:-1])} and {names[-1]} must be specified."
        else:
            message = f"{names[0]} must be specified."
        raise ValueError(message)
