"""The engine's log, as the ``fluree.engine`` logger."""

from __future__ import annotations

import logging

from fluree import _fluree

_BY_NAME = {"OFF": 0, "CRITICAL": 1, "ERROR": 1, "WARNING": 2, "WARN": 2, "INFO": 3, "DEBUG": 4, "TRACE": 5}


def set_log_level(level: str | int) -> None:
    """Send the engine's log events at ``level`` and above to the
    ``fluree.engine`` logger: a :mod:`logging` level (``"INFO"``,
    ``logging.DEBUG``), ``"TRACE"`` for the most detail, or ``"OFF"``.
    Warnings and errors are sent by default.

    This sets what the engine produces; the logger's own level and handlers
    still decide what is shown. Below the level, the engine skips the events
    entirely."""
    if isinstance(level, str):
        try:
            code = _BY_NAME[level.upper()]
        except KeyError:
            raise ValueError(f"unknown log level {level!r}") from None
    elif isinstance(level, int) and not isinstance(level, bool):
        code = 1 if level >= logging.ERROR else 2 if level >= logging.WARNING else 3 if level >= logging.INFO else 4 if level >= logging.DEBUG else 5
    else:
        raise TypeError(f"a log level is a name or a logging level number, not {type(level).__name__}")
    _fluree.set_log_level(code)
