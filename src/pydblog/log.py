"""
Logging for the package.

Every module logs through ``logging.getLogger(__name__)``, which puts them all under
``pydblog``, so one level set controls the lot. Nothing is written until a caller asks
for it: a library that installs handlers on import takes over decisions that belong to
the application running it.

Two levels carry the whole vocabulary. INFO is one line per step — a window read, a
chunk read, a connection opened. DEBUG adds the detail behind each step, including the
generated SQL.
"""

import logging
import sys
from typing import TextIO

LOGGER_NAME = "pydblog"

# Marks the handler this module owns, so reconfiguring replaces that one and leaves
# anything the host added in place.
_HANDLER_NAME = "pydblog-stream"

_FORMAT = "%(asctime)s %(levelname)-8s %(name)s %(message)s"

# Silences "No handlers could be found" without deciding where anything goes.
logging.getLogger(LOGGER_NAME).addHandler(logging.NullHandler())


def configure_logging(
    verbose: bool = False, stream: TextIO | None = None
) -> logging.Logger:
    """
    Send the package's logging somewhere, for callers with no setup of their own.

    An application that already configures logging does not need this: the package's
    loggers are ordinary ones under ``pydblog`` and will inherit whatever it set up.

    Calling this again replaces the handler it installed before rather than adding a
    second one, so turning verbosity on part-way through does not double the output.

    Args:
        verbose: Whether to include the detail behind each step — generated SQL, key
            positions, per-chunk and per-window figures. Off means one line per step.
        stream: Where the output goes. Defaults to stderr.

    Returns:
        The package logger, already configured.
    """

    logger = logging.getLogger(LOGGER_NAME)

    for installed in [h for h in logger.handlers if h.name == _HANDLER_NAME]:
        logger.removeHandler(installed)

    handler = logging.StreamHandler(sys.stderr if stream is None else stream)
    handler.name = _HANDLER_NAME
    handler.setFormatter(logging.Formatter(_FORMAT))

    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    return logger
