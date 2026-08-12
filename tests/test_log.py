"""
Logging setup tests.

A library has to be silent until its host asks for output, and it must never
reconfigure logging on the host's behalf. Both are easy to break by accident, so
they are pinned here.
"""

import io
import logging

import pytest

from pydblog.log import LOGGER_NAME, configure_logging


@pytest.fixture(autouse=True)
def restore_logger():
    """Undo whatever a test did to the package logger."""
    logger = logging.getLogger(LOGGER_NAME)
    handlers = list(logger.handlers)
    level = logger.level

    yield logger

    logger.handlers = handlers
    logger.setLevel(level)


# ---------------------------------------------------------------------------
# Silent by default
# ---------------------------------------------------------------------------


def test_the_package_logger_has_a_null_handler(restore_logger):
    """Without one, Python prints its own 'no handlers' warning."""
    assert any(isinstance(h, logging.NullHandler) for h in restore_logger.handlers)


def test_nothing_is_written_until_logging_is_configured(restore_logger, capsys):
    logging.getLogger("pydblog.somewhere").info("should not appear")

    assert capsys.readouterr().err == ""


# ---------------------------------------------------------------------------
# Turning it on
# ---------------------------------------------------------------------------


def test_configuring_writes_step_level_output(restore_logger):
    stream = io.StringIO()

    configure_logging(stream=stream)
    logging.getLogger("pydblog.somewhere").info("a step happened")

    assert "a step happened" in stream.getvalue()


def test_the_quiet_default_leaves_out_the_detail(restore_logger):
    stream = io.StringIO()

    configure_logging(stream=stream)
    logging.getLogger("pydblog.somewhere").debug("the generated query")

    assert stream.getvalue() == ""


def test_verbose_lets_the_detail_through(restore_logger):
    stream = io.StringIO()

    configure_logging(verbose=True, stream=stream)
    logging.getLogger("pydblog.somewhere").debug("the generated query")

    assert "the generated query" in stream.getvalue()


@pytest.mark.parametrize(
    "verbose, expected", [(False, logging.INFO), (True, logging.DEBUG)]
)
def test_the_level_follows_the_flag(restore_logger, verbose, expected):
    configure_logging(verbose=verbose, stream=io.StringIO())

    assert restore_logger.level == expected


def test_the_output_says_which_step_it_came_from(restore_logger):
    stream = io.StringIO()

    configure_logging(stream=stream)
    logging.getLogger("pydblog.dblog").info("read a chunk")

    assert "pydblog.dblog" in stream.getvalue()


# ---------------------------------------------------------------------------
# Repeated calls, and the host's own setup
# ---------------------------------------------------------------------------


def test_configuring_twice_does_not_duplicate_the_output(restore_logger):
    """A second call replaces the handler rather than adding another."""
    stream = io.StringIO()

    configure_logging(stream=stream)
    configure_logging(stream=stream)
    logging.getLogger("pydblog.somewhere").info("once")

    assert stream.getvalue().count("once") == 1


def test_reconfiguring_can_turn_the_detail_on_later(restore_logger):
    stream = io.StringIO()

    configure_logging(stream=stream)
    configure_logging(verbose=True, stream=stream)
    logging.getLogger("pydblog.somewhere").debug("now visible")

    assert "now visible" in stream.getvalue()


def test_a_handler_the_host_added_is_left_alone(restore_logger):
    """Only the handler this module installed is ever removed."""
    host = logging.StreamHandler(io.StringIO())
    restore_logger.addHandler(host)

    configure_logging(stream=io.StringIO())

    assert host in restore_logger.handlers


def test_the_root_logger_is_never_touched(restore_logger):
    root_handlers = list(logging.getLogger().handlers)

    configure_logging(verbose=True, stream=io.StringIO())

    assert logging.getLogger().handlers == root_handlers
