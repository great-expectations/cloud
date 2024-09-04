from __future__ import annotations

import json
import logging
import uuid
from logging import makeLogRecord
from pathlib import Path
from typing import Any

import freezegun
import pytest

from great_expectations_cloud.logging.logging_cfg import (
    DEFAULT_LOG_DIR,
    DEFAULT_LOG_FILE,
    JSONFormatter,
    LogLevel,
    LogSettings,
    configure_logger,
)

"""
Note: fs fixture sets up a fake fs
"""

TIMESTAMP = "2024-01-01T00:00:00+00:00"

default_log_formatted = {
    "event": "hello",
    "level": "DEBUG",
    "logger": "root",
    "timestamp": TIMESTAMP,
}

default_log_emitted = {"msg": "hello", "name": "root", "levelname": "DEBUG"}


@pytest.fixture(autouse=True)
def set_required_env_vars(monkeypatch, random_uuid, random_string, local_mercury):
    monkeypatch.setenv("GX_CLOUD_ORGANIZATION_ID", random_uuid)
    monkeypatch.setenv("GX_CLOUD_ACCESS_TOKEN", random_string)
    monkeypatch.setenv("GX_CLOUD_BASE_URL", local_mercury)


@pytest.fixture
def logfile_path():
    return Path(DEFAULT_LOG_DIR, DEFAULT_LOG_FILE)


def is_subset(small_set: dict[str, Any], big_set: dict[str, Any]):
    """
    Returns if dict1 a subset of dict2
    """
    return not bool(set(small_set) - set(big_set))


class TestLogLevel:
    @pytest.mark.parametrize(
        "log_level",
        [
            "DEBUG",
            "debug",
            "INFO",
            "info",
            "WARNING",
            "warning",
            "ERROR",
            "error",
            "CRITICAL",
            "critical",
        ],
    )
    def test_valid_values(self, log_level: str):
        enum_instance = LogLevel(log_level)  # should not raise an error
        assert enum_instance == log_level.upper()

    def test_invalid_values(self):
        with pytest.raises(ValueError):
            LogLevel("not_a_valid_log_level")

    def test_numeric_level(self):
        assert LogLevel.DEBUG.numeric_level == 10
        assert LogLevel.INFO.numeric_level == 20
        assert LogLevel.WARNING.numeric_level == 30
        assert LogLevel.ERROR.numeric_level == 40
        assert LogLevel.CRITICAL.numeric_level == 50


@pytest.mark.parametrize("custom_tags", [{}, {"environment": "jungle"}])
@freezegun.freeze_time(TIMESTAMP)
def test_json_formatter(custom_tags):
    fmt = JSONFormatter(custom_tags=custom_tags)
    out_str = fmt.format(logging.makeLogRecord(default_log_emitted))
    actual = json.loads(out_str)

    expected = {**default_log_formatted, **custom_tags}

    assert is_subset(expected, actual)


@freezegun.freeze_time(TIMESTAMP)
def test_json_formatter_exc_info():
    expected = {**default_log_formatted, "exc_info": "(1, 2, 3)"}
    fmt = JSONFormatter()
    log_record = makeLogRecord({**default_log_emitted, "exc_info": (1, 2, 3)})
    out_str = fmt.format(log_record)
    actual = json.loads(out_str)
    assert is_subset(expected, actual)


@freezegun.freeze_time(TIMESTAMP)
def test_json_formatter_extra():
    expected = {**default_log_formatted, "user": "123"}
    fmt = JSONFormatter()
    # Not: makeLogRecord does not account for extra kwarg
    log_record = logging.getLogger(default_log_emitted["name"]).makeRecord(
        default_log_emitted["name"],
        logging.DEBUG,
        "fn_name",
        0,
        default_log_emitted["msg"],
        (),
        None,
        extra={"user": "123"},
    )
    out_str = fmt.format(log_record)
    actual = json.loads(out_str)
    assert is_subset(expected, actual)


@freezegun.freeze_time(TIMESTAMP)
def test_json_formatter_stack_info():
    stack_info = 'File "abc.py", line 553, in <module> ...'
    expected = {**default_log_formatted, "stack_info": stack_info}
    fmt = JSONFormatter()
    log_record = makeLogRecord({**default_log_emitted, "stack_info": stack_info})
    out_str = fmt.format(log_record)
    actual = json.loads(out_str)
    assert is_subset(expected, actual)


def test_logfile(fs, logfile_path):
    assert not Path.exists(logfile_path)

    configure_logger(LogSettings(LogLevel.DEBUG, False, False, {}, None))
    id_in_log = str(uuid.uuid4())
    logging.getLogger().fatal(id_in_log)
    assert Path.exists(logfile_path)
    with open(logfile_path) as f:
        assert id_in_log in f.read()


def test_logfile_skip_log_file(fs, logfile_path):
    assert not Path.exists(logfile_path)
    disable_log_file = True
    configure_logger(LogSettings(LogLevel.DEBUG, disable_log_file, False, {}, None))
    logging.getLogger().fatal("very bad")
    assert not Path.exists(logfile_path)


def test_logger_json():
    json_log = True
    configure_logger(LogSettings(LogLevel.DEBUG, False, json_log, {}, None))
    handlers = logging.getLogger().handlers
    formatter_types = [type(x.formatter) for x in handlers]
    assert JSONFormatter in formatter_types


def test_load_logging_cfg(fs):
    config_dict = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"default_fmt": {"format": "[%(levelname)s] %(name)s: %(message)s"}},
        "handlers": {
            "config_handler": {
                "formatter": "default_fmt",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "": {
                "handlers": ["config_handler"],
                "level": "WARNING",
                "propagate": False,
            },
        },
    }
    config_str = json.dumps(config_dict)
    config_path = "log.config"
    with open(config_path, "wb") as f:
        f.write(config_str.encode())
    assert Path.exists(Path(config_path))
    configure_logger(LogSettings(LogLevel.DEBUG, False, False, {}, Path(config_path)))
    handlers = logging.getLogger().handlers
    assert "config_handler" in [x.name for x in handlers]


def test_log_cfg_file_not_found():
    with pytest.raises(FileNotFoundError):
        configure_logger(LogSettings(LogLevel.DEBUG, False, False, {}, Path("nowhere")))


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
