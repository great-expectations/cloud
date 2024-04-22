from __future__ import annotations

import uuid
from logging import makeLogRecord
import json
from pathlib import Path

import freezegun
import pytest

from great_expectations_cloud.logging.logging_cfg import JSONFormatter, LogLevel, DEFAULT_LOG_DIR, DEFAULT_LOG_FILE

TIMESTAMP = "2024-01-01T00:00:00+00:00"

default_log_formatted = {
        "event": "hello",
        "level": "DEBUG",
        "logger": 'root',
        "timestamp": TIMESTAMP,
    }

default_log_emitted = {"msg": "hello", "name": "root", "levelname": "DEBUG"}

# JSON_FIELDS = (

@pytest.fixture
def logfile_path():
    return Path(DEFAULT_LOG_DIR, DEFAULT_LOG_FILE)

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


import logging
from unittest import TestCase

from great_expectations_cloud.logging.logging_cfg import configure_logger



@pytest.mark.parametrize("custom_tags", [{}, {"environment": "jungle"}])
@freezegun.freeze_time(TIMESTAMP)
def test_json_formatter(custom_tags):
    fmt = JSONFormatter(custom_tags)
    out_str = fmt.format(logging.makeLogRecord(default_log_emitted))
    actual = json.loads(out_str)

    expected = {**default_log_formatted, **custom_tags}

    assert actual == expected

@freezegun.freeze_time(TIMESTAMP)
def test_json_formatter_exc_info():

    expected = {**default_log_formatted, 'exc_info': '(1, 2, 3)'}
    fmt = JSONFormatter()
    log_record = makeLogRecord({**default_log_emitted, "exc_info": (1, 2, 3)})
    out_str = fmt.format(log_record)
    actual = json.loads(out_str)
    assert actual == expected

@freezegun.freeze_time(TIMESTAMP)
def test_json_formatter_stack_info():

    expected = {**default_log_formatted, 'stack_info': 'what is this?'}
    fmt = JSONFormatter()
    log_record = makeLogRecord({**default_log_emitted, 'stack_info': 'what is this?'})
    out_str = fmt.format(log_record)
    actual = json.loads(out_str)
    assert actual == expected


import os


def test_logfile(fs, logfile_path):
    """
    fs sets up a fake fs
    """
    assert not os.path.exists(logfile_path)
    configure_logger(LogLevel.DEBUG, False, False, {}, None)
    id_in_log = str(uuid.uuid4())
    logging.getLogger().fatal(id_in_log)
    assert os.path.exists(logfile_path)
    with open(logfile_path, 'r') as f:
        assert id_in_log in f.read()

def test_logfile_skip_log_file(fs, logfile_path):
    """
    fs sets up a fake fs
    """
    assert not os.path.exists(logfile_path)
    disable_log_file = True
    configure_logger(LogLevel.DEBUG, disable_log_file, False, {}, None)
    logging.getLogger().fatal("very bad")
    assert not os.path.exists(logfile_path)

def test_logger_json():
    """
    fs sets up a fake fs
    """
    json_log = True
    logger = configure_logger(LogLevel.DEBUG, False, json_log, {}, None)
    handlers = logging.getLogger().handlers
    formatter_types = [ type(x.formatter) for x in handlers]
    assert JSONFormatter in formatter_types

if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
