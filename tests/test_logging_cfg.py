from __future__ import annotations
from logging import makeLogRecord
import json

import freezegun
import pytest

from great_expectations_cloud.logging.logging_cfg import JSONFormatter, LogLevel

TIMESTAMP = "2024-01-01T00:00:00+00:00"

default_log_formatted = {
        "event": "hello",
        "level": "DEBUG",
        "logger": 'root',
        "timestamp": TIMESTAMP,
    }

default_log_emitted = {"msg": "hello", "name": "root", "levelname": "DEBUG"}

# JSON_FIELDS = (


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


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
