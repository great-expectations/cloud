from __future__ import annotations

import json

import freezegun
import pytest

from great_expectations_cloud.logging.logging_cfg import JSONFormatter, LogLevel

TIMESTAMP = "2024-01-01T00:00:00+00:00"

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


class TestLogging(TestCase):
    def test_logging(self):
        configure_logger(LogLevel.INFO, False, True, {}, None)
        logger = logging.getLogger()
        with self.assertLogs(logger, level="INFO") as cm:
            logger.info("first message")
            logger.error("second mesage")
            # [ERROR] great_expectations_cloud.agent.agent: GX Agent version: 0.0.47.dev0
            self.assertEqual(cm.output, ["INFO:foo:first message", "ERROR:foo.bar:second message"])


@pytest.mark.parametrize("custom_tags", [{}, {"environment": "jungle"}])
@freezegun.freeze_time(TIMESTAMP)
def test_json_formatter(custom_tags):
    default_log_json = {
        "event": "hello",
        "level": "Level None",
        "logger": None,
        "timestamp": TIMESTAMP,
    }
    expected = {**default_log_json, **custom_tags}
    fmt = JSONFormatter(custom_tags)
    out_str = fmt.format(logging.makeLogRecord({"msg": "hello"}))
    actual = json.loads(out_str)
    assert actual == expected


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
