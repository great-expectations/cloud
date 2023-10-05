from __future__ import annotations

import pytest

from great_expectations_cloud.logging_cfg import LogLevel


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


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
