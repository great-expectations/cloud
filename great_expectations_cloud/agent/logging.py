from __future__ import annotations

import enum
import logging

LOGGER = logging.getLogger(__name__)


class LogLevel(str, enum.Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

    @classmethod
    def _missing_(cls, value: str) -> LogLevel | None:
        value = value.upper()
        for member in cls:
            if member.value == value:
                return member
        return None

    @property
    def numeric_level(self) -> int:
        """
        Returns the numeric level for the log level.
        https://docs.python.org/3/library/logging.html#logging.getLevelName
        """
        return logging.getLevelName(self)


def configure_logger(log_level: LogLevel, **logger_config_kwargs) -> None:
    logging.basicConfig(level=log_level.numeric_level, **logger_config_kwargs)
    LOGGER.info(f"Configured logging at level {log_level}")
