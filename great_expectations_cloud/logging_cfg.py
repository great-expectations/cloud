from __future__ import annotations

import enum
import json
import logging
import logging.config
import pathlib

from typing_extensions import override

LOGGER = logging.getLogger(__name__)


class LogLevel(str, enum.Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

    @override
    @classmethod
    def _missing_(cls, value: object) -> LogLevel | None:
        if not isinstance(value, str):
            return None
        value = value.upper()
        return {m.value: m for m in cls}.get(value)

    @property
    def numeric_level(self) -> int:
        """
        Returns the numeric level for the log level.
        https://docs.python.org/3/library/logging.html#logging.getLevelName
        """
        return logging.getLevelName(self)


def configure_logger(
    log_level: LogLevel, log_cfg_file: pathlib.Path | None, **logger_config_kwargs
) -> None:
    """
    Configure the root logger for the application.
    If a log configuration file is provided, other arguments are ignored.

    See the documentation for the logging.config.dictConfig method for details.
    https://docs.python.org/3/library/logging.config.html#logging-config-dictschema
    """
    if log_cfg_file:
        dict_config = json.loads(log_cfg_file.read_text())
        logging.config.dictConfig(dict_config)
        LOGGER.info(f"Configured logging from file {log_cfg_file}")
    else:
        logging.basicConfig(level=log_level.numeric_level, **logger_config_kwargs)
        LOGGER.info(f"Configured logging at level {log_level}")
