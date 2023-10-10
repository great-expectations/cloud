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
        return logging.getLevelName(  # type: ignore[no-any-return] # will return int if given str
            self
        )


def configure_logger(log_level: LogLevel, log_cfg_file: pathlib.Path | None) -> None:
    """
    Configure the root logger for the application.
    If a log configuration file is provided, other arguments are ignored.

    See the documentation for the logging.config.dictConfig method for details.
    https://docs.python.org/3/library/logging.config.html#logging-config-dictschema

    Note: this method should only be called once in the lifecycle of the application.
    """
    if log_cfg_file:
        if not log_cfg_file.exists():
            raise FileNotFoundError(f"Logging config file not found: {log_cfg_file.absolute()}")
        dict_config = json.loads(log_cfg_file.read_text())
        logging.config.dictConfig(dict_config)
        LOGGER.info(f"Configured logging from file {log_cfg_file}")
    else:
        logging.basicConfig(level=log_level.numeric_level)
        LOGGER.info(f"Configured logging at level {log_level}")
