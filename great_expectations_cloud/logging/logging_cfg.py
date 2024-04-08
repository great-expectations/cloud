from __future__ import annotations

import enum
import json
import logging
import logging.config
import logging.handlers
import pathlib
from typing import MutableMapping, Any, Sequence

import structlog
from structlog.typing import Processor
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


def configure_logger(
    log_level: LogLevel, skip_log_file: bool, log_cfg_file: pathlib.Path | None, json_log: bool, environment: str
) -> None:
    """
    Configure the root logger for the application.
    If a log configuration file is provided, other arguments are ignored.

    See the documentation for the logging.config.dictConfig method for details.
    https://docs.python.org/3/library/logging.config.html#logging-config-dictschema

    Note: this method should only be called once in the lifecycle of the application.
    """
    if log_cfg_file:
        if not log_cfg_file.exists():
            raise FileNotFoundError(  # noqa: TRY003 # one off error
                f"Logging config file not found: {log_cfg_file.absolute()}"
            )
        dict_config = json.loads(log_cfg_file.read_text())
        logging.config.dictConfig(dict_config)
        LOGGER.info(f"Configured logging from file {log_cfg_file}")
    else:
        logDirectory = pathlib.Path("logs")
        if not logDirectory.exists():
            pathlib.Path(logDirectory).mkdir()

        # logger = logging.getLogger()
        # # TODO Replace this one
        # formatter = logging.Formatter(
        #     "%(asctime)s | %(name)s | line: %(lineno)d | %(levelname)s: %(message)s"
        # )

        # The StreamHandler writes logs to stderr based on the provided log level
        # stream_handler = logging.StreamHandler()
        # stream_handler.setLevel(log_level.numeric_level)
        # stream_handler.setFormatter(formatter)
        # logger.addHandler(stream_handler)
        # logger.setLevel(
        #     logging.DEBUG
        # )  # set root logger to lowest-possible level - otherwise it will block levels set for file handler and stream handler
        #
        # if skip_log_file:
        #     return
        #
        # # The FileHandler writes all logs to a local file
        # file_handler = logging.handlers.TimedRotatingFileHandler(
        #     filename=logDirectory / "logfile", when="midnight", backupCount=30
        # )  # creates a new file every day; keeps 30 days of logs at most
        # file_handler.setFormatter(formatter)
        # file_handler.setLevel(logging.DEBUG)
        # file_handler.namer = lambda name: name + ".log"  # append file extension to name
        # logger.addHandler(file_handler)

        # new...
        formatter = structlog.stdlib.ProcessorFormatter(
            processors=_build_processors(json_log),
            foreign_pre_chain=_build_pre_processors(),
        )
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        root = logging.getLogger()
        root.setLevel(log_level.numeric_level)
        root.addHandler(handler)


# This is mocked in our testing, so its useful to be in its own function
def _build_processors(json_log:bool) -> Sequence[Processor]:
    result: list[Processor] = [
        structlog.stdlib.ProcessorFormatter.remove_processors_meta
    ]
    if json_log:
        result.append(structlog.processors.JSONRenderer())
    else:
        result.append(structlog.dev.ConsoleRenderer())
    return result


# Wow the order of these processors is important
# https://www.structlog.org/en/stable/standard-library.html#processors
# tl;dr: The processors modify the logger kwargs in sequential order
#
def _build_pre_processors(tags: dict | None = None):
    return [
        # Append the logger name to event dict argument
        #   .warn("something bad", ..., logger="thatclass")
        structlog.stdlib.add_logger_name,
        # Add log level to event dict
        #   .warn("something bad", ..., level="warn")
        structlog.stdlib.add_log_level,
        # Append timestamp
        #   .warn("something bad",..., timestamp=<timestamp>)
        structlog.processors.TimeStamper(fmt="iso"),
        # If present, add stack trace
        #   .warn("something bad", ..., stack=<stack_trace>)
        structlog.processors.StackInfoRenderer(),
        # If present, add exception
        #   .warn("something bad", ..., exception=<exception>)
        structlog.processors.format_exc_info,
        # Convert all key values to unicode format
        structlog.processors.UnicodeDecoder(),
        # adds our custom environment vars
        #   .warn("something bad", ..., service="mercury", logging_version="0.0.1", env="dev")
        _add_our_custom_fields,
    ]

def _add_our_custom_fields(
    logger: structlog.types.WrappedLogger,
    method_name: str,
    event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    event_dict["service"] = "gx-agent"
    event_dict["env"] = "TODO add environment"

    return event_dict