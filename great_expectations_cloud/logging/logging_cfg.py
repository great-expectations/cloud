from __future__ import annotations

import enum
import json
import logging
import logging.config
import logging.handlers
import pathlib
from typing import TYPE_CHECKING, Any, Final, MutableMapping, Sequence

import structlog
from typing_extensions import override

if TYPE_CHECKING:
    from structlog.typing import Processor

LOGGER = logging.getLogger(__name__)
SERVICE_NAME: Final[str] = "gx-agent"
DEFAULT_FILE_LOGGING_LEVEL: Final[int] = logging.DEBUG


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
    log_level: LogLevel,
    skip_log_file: bool,
    json_log: bool,
    environment: str,
    log_cfg_file: pathlib.Path | None,
) -> None:
    """
    Configure the root logger for the application.
    If a log configuration file is provided, other arguments are ignored.

    See the documentation for the logging.config.dictConfig method for details.
    https://docs.python.org/3/library/logging.config.html#logging-config-dictschema

    Note: this method should only be called once in the lifecycle of the application.
    """
    if log_cfg_file:
        _load_cfg_from_file(log_cfg_file)
        return

    root = logging.getLogger()
    root.setLevel(log_level.numeric_level)

    structured_log_handler = _get_structured_log_handler(json_log, environment)
    root.addHandler(structured_log_handler)

    if not skip_log_file:
        file_handler = _get_file_handler()
        root.addHandler(file_handler)


def _get_file_handler():
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | line: %(lineno)d | %(levelname)s: %(message)s"
    )
    log_dir = pathlib.Path("logs")
    if not log_dir.exists():
        pathlib.Path(log_dir).mkdir()
    # The FileHandler writes all logs to a local file
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_dir / "logfile", when="midnight", backupCount=30
    )  # creates a new file every day; keeps 30 days of logs at most
    file_handler.setFormatter(formatter)
    file_handler.setLevel(DEFAULT_FILE_LOGGING_LEVEL)
    file_handler.namer = lambda name: name + ".log"  # append file extension to name
    return file_handler


def _get_structured_log_handler(json_log: bool, environment: str):
    def _add_our_custom_fields(
        logger: structlog.types.WrappedLogger,
        method_name: str,
        event_dict: MutableMapping[str, Any],
    ) -> MutableMapping[str, Any]:
        event_dict["service"] = SERVICE_NAME
        event_dict["env"] = environment

        return event_dict

    handler = logging.StreamHandler()

    custom_fmt_processors: list[Processor] = [_add_our_custom_fields]

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=_build_pre_processors(custom_fmt_processors),
        processors=_select_final_output(json_log),
    )
    handler.setFormatter(formatter)
    return handler


def _load_cfg_from_file(log_cfg_file):
    if not log_cfg_file.exists():
        raise FileNotFoundError(  # noqa: TRY003 # one off error
            f"Logging config file not found: {log_cfg_file.absolute()}"
        )
    dict_config = json.loads(log_cfg_file.read_text())
    logging.config.dictConfig(dict_config)
    LOGGER.info(f"Configured logging from file {log_cfg_file}")


def _select_final_output(json_log: bool) -> Sequence[Processor]:
    # remove_processors_meta cleans up the event dict
    processors: list[Processor] = [structlog.stdlib.ProcessorFormatter.remove_processors_meta]
    if json_log:
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()
    processors.append(renderer)
    return processors


# Wow the order of these processors is important
# https://www.structlog.org/en/stable/standard-library.html#processors
# tl;dr: The processors modify the logger kwargs in sequential order
#
def _build_pre_processors(custom_processors: list[Processor]):
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
        #   .warn("something bad", ..., service="gx-agent", env="production")
        *custom_processors,
    ]
