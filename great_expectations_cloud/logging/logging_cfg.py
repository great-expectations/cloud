from __future__ import annotations

import dataclasses as dc
import enum
import json
import logging
import logging.config
import logging.handlers
import pathlib
from collections.abc import Callable, MutableMapping, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal

import structlog
from typing_extensions import override

if TYPE_CHECKING:
    from structlog.types import Processor

    from great_expectations_cloud.agent.config import GxAgentEnvVars

LOGGER = logging.getLogger(__name__)

DEFAULT_LOG_FILE: Final[str] = "logfile"
DEFAULT_LOG_DIR = "logs"
SERVICE_NAME: Final[str] = "gx-agent"
DEFAULT_FILE_LOGGING_LEVEL: Final[int] = logging.DEBUG


@dc.dataclass
class LogSettings:
    log_level: LogLevel
    skip_log_file: bool
    json_log: bool
    custom_tags: dict[str, Any]
    log_cfg_file: pathlib.Path | None


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


def _logging_level_from_str(level: str) -> int:
    if not level:
        return logging.INFO

    log_level = logging.getLevelName(level.upper())
    if isinstance(log_level, int):
        return log_level
    return logging.INFO


def _build_processors(config: GxAgentEnvVars) -> Sequence[Processor]:
    result: list[Processor] = [
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
    ]

    if config.environment.lower() == "local":
        result.append(structlog.dev.ConsoleRenderer())
    else:
        result.append(structlog.processors.JSONRenderer())

    return result


def _add_default_tags(
    tags: dict[str, Any] | None = None,
) -> Callable[
    [structlog.types.WrappedLogger, str, MutableMapping[str, Any]], MutableMapping[str, Any]
]:
    def add(
        logger: structlog.types.WrappedLogger,
        method_name: str,
        event_dict: MutableMapping[str, Any],
    ) -> MutableMapping[str, Any]:
        if tags is not None:
            event_dict.update(tags)
        return event_dict

    return add


def _logging_configuration_processor(
    config: GxAgentEnvVars,
) -> Callable[
    [structlog.types.WrappedLogger, str, MutableMapping[str, Any]], MutableMapping[str, Any]
]:
    def _logging_configuration_processor_inner(
        logger: structlog.types.WrappedLogger,
        method_name: str,
        event_dict: MutableMapping[str, Any],
    ) -> MutableMapping[str, Any]:
        event_dict["service"] = config.service_name
        event_dict["logging_version"] = config.logging_version
        event_dict["env"] = config.environment
        return event_dict

    return _logging_configuration_processor_inner


def _build_pre_processors(
    config: GxAgentEnvVars, tags: dict[str, Any] | None = None
) -> Sequence[Processor]:
    return [
        structlog.stdlib.ExtraAdder(),
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.contextvars.merge_contextvars,
        _logging_configuration_processor(config),
        _add_default_tags(tags=tags),
    ]


# TODO Add org ID
def configure_logger(log_settings: LogSettings, config: GxAgentEnvVars) -> None:
    """
    Configure the root logger for the application.
    If a log configuration file is provided, other arguments are ignored.

    See the documentation for the logging.config.dictConfig method for details.
    https://docs.python.org/3/library/logging.config.html#logging-config-dictschema

    Note: this method should only be called once in the lifecycle of the application.
    """

    if log_settings.log_cfg_file:
        _load_cfg_from_file(log_settings.log_cfg_file)
        return

    root = logging.getLogger()

    log_level = _logging_level_from_str(log_settings.log_level)

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=_build_processors(config),
        foreign_pre_chain=_build_pre_processors(config=config, tags=log_settings.custom_tags),
    )
    structlog_handler = logging.StreamHandler()
    structlog_handler.setFormatter(formatter)
    structlog_handler.setLevel(log_level)

    root.setLevel(log_level)

    root.addHandler(structlog_handler)

    # 2024-08-12: Reduce noise of pika reconnects
    logging.getLogger("pika").setLevel(logging.WARNING)

    # Reduce noise from GX Core
    logging.getLogger("great_expectations").setLevel(logging.WARNING)

    # Reduce noise from GX Core version checker
    logging.getLogger("great_expectations.data_context._version_checker").setLevel(logging.ERROR)

    # TODO Define file loggers as dictConfig as well
    if not log_settings.skip_log_file:
        file_handler = _get_file_handler()
        root.addHandler(file_handler)

    logging.getLogger(__name__).setLevel(log_level)


def _get_file_handler() -> logging.handlers.TimedRotatingFileHandler:
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | line: %(lineno)d | %(levelname)s: %(message)s"
    )
    log_dir = pathlib.Path(DEFAULT_LOG_DIR)
    if not log_dir.exists():
        pathlib.Path(log_dir).mkdir()
    # The FileHandler writes all logs to a local file
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_dir / DEFAULT_LOG_FILE, when="midnight", backupCount=30
    )  # creates a new file every day; keeps 30 days of logs at most
    file_handler.setFormatter(formatter)
    file_handler.setLevel(DEFAULT_FILE_LOGGING_LEVEL)
    file_handler.namer = lambda name: name + ".log"  # append file extension to name
    return file_handler


def _load_cfg_from_file(log_cfg_file: pathlib.Path) -> None:
    if not log_cfg_file.exists():
        raise FileNotFoundError(  # noqa: TRY003 # one off error
            f"Logging config file not found: {log_cfg_file.absolute()}"
        )
    dict_config = json.loads(log_cfg_file.read_text())
    logging.config.dictConfig(dict_config)
    LOGGER.info(f"Configured logging from file {log_cfg_file}")


class JSONFormatter(logging.Formatter):
    """
    All custom formatting is done through subclassing this Formatter class
    Note: Defined within fn bc parametrization of Formatters is not supported by dictConfig

    """

    _SKIP_KEYS: ClassVar[frozenset[str]] = frozenset(
        [
            "exc_text",
            "levelno",
            "lineno",
            "msecs",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "thread",
            "threadName",
        ]
    )

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: Literal["%", "{", "$"] = "%",
        validate: bool = True,
        **kwargs: dict[str, Any],
    ):
        super().__init__(fmt, datefmt, style, validate)
        if custom_tags := kwargs.get("custom_tags"):
            self.custom_tags = custom_tags
        else:
            self.custom_tags = {}

    @override
    def format(self, record: logging.LogRecord) -> str:
        """
        TODO Support fstrings substitution containing '%s' syntax

        Example from snowflake-connector-python:
        logger.error(
            "Snowflake Connector for Python Version: %s, "
            "Python Version: %s, Platform: %s",
            SNOWFLAKE_CONNECTOR_VERSION,
            PYTHON_VERSION,
            PLATFORM,
        )
        """
        log_full = record.__dict__

        log_full["event"] = record.msg
        log_full["level"] = record.levelname
        log_full["logger"] = record.name
        log_full["timestamp"] = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()

        if record.exc_info:
            log_full["exc_info"] = str(record.exc_info)

        log_subset = {
            key: value
            for key, value in log_full.items()
            if key is not None and key not in self._SKIP_KEYS
        }

        complete_dict = {
            **log_subset,
            **self.custom_tags,
        }

        try:
            return json.dumps(complete_dict)
        except TypeError:
            # Use repr() to avoid infinite recursion due to throwing another error
            complete_dict = {key: repr(value) for key, value in complete_dict.items()}
            return json.dumps(complete_dict)
