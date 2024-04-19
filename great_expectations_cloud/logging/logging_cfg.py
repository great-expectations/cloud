from __future__ import annotations

import enum
import json
import logging
import logging.config
import logging.handlers
import pathlib
from datetime import datetime, timezone
from typing import Any, Final

from typing_extensions import override

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


# TODO Add org ID
def configure_logger(
    log_level: LogLevel,
    skip_log_file: bool,
    json_log: bool,
    custom_tags: dict[str, Any],
    log_cfg_file: pathlib.Path | None,
) -> None:  # TODO Simplify args
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

    # change 'default' handler formatter to 'json'
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"default_fmt": {"format": "[%(levelname)s] %(name)s: %(message)s"}},
        "handlers": {
            "default_handler": {
                "formatter": "default_fmt",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "": {  # root logger
                "handlers": ["default_handler"],
                "level": "WARNING",
                "propagate": False,
            },
        },
    }

    logging.config.dictConfig(config)

    # TODO Define file loggers as dictConfig as well
    root = logging.getLogger()
    # TODO Safer way to get handler
    if json_log:
        root.handlers[0].setFormatter(JSONFormatter(custom_tags=custom_tags))
    root.setLevel(log_level.numeric_level)

    if not skip_log_file:
        file_handler = _get_file_handler()
        root.addHandler(file_handler)


def _get_file_handler() -> logging.handlers.TimedRotatingFileHandler:
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

    LogRecord attribute:
    'name': 'great_expectations_cloud.agent.agent',
     'msg': 'GX Agent version: 0.0.47.dev0',
     'args': (),
     'levelname': 'ERROR',
     'levelno': 40,
     'pathname': '/Users/r/py/cloud/great_expectations_cloud/agent/agent.py',
     'filename': 'agent.py',
     'module': 'agent',
     'exc_info': None,
     'exc_text': None,
     'stack_info': None,
     'lineno': 91,
     'funcName': '__init__',
     'created': 1713292929.1121202,
     'msecs': 112.0,
     'relativeCreated': 5210.726261138916,
     'thread': 8276975872,
     'threadName': 'MainThread',
     'processName': 'MainProcess',
     'process': 44923}
    """

    def __init__(
        self,
        fmt=None,
        datefmt=None,
        style="%",
        custom_tags: dict[str, Any] = {},
        validate=True,
        *args,
        defaults=None,
    ):
        super().__init__(fmt, datefmt, style, validate)
        self.custom_tags = custom_tags

    @override
    def format(self, record):
        # DD ref:
        # {"event": "send_request_body.started request=<Request [b'GET']>", "logger": "httpcore.http11", "level": "debug",
        #  "timestamp": "2024-04-18T10:17:56.411405Z", "service": "unset", "logging_version": "0.2.0", "env": "robs",
        #  "dd.trace_id": "0", "dd.span_id": "0"}

        formatted_record = {
            "event": record.msg,
            "level": record.levelname,
            "logger": record.name,
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
        }
        # TODO add exc, stack

        complete_dict = {**formatted_record, **self.custom_tags}

        return json.dumps(complete_dict)
