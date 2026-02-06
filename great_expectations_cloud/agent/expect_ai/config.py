from __future__ import annotations

OPENAI_MODEL = "gpt-4o-2024-11-20"
# OPENAI_MODEL = "gpt-4o-mini-2024-07-18"

logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "detailed": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "detailed",
            "level": "DEBUG",
        },
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["console"],
            "level": "WARNING",  # Default log level for all other loggers
            "propagate": True,
        },
        "expect_ai": {
            "handlers": ["console"],
            "level": "DEBUG",  # Specific level for this module
            "propagate": False,
        },
        "langsmith.client": {
            "handlers": ["console"],  # send to dev null because this is chatty and broken
            "level": "CRITICAL",  # Specific level for this module
            "propagate": False,
        },
    },
}
