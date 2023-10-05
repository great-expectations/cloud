from __future__ import annotations

import argparse
import dataclasses as dc
import logging
import pathlib

from great_expectations_cloud.logging_cfg import LogLevel, configure_logger

LOGGER = logging.getLogger(__name__)


@dc.dataclass(frozen=True)
class Arguments:
    log_level: LogLevel
    log_cfg_file: pathlib.Path | None


def _parse_args() -> Arguments:
    """
    Parse arguments from the command line and return them as a type aware
    `Arguments` dataclass.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-level",
        help="Level of logging to use. Defaults to WARNING.",
        default="WARNING",
        type=LogLevel,
    )
    parser.add_argument(
        "--log-cfg-file",
        help="Path to a logging configuration json file.",
        type=pathlib.Path,
    )
    args = parser.parse_args()
    return Arguments(
        log_level=args.log_level,
        log_cfg_file=args.log_cfg_file,
    )


def main() -> None:
    args: Arguments = _parse_args()
    configure_logger(
        log_level=args.log_level,
        log_cfg_file=args.log_cfg_file,
    )

    # lazy import to ensure our cli is fast and responsive
    from great_expectations_cloud.agent import run_agent

    run_agent()


if __name__ == "__main__":
    main()
