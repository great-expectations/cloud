from __future__ import annotations

import argparse
import dataclasses as dc
import logging
import pathlib

from great_expectations_cloud.logging.logging_cfg import LogLevel, configure_logger

LOGGER = logging.getLogger(__name__)


@dc.dataclass(frozen=True)
class Arguments:
    log_level: LogLevel
    skip_log_file: bool
    log_cfg_file: pathlib.Path | None
    version: bool


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
        "--skip-log-file",
        help="Skip writing debug logs to a file. Defaults to False. Does not affect logging to stdout/stderr.",
        default=False,
        type=pathlib.Path,
    )
    parser.add_argument(
        "--log-cfg-file",
        help="Path to a logging configuration json file. Supersedes --log-level and --skip-log-file.",
        type=pathlib.Path,
    )
    parser.add_argument("--version", help="Show the gx agent version.", action="store_true")
    args = parser.parse_args()
    return Arguments(
        log_level=args.log_level,
        skip_log_file=args.skip_log_file,
        log_cfg_file=args.log_cfg_file,
        version=args.version,
    )


def main() -> None:
    # lazy imports ensure our cli is fast and responsive
    args: Arguments = _parse_args()
    configure_logger(
        log_level=args.log_level, skip_log_file=args.skip_log_file, log_cfg_file=args.log_cfg_file
    )

    if args.version:
        from great_expectations_cloud.agent import get_version

        print(f"GX Agent version: {get_version()}")
        return

    from great_expectations_cloud.agent import run_agent

    run_agent()


if __name__ == "__main__":
    main()
