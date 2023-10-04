from __future__ import annotations

import argparse
import dataclasses as dc
import logging

from great_expectations_cloud.agent.logging_cfg import LogLevel, configure_logger

LOGGER = logging.getLogger(__name__)


@dc.dataclass(frozen=True)
class Arguments:
    log_level: LogLevel


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
    args = parser.parse_args()
    return Arguments(
        log_level=args.log_level,
    )


def main() -> None:
    args: Arguments = _parse_args()
    configure_logger(args.log_level)

    # lazy import to ensure our cli is fast and responsive
    from great_expectations_cloud.agent import run_agent

    run_agent()


if __name__ == "__main__":
    main()
