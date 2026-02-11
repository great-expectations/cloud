from __future__ import annotations

import logging
import socket
from concurrent.futures import TimeoutError as FuturesTimeoutError
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.models import (
    CreatedResource,
    RunCheckpointEvent,
    RunScheduledCheckpointEvent,
    RunWindowCheckpointEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent.interfaces import DataAsset, Datasource

from great_expectations.datasource.fluent.interfaces import TestConnectionError

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)
DATASOURCE_TEST_CONNECTION_TIMEOUT_SECONDS: Final[int] = 600


class RunCheckpointAction(AgentAction[RunCheckpointEvent]):
    @override
    def run(self, event: RunCheckpointEvent, id: str) -> ActionResult:
        return run_checkpoint(self._context, event, id)


class MissingCheckpointNameError(ValueError):
    """Property checkpoint_name is required but not present."""


# using a dataclass because we don't want the pydantic behavior of copying objects
@dataclass
class DataSourceAssets:
    data_source: Datasource[Any, Any]
    assets_by_name: dict[str, DataAsset[Any, Any]]


def check_datasource_and_assets_connection(
    ds_name: str,
    data_sources_assets: DataSourceAssets,
    log_extra: dict[str, Any],
) -> None:
    """
    Test connection to a datasource and its assets.

    This function is designed to be run in a worker thread with a timeout.

    Args:
        ds_name: Name of the datasource
        data_sources_assets: DataSourceAssets containing datasource and assets
        log_extra: Extra logging context

    Raises:
        TestConnectionError: If connection test fails
    """
    data_source = data_sources_assets.data_source
    LOGGER.debug(
        "Testing datasource connection",
        extra={**log_extra, "datasource_name": ds_name},
    )
    data_source.test_connection(test_assets=False)  # raises `TestConnectionError` on failure
    LOGGER.debug(
        "Datasource connection successful",
        extra={**log_extra, "datasource_name": ds_name},
    )

    for asset_name, data_asset in data_sources_assets.assets_by_name.items():
        LOGGER.debug(
            "Testing data asset connection",
            extra={**log_extra, "datasource_name": ds_name, "asset_name": asset_name},
        )
        data_asset.test_connection()  # raises `TestConnectionError` on failure
        LOGGER.debug(
            "Data asset connection successful",
            extra={**log_extra, "datasource_name": ds_name, "asset_name": asset_name},
        )


def check_datasource_and_assets_connection_with_timeout(
    ds_name: str,
    data_sources_assets: DataSourceAssets,
    log_extra: dict[str, Any],
    timeout: int = DATASOURCE_TEST_CONNECTION_TIMEOUT_SECONDS,
) -> None:
    """
    Test connection to a datasource and its assets with a timeout.

    Runs the connection test in a worker thread and enforces a timeout.
    If the timeout is exceeded, raises TestConnectionError.

    Args:
        ds_name: Name of the datasource
        data_sources_assets: DataSourceAssets containing datasource and assets
        log_extra: Extra logging context
        timeout: Timeout in seconds (default: DATASOURCE_TEST_CONNECTION_TIMEOUT_SECONDS)

    Raises:
        TestConnectionError: If connection test fails or timeout is exceeded
    """
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        future = executor.submit(
            check_datasource_and_assets_connection, ds_name, data_sources_assets, log_extra
        )
        try:
            future.result(timeout=timeout)
        except FuturesTimeoutError:
            LOGGER.warning(
                f"Datasource connection test timed out after {timeout} seconds",
                extra={**log_extra, "datasource_name": ds_name, "timeout_seconds": timeout},
            )
            raise TestConnectionError(
                message=f"Datasource '{ds_name}' was unresponsive after {timeout} seconds"
            ) from None
    finally:
        # shutdown(wait=False) is necessary here because the default shutdown(wait=True)
        # would block until the worker thread completes. On timeout, the worker thread
        # may still be blocked on an unresponsive datasource connection, and waiting for
        # it would defeat the purpose of the timeout. Python threads cannot be forcibly
        # interrupted, so the abandoned thread will continue running in the background
        # until the blocking I/O call returns or the process exits.
        executor.shutdown(wait=False)


def run_checkpoint(
    context: CloudDataContext,
    event: RunCheckpointEvent | RunScheduledCheckpointEvent | RunWindowCheckpointEvent,
    id: str,
    expectation_parameters: dict[str, Any] | None = None,
) -> ActionResult:
    """Run a checkpoint and return the result."""
    hostname = socket.gethostname()
    log_extra = {
        "correlation_id": id,
        "checkpoint_name": event.checkpoint_name,
        "hostname": hostname,
    }

    # the checkpoint_name property on possible events is optional for backwards compatibility,
    # but this action requires it in order to run:
    if not event.checkpoint_name:
        raise MissingCheckpointNameError

    LOGGER.debug("Fetching checkpoint from context", extra=log_extra)
    checkpoint = context.checkpoints.get(name=event.checkpoint_name)
    LOGGER.debug(
        "Checkpoint fetched successfully",
        extra={
            **log_extra,
            "validation_definitions_count": len(checkpoint.validation_definitions),
        },
    )

    # only GX-managed Checkpoints are currently validated here and they contain only one validation definition, but
    # the Checkpoint does allow for multiple validation definitions so we'll be defensive and ensure we only test each
    # source/asset once
    data_sources_assets_by_data_source_name: dict[str, DataSourceAssets] = {}
    for vd in checkpoint.validation_definitions:
        ds = vd.data_source
        ds_name = ds.name
        # create assets by name dict
        if ds_name not in data_sources_assets_by_data_source_name:
            data_sources_assets_by_data_source_name[ds_name] = DataSourceAssets(
                data_source=ds, assets_by_name={}
            )
        data_sources_assets_by_data_source_name[ds_name].assets_by_name[vd.asset.name] = vd.asset

    # Test connections to all datasources and assets
    for ds_name, data_sources_assets in data_sources_assets_by_data_source_name.items():
        check_datasource_and_assets_connection_with_timeout(ds_name, data_sources_assets, log_extra)

    LOGGER.debug(
        "Running checkpoint",
        extra={
            **log_extra,
            "datasources_count": len(data_sources_assets_by_data_source_name),
            "has_expectation_parameters": expectation_parameters is not None,
        },
    )
    checkpoint_run_result = checkpoint.run(
        batch_parameters=event.splitter_options, expectation_parameters=expectation_parameters
    )
    LOGGER.debug(
        "Checkpoint run completed",
        extra={
            **log_extra,
            "run_results_count": len(checkpoint_run_result.run_results),
        },
    )

    validation_results = checkpoint_run_result.run_results
    created_resources = []
    for key in validation_results.keys():
        suite_validation_result = validation_results[key]
        if suite_validation_result.id is None:
            raise RuntimeError(f"SuiteValidationResult.id is None for key: {key}")  # noqa: TRY003
        created_resource = CreatedResource(
            resource_id=suite_validation_result.id,
            type="SuiteValidationResult",
        )
        created_resources.append(created_resource)

    LOGGER.debug(
        "Checkpoint action completed successfully",
        extra={
            **log_extra,
            "created_resources_count": len(created_resources),
        },
    )

    return ActionResult(
        id=id,
        type=event.type,
        created_resources=created_resources,
    )


register_event_action("1", RunCheckpointEvent, RunCheckpointAction)
