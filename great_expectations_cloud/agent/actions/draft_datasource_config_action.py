from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable
from uuid import UUID

import great_expectations as gx
from great_expectations.compatibility import pydantic
from great_expectations.core.http import create_session
from packaging.version import Version
from typing_extensions import override

from great_expectations_cloud.agent.actions import ActionResult, AgentAction
from great_expectations_cloud.agent.config import (
    GxAgentEnvVars,
    generate_config_validation_error_text,
)
from great_expectations_cloud.agent.models import DraftDatasourceConfigEvent

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext

if TYPE_CHECKING:
    from great_expectations_cloud.agent.models import Event

GX_VERSION = Version(gx.__version__)


# TODO: Placeholder for datasource config action registration / implementation. Consider moving to a better place.
#  Also consider moving to a class-based implementation and only doing the version check once on load.
def lookup_runner(context: CloudDataContext) -> Callable:  # TODO: Return signature?
    """Lookup the correct runner implementation based on the context version."""

    # if Version("0.18.0") <= GX_VERSION < Version("1.0.0"):
    #     return run_checkpoint_v0_18
    # elif GX_VERSION >= Version("1.0.0"):
    #     return run_checkpoint_v1_0
    raise NotImplementedError


def draft_datasource_config_action_impl(
    context: CloudDataContext, event: Event, id: str
) -> ActionResult:
    # TODO: Should the lookup be cached?
    # TODO: Should the lookup be of a class instead of a function? E.g. a class tied to the context version?
    runner = lookup_runner(context)
    return runner(context, event, id)


def draft_datasource_config_action_v0_18(
    context: CloudDataContext, event: Event, id: str
) -> ActionResult:
    # TODO: The code from the run() method in DraftDatasourceConfigAction should be moved here.
    raise NotImplementedError


class DraftDatasourceConfigAction(AgentAction[DraftDatasourceConfigEvent]):
    @override
    def run(self, event: DraftDatasourceConfigEvent, id: str) -> ActionResult:
        draft_config = self.get_draft_config(config_id=event.config_id)
        datasource_type = draft_config.get("type", None)
        if datasource_type is None:
            raise ValueError(
                "The DraftDatasourceConfigAction can only be used with a "
                "fluent-style Data Source."
            )
        try:
            datasource_cls = self._context.sources.type_lookup[datasource_type]
        except KeyError as exc:
            raise ValueError(
                "DraftDatasourceConfigAction received an unknown Data Source type."
            ) from exc
        datasource = datasource_cls(**draft_config)
        datasource._data_context = self._context
        datasource.test_connection(test_assets=True)  # raises `TestConnectionError` on failure
        return ActionResult(id=id, type=event.type, created_resources=[])

    def get_draft_config(self, config_id: UUID) -> dict[str, Any]:
        try:
            config = GxAgentEnvVars()
        except pydantic.ValidationError as validation_err:
            raise RuntimeError(
                generate_config_validation_error_text(validation_err)
            ) from validation_err
        resource_url = (
            f"{config.gx_cloud_base_url}/organizations/"
            f"{config.gx_cloud_organization_id}/datasources/drafts/{config_id}"
        )
        session = create_session(access_token=config.gx_cloud_access_token)
        response = session.get(resource_url)
        if not response.ok:
            raise RuntimeError(
                "DraftDatasourceConfigAction encountered an error while " "connecting to GX Cloud"
            )
        data = response.json()
        try:
            return data["data"]["attributes"]["draft_config"]  # type: ignore[no-any-return]
        except KeyError as e:
            raise RuntimeError("Malformed response received from GX Cloud") from e
