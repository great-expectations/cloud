from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from great_expectations.compatibility import pydantic
from great_expectations.core.http import create_session
from typing_extensions import override

from great_expectations_cloud.agent.actions import ActionResult, AgentAction
from great_expectations_cloud.agent.actions.compatibility import lookup_runner
from great_expectations_cloud.agent.config import (
    GxAgentEnvVars,
    generate_config_validation_error_text,
)
from great_expectations_cloud.agent.models import DraftDatasourceConfigEvent

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


class DraftDatasourceConfigAction(AgentAction[DraftDatasourceConfigEvent]):
    @override
    def run(self, event: DraftDatasourceConfigEvent, id: str) -> ActionResult:
        runner = lookup_runner()
        return runner.run_check_datasource_config(context=self._context, event=event, id=id)


def check_draft_datasource_config(
    context: CloudDataContext, event: DraftDatasourceConfigEvent, id: str
) -> ActionResult:
    draft_config = _get_draft_config(config_id=event.config_id)
    datasource_type = draft_config.get("type", None)
    if datasource_type is None:
        raise ValueError(
            "The DraftDatasourceConfigAction can only be used with a " "fluent-style Data Source."
        )
    try:
        datasource_cls = context.sources.type_lookup[datasource_type]
    except KeyError as exc:
        raise ValueError(
            "DraftDatasourceConfigAction received an unknown Data Source type."
        ) from exc
    datasource = datasource_cls(**draft_config)
    datasource._data_context = context
    datasource.test_connection(test_assets=True)  # raises `TestConnectionError` on failure
    return ActionResult(id=id, type=event.type, created_resources=[])


def _get_draft_config(config_id: UUID) -> dict[str, Any]:
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
