from __future__ import annotations

from typing import Any
from uuid import UUID

from great_expectations.compatibility import pydantic
from great_expectations.core.http import create_session
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from typing_extensions import override

from great_expectations_cloud.agent.actions import ActionResult, AgentAction
from great_expectations_cloud.agent.config import (
    GxAgentEnvVars,
    generate_config_validation_error_text,
)
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.exceptions import ErrorCode, raise_with_error_code
from great_expectations_cloud.agent.models import DraftDatasourceConfigEvent


class DraftDatasourceConfigAction(AgentAction[DraftDatasourceConfigEvent]):
    @override
    def run(self, event: DraftDatasourceConfigEvent, id: str) -> ActionResult:
        try:
            return self.check_draft_datasource_config(event=event, id=id)
        except TestConnectionError as e:
            # NOTE: This (string matching) is a temporary solution to handle the error codes for the 0.18.x release
            # as a demo / proof of concept of the error code system.
            # It will be replaced in the GX Core 1.0.0 actions with more specific exceptions that can be caught.
            # These exceptions will be raised with the appropriate error code, and also contain parameters of any
            # relevant information that can be used to construct a user-friendly error message.
            if "Incorrect username or password was specified" in str(
                e
            ) and "snowflake.connector.errors.DatabaseError" in str(e):
                raise_with_error_code(e=e, error_code=ErrorCode.WRONG_USERNAME_OR_PASSWORD)
            else:
                raise_with_error_code(e=e, error_code=ErrorCode.GENERIC_UNHANDLED_ERROR)

    def check_draft_datasource_config(
        self, event: DraftDatasourceConfigEvent, id: str
    ) -> ActionResult:
        draft_config = self.get_draft_config(config_id=event.config_id)
        datasource_type = draft_config.get("type", None)
        if datasource_type is None:
            raise TypeError(  # noqa: TRY003 # one off error
                "The DraftDatasourceConfigAction can only be used with a "
                "fluent-style Data Source."
            )
        try:
            datasource_cls = self._context.sources.type_lookup[datasource_type]
        except LookupError as exc:
            raise TypeError(  # noqa: TRY003 # one off error
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
            raise RuntimeError(  # noqa: TRY003 # one off error
                "DraftDatasourceConfigAction encountered an error while " "connecting to GX Cloud"
            )
        data = response.json()
        try:
            return data["data"]["attributes"]["draft_config"]  # type: ignore[no-any-return]
        except KeyError as e:
            raise RuntimeError(  # noqa: TRY003 # one off error
                "Malformed response received from GX Cloud"
            ) from e


register_event_action("0", DraftDatasourceConfigEvent, DraftDatasourceConfigAction)
