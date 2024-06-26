from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.sqlalchemy import inspect
from great_expectations.core.http import create_session
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.datasource.fluent.interfaces import Datasource, TestConnectionError
from typing_extensions import override

from great_expectations_cloud.agent.actions import ActionResult, AgentAction
from great_expectations_cloud.agent.config import (
    GxAgentEnvVars,
    generate_config_validation_error_text,
)
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.exceptions import ErrorCode, raise_with_error_code
from great_expectations_cloud.agent.models import DraftDatasourceConfigEvent

if TYPE_CHECKING:
    from great_expectations.compatibility.sqlalchemy.engine import Inspector


class DraftDatasourceConfigAction(AgentAction[DraftDatasourceConfigEvent]):
    # TODO: New actions need to be created that are compatible with GX v1 and registered for v1.
    #  This action is registered for v0, see register_event_action()

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

        if isinstance(datasource, SQLDatasource):
            table_names = self._get_table_names(datasource=datasource)
            self._update_table_names_list(config_id=event.config_id, table_names=table_names)

        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[],
        )

    def _get_table_names(self, datasource: Datasource) -> list[str]:
        inspector: Inspector = inspect(datasource.get_engine())
        return inspector.get_table_names()  # type: ignore[no-any-return] # method returns a list of strings

    def _update_table_names_list(self, config_id: UUID, table_names: list[str]) -> None:
        try:
            cloud_config = GxAgentEnvVars()
        except pydantic.ValidationError as validation_err:
            raise RuntimeError(
                generate_config_validation_error_text(validation_err)
            ) from validation_err

        session = create_session(access_token=cloud_config.gx_cloud_access_token)
        response = session.patch(
            url=f"{cloud_config.gx_cloud_base_url}/organizations/"
            f"{cloud_config.gx_cloud_organization_id}/datasources/drafts/{config_id}",
            json={"table_names": table_names},
        )
        if not response.ok:
            raise RuntimeError(  # noqa: TRY003 # one off error
                f"DraftDatasourceConfigAction encountered an error while connecting to GX Cloud. "
                f"Unable to update "
                f"table_names for Draft Config with ID"
                f"={config_id}.",
            )

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
