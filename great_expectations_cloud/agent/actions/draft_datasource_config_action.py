from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from great_expectations.core.http import create_session
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.datasource.fluent.interfaces import Datasource, TestConnectionError
from sqlalchemy import inspect
from typing_extensions import override

from great_expectations_cloud.agent.actions import ActionResult, AgentAction
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.exceptions import ErrorCode, raise_with_error_code
from great_expectations_cloud.agent.models import DraftDatasourceConfigEvent

if TYPE_CHECKING:
    from sqlalchemy.engine import Inspector


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
            datasource_cls = self._context.data_sources.type_lookup[datasource_type]
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
        return inspector.get_table_names()

    def _update_table_names_list(self, config_id: UUID, table_names: list[str]) -> None:
        with create_session(access_token=self._auth_key) as session:
            url = f"{self._base_url}/api/v1/organizations/{self._organization_id}/draft-table-names/{config_id}"
            response = session.put(
                url=url,
                json={"data": {"table_names": table_names}},
            )
        if not response.ok:
            raise RuntimeError(  # noqa: TRY003 # one off error
                f"DraftDatasourceConfigAction encountered an error while connecting to GX Cloud. "
                f"Unable to update "
                f"table_names for Draft Config with ID"
                f"={config_id}.",
            )

    def get_draft_config(self, config_id: UUID) -> dict[str, Any]:
        resource_url = (
            f"{self._base_url}/api/v1/organizations/"
            f"{self._organization_id}/draft-datasources/{config_id}"
        )
        with create_session(access_token=self._auth_key) as session:
            response = session.get(resource_url)
            if not response.ok:
                raise RuntimeError(  # noqa: TRY003 # one off error
                    "DraftDatasourceConfigAction encountered an error while "
                    "connecting to GX Cloud"
                )
        data = response.json()
        try:
            return data["data"]["config"]  # type: ignore[no-any-return]
        except KeyError as e:
            raise RuntimeError(  # noqa: TRY003 # one off error
                "Malformed response received from GX Cloud"
            ) from e


register_event_action("1", DraftDatasourceConfigEvent, DraftDatasourceConfigAction)
