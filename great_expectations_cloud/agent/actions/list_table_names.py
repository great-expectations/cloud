from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations.core.http import create_session
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.exceptions import GXCloudError
from sqlalchemy import inspect
from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.models import (
    ListTableNamesEvent,
)

if TYPE_CHECKING:
    from sqlalchemy.engine.reflection import Inspector


class ListTableNamesAction(AgentAction[ListTableNamesEvent]):
    # TODO: New actions need to be created that are compatible with GX v1 and registered for v1.
    #  This action is registered for v0, see register_event_action()

    @override
    def run(self, event: ListTableNamesEvent, id: str) -> ActionResult:
        datasource_name: str = event.datasource_name
        datasource = self._context.data_sources.get(name=datasource_name)
        if not isinstance(datasource, SQLDatasource):
            raise TypeError(  # noqa: TRY003 # one off error
                f"This operation requires a SQL Data Source but got {type(datasource).__name__}."
            )

        inspector: Inspector = inspect(datasource.get_engine())
        table_names: list[str] = inspector.get_table_names()
        view_names: list[str] = inspector.get_view_names()

        self._add_or_update_table_names_list(
            datasource_id=str(datasource.id), table_names=[*table_names, *view_names]
        )

        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[],
        )

    def _add_or_update_table_names_list(self, datasource_id: str, table_names: list[str]) -> None:
        with create_session(access_token=self._auth_key) as session:
            response = session.put(
                url=f"{self._base_url}/api/v1/organizations/"
                f"{self._organization_id}/table-names/{datasource_id}",
                json={"data": {"table_names": table_names}},
            )
        if response.status_code != 200:  # noqa: PLR2004
            raise GXCloudError(
                message=f"ListTableNamesAction encountered an error while connecting to GX Cloud. "
                f"Unable to update "
                f"table_names for Data Source with ID"
                f"={datasource_id}.",
                response=response,
            )


register_event_action("1", ListTableNamesEvent, ListTableNamesAction)
