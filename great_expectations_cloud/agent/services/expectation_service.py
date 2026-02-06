from __future__ import annotations

import logging
from http import HTTPStatus
from typing import TYPE_CHECKING
from urllib.parse import urljoin
from uuid import UUID

from great_expectations.core.http import create_session

from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import (
    ExistingExpectationContext,
)
from great_expectations_cloud.agent.services.exceptions import MissingCloudConfigError

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


logger = logging.getLogger(__name__)


class ListExpectationsError(Exception):
    def __init__(self, status_code: int, asset_id: str | UUID):
        message = f"Failed with status {status_code} for asset {asset_id}"
        super().__init__(f"Could not get expectations: {message}")


class ExpectationService:
    def __init__(self, context: CloudDataContext):
        self._context = context

    def get_existing_expectations_by_data_asset(
        self,
        data_source_name: str,
        data_asset_name: str,
    ) -> list[ExistingExpectationContext]:
        """
        Args:
            data_source_name: Filter by data source
            data_asset_name: Filter by data asset

        Returns:
            A list of structured expectation context objects, intended to be used as context for the LLM.

        """
        if self._context.ge_cloud_config is None:
            raise MissingCloudConfigError()

        data_source = self._context.data_sources.get(data_source_name)
        asset = data_source.get_asset(data_asset_name)
        organization_id = self._context.ge_cloud_config.organization_id
        workspace_id = self._context.ge_cloud_config.workspace_id

        url = urljoin(
            base=self._context.ge_cloud_config.base_url,
            url=f"/api/v1/organizations/{organization_id}/workspaces/{workspace_id}/expectations?data_asset_id={asset.id}",
        )

        with create_session(access_token=self._context.ge_cloud_config.access_token) as session:
            response = session.get(url=url)
            if response.status_code != HTTPStatus.OK:
                raise ListExpectationsError(
                    status_code=response.status_code,
                    asset_id=asset.id,
                )

        data = response.json()["data"] or []
        result = []

        for item in data:
            config = item["config"]
            # Extract domain from kwargs.column, default to "table"
            domain = config.get("kwargs", {}).get("column", "table")

            context = ExistingExpectationContext(
                domain=domain,
                expectation_type=config.get("expectation_type"),  # guaranteed to exist
                description=config.get("description", "") or "",  # not guaranteed to exist
            )
            result.append(context)

        return result
