from __future__ import annotations

import logging
from enum import StrEnum
from http import HTTPStatus
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin
from uuid import UUID

from great_expectations.core.http import create_session

from great_expectations_cloud.agent.models import CreatedResource
from great_expectations_cloud.agent.services.exceptions import MissingCloudConfigError

if TYPE_CHECKING:
    import great_expectations.expectations as gxe
    from great_expectations.data_context import CloudDataContext

logger = logging.getLogger(__name__)


class CreatedResourceTypes(StrEnum):
    EXPECTATION = "Expectation"
    EXPECTATION_DRAFT_CONFIG = "ExpectationDraftConfig"


class ExpectationDraftConfigError(Exception):
    """Exception raised when expectation draft config operations fail."""

    def __init__(self, status_code: int, asset_id: str):
        message = f"Failed with status {status_code} for asset {asset_id}"
        super().__init__(f"Could not create expectation draft config: {message}")


class ExpectationDraftConfigService:
    """Service for creating expectation draft configs.

    This service encapsulates the logic for creating expectation draft configurations
    that can be shared between different actions like GenerateExpectationsAction and GenerateSqlExpectationAction.
    """

    def __init__(self, context: CloudDataContext, created_via: str):
        self._context = context
        self._created_via = created_via

    def create_expectation_draft_configs(
        self,
        data_source_name: str,
        data_asset_name: str,
        expectations: list[gxe.Expectation],
        event_id: str,
    ) -> list[CreatedResource]:
        """Create expectation draft configs for the given expectations.

        Args:
            data_source_name: Name of the data source
            data_asset_name: Name of the data asset
            expectations: List of expectations to create draft configs for
            event_id: Event ID to include in the Agent-Job-Id header for tracing

        Returns:
            List of CreatedResource objects representing the created draft configs

        Raises:
            ExpectationDraftConfigError: If the API request fails
        """
        if self._context.ge_cloud_config is None:
            raise MissingCloudConfigError()

        data_source = self._context.data_sources.get(data_source_name)
        asset = data_source.get_asset(data_asset_name)

        organization_id = self._context.ge_cloud_config.organization_id

        workspace_id = self._context.ge_cloud_config.workspace_id

        payload = {
            "data": [
                self._construct_expectation_draft_config_payload(
                    asset_id=str(asset.id),
                    expectation=expectation,
                    organization_id=UUID(organization_id),
                )
                for expectation in expectations
            ]
        }

        url = urljoin(
            base=self._context.ge_cloud_config.base_url,
            url=f"/api/v1/organizations/{organization_id}/workspaces/{workspace_id}/expectation-draft-configs",
        )

        headers = {}
        if event_id:
            headers["Agent-Job-Id"] = event_id

        with create_session(access_token=self._context.ge_cloud_config.access_token) as session:
            response = session.post(url=url, json=payload, headers=headers)
            if response.status_code != HTTPStatus.CREATED:
                raise ExpectationDraftConfigError(
                    status_code=response.status_code,
                    asset_id=str(asset.id),
                )

        created_resources = [
            CreatedResource(
                resource_id=str(resource["id"]), type=CreatedResourceTypes.EXPECTATION_DRAFT_CONFIG
            )
            for resource in response.json()["data"]
        ]

        return created_resources

    def create_single_expectation_draft_config(
        self,
        data_source_name: str,
        data_asset_name: str,
        expectation: gxe.Expectation,
        event_id: str,
    ) -> CreatedResource:
        """Create a single expectation draft config.

        Args:
            data_source_name: Name of the data source
            data_asset_name: Name of the data asset
            expectation: The expectation to create a draft config for
            event_id: Event ID to include in the Agent-Job-Id header for tracing

        Returns:
            CreatedResource representing the created draft config
        """
        created_resources = self.create_expectation_draft_configs(
            data_source_name=data_source_name,
            data_asset_name=data_asset_name,
            expectations=[expectation],
            event_id=event_id,
        )
        return created_resources[0]

    def _construct_expectation_draft_config_payload(
        self,
        asset_id: str,
        expectation: gxe.Expectation,
        organization_id: UUID,
    ) -> dict[str, Any]:
        """Construct the payload for a single expectation draft config.

        Args:
            asset_id: ID of the asset
            expectation: The expectation to create a payload for
            organization_id: Organization ID

        Returns:
            Dictionary payload for the expectation draft config
        """
        exp = self._construct_expectation_obj([expectation])[0]
        return {
            "asset_id": asset_id,
            "draft_expectation": exp,
            "organization_id": str(organization_id),
        }

    def _construct_expectation_obj(
        self,
        expectations: list[gxe.Expectation],
    ) -> list[dict[str, Any]]:
        """Construct expectation objects in the format expected by the API.

        Args:
            expectations: List of expectations to convert

        Returns:
            List of dictionaries representing expectation objects
        """
        payload = []
        for expectation in expectations:
            expectation_payload = expectation.configuration.to_json_dict()
            expectation_payload["autogenerated"] = True
            expectation_payload["created_via"] = self._created_via

            # Backend expects `expectation_type` instead of `type`:
            expectation_payload["expectation_type"] = expectation_payload.pop("type")
            payload.append(expectation_payload)

        return payload
