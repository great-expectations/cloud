from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final

import great_expectations as gx
import pytest

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext

LOGGER: Final = logging.getLogger("tests")


@pytest.fixture(scope="module")
def cloud_base_url() -> str:
    return "http://localhost:5000"


@pytest.fixture(scope="module")
def cloud_organization_id() -> str:
    return "0ccac18e-7631-4bdd-8a42-3c35cce574c6"


@pytest.fixture(scope="module")
def cloud_access_token() -> str:
    return "ad2a171fe8134d3f90ecd8af16abf3c5.V1.6syarSNu5a3UNJcnPJsj87TKzKflZr3zwz9ChNCevHSLJ06TmrFY9d1gGpIC67lezYQq99dBgK9iYu2xSS_X5A"


@pytest.fixture(scope="module")
def context(
    cloud_base_url: str, cloud_organization_id: str, cloud_access_token: str
) -> CloudDataContext:
    context = gx.get_context(
        cloud_mode=True,
        cloud_base_url=cloud_base_url,
        cloud_organization_id=cloud_organization_id,
        cloud_access_token=cloud_access_token,
        # cloud_base_url=os.environ.get("GX_CLOUD_BASE_URL"),
        # cloud_organization_id=os.environ.get("GX_CLOUD_ORGANIZATION_ID"),
        # cloud_access_token=os.environ.get("GX_CLOUD_ACCESS_TOKEN"),
    )
    # assert isinstance(context, CloudDataContext)
    return context


# @pytest.fixture(scope="function")
# def empty_ge_cloud_data_context_config(
#     ge_cloud_base_url, ge_cloud_organization_id, ge_cloud_access_token
# ):
#     config_yaml_str = f"""
# stores:
#   default_evaluation_parameter_store:
#     class_name: EvaluationParameterStore
#   default_expectations_store:
#     class_name: ExpectationsStore
#     store_backend:
#       class_name: {GXCloudStoreBackend.__name__}
#       ge_cloud_base_url: {ge_cloud_base_url}
#       ge_cloud_resource_type: expectation_suite
#       ge_cloud_credentials:
#         access_token: {ge_cloud_access_token}
#         organization_id: {ge_cloud_organization_id}
#       suppress_store_backend_id: True
#   default_validations_store:
#     class_name: ValidationsStore
#     store_backend:
#       class_name: {GXCloudStoreBackend.__name__}
#       ge_cloud_base_url: {ge_cloud_base_url}
#       ge_cloud_resource_type: validation_result
#       ge_cloud_credentials:
#         access_token: {ge_cloud_access_token}
#         organization_id: {ge_cloud_organization_id}
#       suppress_store_backend_id: True
#   default_checkpoint_store:
#     class_name: CheckpointStore
#     store_backend:
#       class_name: {GXCloudStoreBackend.__name__}
#       ge_cloud_base_url: {ge_cloud_base_url}
#       ge_cloud_resource_type: checkpoint
#       ge_cloud_credentials:
#         access_token: {ge_cloud_access_token}
#         organization_id: {ge_cloud_organization_id}
#       suppress_store_backend_id: True
#   default_profiler_store:
#     class_name: ProfilerStore
#     store_backend:
#       class_name: {GXCloudStoreBackend.__name__}
#       ge_cloud_base_url: {ge_cloud_base_url}
#       ge_cloud_resource_type: profiler
#       ge_cloud_credentials:
#         access_token: {ge_cloud_access_token}
#         organization_id: {ge_cloud_organization_id}
#       suppress_store_backend_id: True
# evaluation_parameter_store_name: default_evaluation_parameter_store
# expectations_store_name: default_expectations_store
# validations_store_name: default_validations_store
# checkpoint_store_name: default_checkpoint_store
# profiler_store_name: default_profiler_store
# include_rendered_content:
#     globally: True
# """
#     yaml = YAML(typ="safe")
#     data_context_config_dict = yaml.load(config_yaml_str)
#     return DataContextConfig(**data_context_config_dict)


# @pytest.fixture
# def ge_cloud_id():
#     # Fake id but adheres to the format required of a UUID
#     return "731ee1bd-604a-4851-9ee8-bca8ffb32bce"


# @pytest.fixture
# def ge_cloud_base_url() -> str:
#     return "https://app.greatexpectations.fake.io"


# @pytest.fixture
# def ge_cloud_organization_id() -> str:
#     return str(uuid.UUID("12345678123456781234567812345678"))


# @pytest.fixture
# def ge_cloud_access_token() -> str:
#     return "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"


# @pytest.fixture
# def ge_cloud_config(ge_cloud_base_url, ge_cloud_organization_id, ge_cloud_access_token):
#     return GXCloudConfig(
#         base_url=ge_cloud_base_url,
#         organization_id=ge_cloud_organization_id,
#         access_token=ge_cloud_access_token,
#     )


# @pytest.fixture
# def empty_cloud_data_context(
#     # cloud_api_fake,
#     tmp_path: pathlib.Path,
#     empty_ge_cloud_data_context_config: DataContextConfig,
#     ge_cloud_config: GXCloudConfig,
# ) -> CloudDataContext:
#     project_path = tmp_path / "empty_data_context"
#     project_path.mkdir()
#     project_path_name: str = str(project_path)

#     context = CloudDataContext(
#         project_config=empty_ge_cloud_data_context_config,
#         context_root_dir=project_path_name,
#         cloud_base_url=ge_cloud_config.base_url,
#         cloud_access_token=ge_cloud_config.access_token,
#         cloud_organization_id=ge_cloud_config.organization_id,
#     )

#     return context
