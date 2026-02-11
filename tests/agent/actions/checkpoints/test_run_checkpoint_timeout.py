from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import pytest
from great_expectations.datasource.fluent import Datasource
from great_expectations.datasource.fluent.interfaces import (
    DataAsset,
    TestConnectionError,
)

from great_expectations_cloud.agent.actions.run_checkpoint import (
    DATASOURCE_TEST_CONNECTION_TIMEOUT_SECONDS,
    DataSourceAssets,
    test_datasource_and_assets_connection,
    test_datasource_and_assets_connection_with_timeout,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit


@pytest.fixture
def mock_datasource(mocker: MockerFixture) -> Any:
    """Create a mock datasource."""
    datasource = mocker.Mock(spec=Datasource)
    datasource.name = "test-datasource"
    datasource.type = "sql"
    datasource.test_connection.return_value = None
    return datasource


@pytest.fixture
def mock_data_asset(mocker: MockerFixture) -> Any:
    """Create a mock data asset."""
    asset = mocker.Mock(spec=DataAsset)
    asset.name = "test-asset"
    asset.test_connection.return_value = None
    return asset


@pytest.fixture
def log_extra() -> dict[str, str]:
    """Create mock log_extra dict."""
    return {
        "correlation_id": "test-id",
        "checkpoint_name": "test-checkpoint",
        "hostname": "test-host",
    }


class TestTestDatasourceAndAssetsConnection:
    """Tests for _test_datasource_and_assets_connection function."""

    def test_successful_connection_single_asset(
        self, mock_datasource: Any, mock_data_asset: Any, log_extra: dict[str, str]
    ):
        """Test successful connection test with single asset."""
        data_sources_assets = DataSourceAssets(
            data_source=mock_datasource, assets_by_name={"test-asset": mock_data_asset}
        )

        test_datasource_and_assets_connection(
            ds_name="test-datasource", data_sources_assets=data_sources_assets, log_extra=log_extra
        )

        mock_datasource.test_connection.assert_called_once_with(test_assets=False)
        mock_data_asset.test_connection.assert_called_once()

    def test_successful_connection_multiple_assets(
        self, mock_datasource: Any, mocker: MockerFixture, log_extra: dict[str, str]
    ):
        """Test successful connection test with multiple assets."""
        asset1 = mocker.Mock(spec=DataAsset)
        asset1.name = "asset-1"
        asset2 = mocker.Mock(spec=DataAsset)
        asset2.name = "asset-2"
        asset3 = mocker.Mock(spec=DataAsset)
        asset3.name = "asset-3"

        data_sources_assets = DataSourceAssets(
            data_source=mock_datasource,
            assets_by_name={"asset-1": asset1, "asset-2": asset2, "asset-3": asset3},
        )

        test_datasource_and_assets_connection(
            ds_name="test-datasource", data_sources_assets=data_sources_assets, log_extra=log_extra
        )

        mock_datasource.test_connection.assert_called_once_with(test_assets=False)
        asset1.test_connection.assert_called_once()
        asset2.test_connection.assert_called_once()
        asset3.test_connection.assert_called_once()

    def test_successful_connection_no_assets(self, mock_datasource: Any, log_extra: dict[str, str]):
        """Test successful connection test with no assets."""
        data_sources_assets = DataSourceAssets(data_source=mock_datasource, assets_by_name={})

        test_datasource_and_assets_connection(
            ds_name="test-datasource", data_sources_assets=data_sources_assets, log_extra=log_extra
        )

        mock_datasource.test_connection.assert_called_once_with(test_assets=False)

    def test_datasource_connection_failure(
        self, mock_datasource: Any, mock_data_asset: Any, log_extra: dict[str, str]
    ):
        """Test datasource connection failure raises TestConnectionError."""
        mock_datasource.test_connection.side_effect = TestConnectionError(
            message="Connection failed"
        )
        data_sources_assets = DataSourceAssets(
            data_source=mock_datasource, assets_by_name={"test-asset": mock_data_asset}
        )

        with pytest.raises(TestConnectionError, match="Connection failed"):
            test_datasource_and_assets_connection(
                ds_name="test-datasource",
                data_sources_assets=data_sources_assets,
                log_extra=log_extra,
            )

        mock_datasource.test_connection.assert_called_once_with(test_assets=False)
        mock_data_asset.test_connection.assert_not_called()

    def test_asset_connection_failure(
        self, mock_datasource: Any, mock_data_asset: Any, log_extra: dict[str, str]
    ):
        """Test asset connection failure raises TestConnectionError."""
        mock_data_asset.test_connection.side_effect = TestConnectionError(
            message="Asset connection failed"
        )
        data_sources_assets = DataSourceAssets(
            data_source=mock_datasource, assets_by_name={"test-asset": mock_data_asset}
        )

        with pytest.raises(TestConnectionError, match="Asset connection failed"):
            test_datasource_and_assets_connection(
                ds_name="test-datasource",
                data_sources_assets=data_sources_assets,
                log_extra=log_extra,
            )

        mock_datasource.test_connection.assert_called_once_with(test_assets=False)
        mock_data_asset.test_connection.assert_called_once()

    def test_asset_connection_failure_multiple_assets(
        self, mock_datasource: Any, mocker: MockerFixture, log_extra: dict[str, str]
    ):
        """Test that when second asset fails, first asset was tested."""
        asset1 = mocker.Mock(spec=DataAsset)
        asset1.name = "asset-1"
        asset1.test_connection.return_value = None

        asset2 = mocker.Mock(spec=DataAsset)
        asset2.name = "asset-2"
        asset2.test_connection.side_effect = TestConnectionError(message="Asset 2 failed")

        data_sources_assets = DataSourceAssets(
            data_source=mock_datasource, assets_by_name={"asset-1": asset1, "asset-2": asset2}
        )

        with pytest.raises(TestConnectionError, match="Asset 2 failed"):
            test_datasource_and_assets_connection(
                ds_name="test-datasource",
                data_sources_assets=data_sources_assets,
                log_extra=log_extra,
            )

        mock_datasource.test_connection.assert_called_once_with(test_assets=False)
        asset1.test_connection.assert_called_once()
        asset2.test_connection.assert_called_once()


class TestTestDatasourceAndAssetsConnectionWithTimeout:
    """Tests for _test_datasource_and_assets_connection_with_timeout function."""

    def test_successful_connection_within_timeout(
        self, mock_datasource: Any, mock_data_asset: Any, log_extra: dict[str, str]
    ):
        """Test successful connection test that completes within timeout."""
        data_sources_assets = DataSourceAssets(
            data_source=mock_datasource, assets_by_name={"test-asset": mock_data_asset}
        )

        test_datasource_and_assets_connection_with_timeout(
            ds_name="test-datasource",
            data_sources_assets=data_sources_assets,
            log_extra=log_extra,
            timeout=5,
        )

        mock_datasource.test_connection.assert_called_once_with(test_assets=False)
        mock_data_asset.test_connection.assert_called_once()

    def test_connection_failure_propagated(
        self, mock_datasource: Any, mock_data_asset: Any, log_extra: dict[str, str]
    ):
        """Test that connection errors are propagated from worker thread."""
        mock_datasource.test_connection.side_effect = TestConnectionError(
            message="Connection failed"
        )
        data_sources_assets = DataSourceAssets(
            data_source=mock_datasource, assets_by_name={"test-asset": mock_data_asset}
        )

        with pytest.raises(TestConnectionError, match="Connection failed"):
            test_datasource_and_assets_connection_with_timeout(
                ds_name="test-datasource",
                data_sources_assets=data_sources_assets,
                log_extra=log_extra,
                timeout=5,
            )

        mock_datasource.test_connection.assert_called_once_with(test_assets=False)

    def test_timeout_raises_test_connection_error(
        self, mock_datasource: Any, mock_data_asset: Any, log_extra: dict[str, str]
    ):
        """Test that timeout raises TestConnectionError with appropriate message."""

        # Make test_connection sleep longer than the timeout
        def slow_connection(**kwargs):
            time.sleep(3)

        mock_datasource.test_connection.side_effect = slow_connection
        data_sources_assets = DataSourceAssets(
            data_source=mock_datasource, assets_by_name={"test-asset": mock_data_asset}
        )

        with pytest.raises(
            TestConnectionError,
            match="Datasource 'test-datasource' was unresponsive after 1 seconds",
        ):
            test_datasource_and_assets_connection_with_timeout(
                ds_name="test-datasource",
                data_sources_assets=data_sources_assets,
                log_extra=log_extra,
                timeout=1,
            )

        mock_datasource.test_connection.assert_called_once_with(test_assets=False)

    def test_timeout_with_asset_connection(
        self, mock_datasource: Any, mock_data_asset: Any, log_extra: dict[str, str]
    ):
        """Test timeout when asset connection is slow."""

        def slow_asset_connection():
            time.sleep(3)

        mock_data_asset.test_connection.side_effect = slow_asset_connection
        data_sources_assets = DataSourceAssets(
            data_source=mock_datasource, assets_by_name={"test-asset": mock_data_asset}
        )

        with pytest.raises(
            TestConnectionError,
            match="Datasource 'test-datasource' was unresponsive after 1 seconds",
        ):
            test_datasource_and_assets_connection_with_timeout(
                ds_name="test-datasource",
                data_sources_assets=data_sources_assets,
                log_extra=log_extra,
                timeout=1,
            )

        mock_datasource.test_connection.assert_called_once_with(test_assets=False)

    def test_uses_default_timeout_constant(
        self, mock_datasource: Any, mock_data_asset: Any, log_extra: dict[str, str]
    ):
        """Test that default timeout uses DATASOURCE_TEST_CONNECTION_TIMEOUT_SECONDS."""
        data_sources_assets = DataSourceAssets(
            data_source=mock_datasource, assets_by_name={"test-asset": mock_data_asset}
        )

        # Should not timeout with default value (600 seconds)
        test_datasource_and_assets_connection_with_timeout(
            ds_name="test-datasource", data_sources_assets=data_sources_assets, log_extra=log_extra
        )

        mock_datasource.test_connection.assert_called_once_with(test_assets=False)
        mock_data_asset.test_connection.assert_called_once()

    def test_custom_timeout_value(
        self, mock_datasource: Any, mock_data_asset: Any, log_extra: dict[str, str]
    ):
        """Test that custom timeout value can be specified."""
        data_sources_assets = DataSourceAssets(
            data_source=mock_datasource, assets_by_name={"test-asset": mock_data_asset}
        )

        # Should complete successfully with custom timeout
        test_datasource_and_assets_connection_with_timeout(
            ds_name="test-datasource",
            data_sources_assets=data_sources_assets,
            log_extra=log_extra,
            timeout=30,
        )

        mock_datasource.test_connection.assert_called_once_with(test_assets=False)
        mock_data_asset.test_connection.assert_called_once()

    def test_timeout_message_includes_datasource_name(
        self, mock_datasource: Any, mock_data_asset: Any, log_extra: dict[str, str]
    ):
        """Test that timeout error message includes the datasource name."""

        def slow_connection(**kwargs):
            time.sleep(3)

        mock_datasource.test_connection.side_effect = slow_connection
        mock_datasource.name = "my-special-datasource"

        data_sources_assets = DataSourceAssets(
            data_source=mock_datasource, assets_by_name={"test-asset": mock_data_asset}
        )

        with pytest.raises(
            TestConnectionError, match="Datasource 'my-special-datasource' was unresponsive"
        ):
            test_datasource_and_assets_connection_with_timeout(
                ds_name="my-special-datasource",
                data_sources_assets=data_sources_assets,
                log_extra=log_extra,
                timeout=1,
            )

    def test_timeout_message_includes_timeout_value(
        self, mock_datasource: Any, mock_data_asset: Any, log_extra: dict[str, str]
    ):
        """Test that timeout error message includes the timeout value."""

        def slow_connection(**kwargs):
            time.sleep(3)

        mock_datasource.test_connection.side_effect = slow_connection
        data_sources_assets = DataSourceAssets(
            data_source=mock_datasource, assets_by_name={"test-asset": mock_data_asset}
        )

        with pytest.raises(TestConnectionError, match="after 2 seconds"):
            test_datasource_and_assets_connection_with_timeout(
                ds_name="test-datasource",
                data_sources_assets=data_sources_assets,
                log_extra=log_extra,
                timeout=2,
            )

    def test_no_assets_with_timeout(self, mock_datasource: Any, log_extra: dict[str, str]):
        """Test timeout behavior with no assets."""
        data_sources_assets = DataSourceAssets(data_source=mock_datasource, assets_by_name={})

        test_datasource_and_assets_connection_with_timeout(
            ds_name="test-datasource",
            data_sources_assets=data_sources_assets,
            log_extra=log_extra,
            timeout=5,
        )

        mock_datasource.test_connection.assert_called_once_with(test_assets=False)

    def test_multiple_assets_timeout(
        self, mock_datasource: Any, mocker: MockerFixture, log_extra: dict[str, str]
    ):
        """Test timeout with multiple assets."""
        asset1 = mocker.Mock(spec=DataAsset)
        asset1.name = "asset-1"
        asset1.test_connection.return_value = None

        asset2 = mocker.Mock(spec=DataAsset)
        asset2.name = "asset-2"

        def slow_connection():
            time.sleep(3)

        asset2.test_connection.side_effect = slow_connection

        data_sources_assets = DataSourceAssets(
            data_source=mock_datasource, assets_by_name={"asset-1": asset1, "asset-2": asset2}
        )

        with pytest.raises(
            TestConnectionError, match="Datasource 'test-datasource' was unresponsive"
        ):
            test_datasource_and_assets_connection_with_timeout(
                ds_name="test-datasource",
                data_sources_assets=data_sources_assets,
                log_extra=log_extra,
                timeout=1,
            )

        mock_datasource.test_connection.assert_called_once_with(test_assets=False)


class TestTimeoutConstant:
    """Test the timeout constant value."""

    def test_timeout_constant_value(self):
        """Test that DATASOURCE_TEST_CONNECTION_TIMEOUT_SECONDS is set to 600."""
        assert DATASOURCE_TEST_CONNECTION_TIMEOUT_SECONDS == 600
