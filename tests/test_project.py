import pathlib
from typing import Final

import pytest
import tomli
from packaging.version import Version

PROJECT_ROOT: Final = pathlib.Path(__file__).parent.parent
PYPROJECT_TOML: Final = PROJECT_ROOT / "pyproject.toml"


@pytest.fixture
def min_gx_version() -> Version:
    pyproject_dict = tomli.loads(PYPROJECT_TOML.read_text())
    gx_version: str = pyproject_dict["tool"]["poetry"]["dependencies"][
        "great-expectations"
    ].replace("^", "")
    return Version(gx_version)


def test_great_expectations_is_installed(min_gx_version):
    import great_expectations

    assert Version(great_expectations.__version__) >= min_gx_version


if __name__ == "__main__":
    pytest.main([__file__, "-vv", "-rEf"])
