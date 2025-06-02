import pytest

from great_expectations_cloud.agent.utils import (
    TriangularInterpolationOptions,
    param_safe_unique_id,
    triangular_interpolation,
)


@pytest.mark.parametrize(
    "input_value,expected_output",
    [
        # Boundary values
        (0.0, 0.0),  # Minimum value
        (0.5, 10.0),  # Peak value
        (1.0, 0.0),  # Maximum value
        # First half range (0 to 0.5)
        (0.1, 2.0),
        (0.25, 5.0),
        # Second half range (0.5 to 1)
        (0.75, 5.0),
        (0.9, 2.0),
        # Outside range (should clamp)
        (-0.5, 0.0),
        (1.5, 0.0),
        # Rounding to one decimal place
        (0.123, 2.5),
        (0.876, 2.5),
    ],
)
def test_triangular_interpolation(input_value, expected_output):
    assert triangular_interpolation(input_value) == expected_output


def test_triangular_interpolation_custom_parameters():
    """Test triangular interpolation with custom parameters using options object."""
    # Test with custom input range (0-100) and output range (0-1)
    options_100_to_1 = TriangularInterpolationOptions(
        input_range=(0, 100),
        output_range=(0, 1),
        round_precision=2,
    )

    assert triangular_interpolation(value=0, options=options_100_to_1) == 0.0
    assert triangular_interpolation(value=50, options=options_100_to_1) == 1.0
    assert triangular_interpolation(value=100, options=options_100_to_1) == 0.0

    # Test different rounding precision
    options_precision = TriangularInterpolationOptions(round_precision=2)
    assert triangular_interpolation(value=0.123, options=options_precision) == 2.46

    # Test asymmetric ranges
    options_asymmetric = TriangularInterpolationOptions(
        input_range=(0, 10),
        output_range=(0, 100),
        round_precision=0,
    )
    assert triangular_interpolation(value=2, options=options_asymmetric) == 40

    # Test with specific values for more complex ranges
    options_shifted = TriangularInterpolationOptions(
        input_range=(-1, 1),
        output_range=(5, 15),
    )
    assert triangular_interpolation(value=0, options=options_shifted) == 15.0
    assert triangular_interpolation(value=-1, options=options_shifted) == 5.0
    assert triangular_interpolation(value=1, options=options_shifted) == 5.0


def test_get_param_safe_unique_id():
    assert len(param_safe_unique_id(16)) == 16
