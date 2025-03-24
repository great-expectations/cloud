from __future__ import annotations

from dataclasses import dataclass

INPUT_RANGE = (0.0, 1.0)
OUTPUT_RANGE = (0.0, 10.0)
ROUND_PRECISION = 1


@dataclass
class TriangularInterpolationOptions:
    """Options for triangular interpolation."""

    input_range: tuple[float, float] = INPUT_RANGE
    output_range: tuple[float, float] = OUTPUT_RANGE
    round_precision: int = ROUND_PRECISION


def triangular_interpolation(
    value: float, options: TriangularInterpolationOptions | None = None
) -> float:
    """
    Maps a value between input range to a triangular pattern between output range.
    Values between input_min-midpoint map from output_min to output_max linearly.
    Values between midpoint-input_max map from output_max to output_min linearly.
    The midpoint is automatically calculated as the middle of the input range.
    Result is always rounded to the specified precision.

    Args:
        value (float): Input value between input_min and input_max
        options (TriangularInterpolationOptions, optional): Configuration options for the interpolation.
            If not provided, default values will be used.

    Returns:
        float: Interpolated value between output_min and output_max, rounded to specified precision
    """
    if options is None:
        options = TriangularInterpolationOptions()

    # Extract values from options for readability
    input_min, input_max = options.input_range
    output_min, output_max = options.output_range
    round_precision = options.round_precision

    # Calculate midpoint
    midpoint = input_min + (input_max - input_min) / 2

    # Ensure input is between input_min and input_max
    clamped_value = max(input_min, min(input_max, value))

    if clamped_value <= midpoint:
        # First half: linear mapping from input_min→output_min to midpoint→output_max
        normalized_value = (clamped_value - input_min) / (midpoint - input_min)
        result = output_min + normalized_value * (output_max - output_min)
    else:
        # Second half: linear mapping from midpoint→output_max to input_max→output_min
        normalized_value = (clamped_value - midpoint) / (input_max - midpoint)
        result = output_max - normalized_value * (output_max - output_min)

    # Round to specified precision
    result = round(result, round_precision)

    return result
