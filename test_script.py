import numpy as np
import pytest

from script import get_percentiles


def test_get_percentiles_default_values():
    """Test get_percentiles with default values"""
    result = get_percentiles()
    expected_length = 99  # 0.01 to 0.99 by 0.01

    assert len(result) == expected_length
    assert result[0] == 0.01  # First value
    assert result[-1] == 0.99  # Last value
    assert np.isclose(result[1] - result[0], 0.01)  # Step size


def test_get_percentiles_custom_values():
    """Test get_percentiles with custom values"""
    result = get_percentiles(start=0.1, end=0.5, step=0.1)
    expected = np.array([0.1, 0.2, 0.3, 0.4, 0.5])

    assert len(result) == 5
    assert np.allclose(result, expected)


def test_get_percentiles_single_value():
    """Test get_percentiles with start equal to end"""
    result = get_percentiles(start=0.5, end=0.5, step=0.1)
    assert len(result) == 1
    assert result[0] == 0.5
