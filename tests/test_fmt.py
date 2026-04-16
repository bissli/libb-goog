"""Tests for sheets._fmt() value formatting.
"""
import pytest
from goog.sheets import _fmt


@pytest.mark.parametrize(('input_val', 'expected'), [
    ('1,234.56', 1234.56),
    ('1,234', 1234),
    ('10,000,000', 10000000),
    ('(1,234)', -1234),
    ('(5.67)', -5.67),
    ('(100)', -100),
    ('45.6%', 45.6),
    ('-12.3%', -12.3),
    ('100%', 100),
    ('123', 123),
    ('-456', -456),
    ('12.34', 12.34),
    ('-0.5', -0.5),
    ('hello', 'hello'),
    ('abc123', 'abc123'),
    ('', None),
    ('-', None),
    (None, None),
    ('  123  ', 123),
    ('  hello  ', 'hello'),
])
def test_fmt(input_val, expected):
    """Verify _fmt handles commas, parens, percents, types, and edge cases.
    """
    result = _fmt(input_val)
    assert result == expected, f'_fmt({input_val!r}) = {result!r}, expected {expected!r}'
    if expected is not None:
        assert type(result) is type(expected)
