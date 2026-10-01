import pytest
from dagster._core.definitions.utils import (
    validate_group_name,
    normalize_group_name,
    DEFAULT_GROUP_NAME,
)
from dagster._core.errors import DagsterInvalidDefinitionError


def test_validate_group_name_accepts_deep_hierarchies():
    # These examples previously triggered a validation error; they should now be accepted.
    for name in (
        "logistics/inbound/replenishment",
        "payroll/emea/q3_close",
        "x_1/y_2/z_3",
        "marketing/foo/bar",
    ):
        # Should not raise an exception
        validate_group_name(name)




def test_normalize_group_name_idempotent_and_defaults():
    # Idempotence: applying normalize_group_name twice yields the same result.
    test_names = [
        "marketing/foo/bar",
        "logistics/inbound/replenishment",
        "singlelevel",
        None,
    ]
    for name in test_names:
        first = normalize_group_name(name)
        second = normalize_group_name(first)
        assert second == first

    # When None is provided, the default group name is used.
    assert normalize_group_name(None) == DEFAULT_GROUP_NAME
    # The default group name itself is stable under normalization.
    assert normalize_group_name(DEFAULT_GROUP_NAME) == DEFAULT_GROUP_NAME
