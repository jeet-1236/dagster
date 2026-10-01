import pytest
import dagster as dg
from dagster._core.definitions.partitions.partition_key_range import PartitionKeyRange


def test_get_partition_keys_in_range_is_inclusive():
    # List-keyed partitions in UI order
    partitions = ["ap_south", "ap_northeast", "eu_central", "us_west"]
    static_def = dg.StaticPartitionsDefinition(partitions)

    # Request a span that should include every key
    key_range = PartitionKeyRange(start="ap_south", end="us_west")
    result = static_def.get_partition_keys_in_range(key_range)

    # The bug left out the final key; the fix returns the full inclusive list
    assert result == partitions
