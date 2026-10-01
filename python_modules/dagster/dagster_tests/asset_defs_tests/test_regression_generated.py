import pytest
from dagster._core.definitions.asset_key import AssetKey, key_prefix_from_coercible


def test_escaped_user_string_roundtrip_complex():
    # AssetKey with both slashes and backslashes in its parts.
    key = AssetKey(["a/b\\c", "d/e\\f"])
    escaped = key.to_escaped_user_string()
    # The escaped representation must contain escaped slashes (\/) and escaped backslashes (\\).
    assert r"\/" in escaped
    assert r"\\" in escaped
    # Round‑trip through the escaped parser should yield the original key.
    roundtrip = AssetKey.from_escaped_user_string(escaped)
    assert roundtrip == key
