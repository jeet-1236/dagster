import dagster as dg
import pytest
from dagster._core.execution.plan.inputs import join_and_hash


def test_multiple_upstream_values():
    @dg.op
    def a():
        return 1

    @dg.op
    def b():
        return 2

    @dg.op
    def c():
        return 3

    @dg.op
    def collect(items):
        return items

    @dg.job
    def my_job():
        collect([a(), b(), c()])

    result = my_job.execute_in_process()
    assert result.success
    assert result.output_for_node("collect") == [1, 2, 3]


def test_multiple_upstream_with_optional_skip():
    @dg.op
    def a():
        return 1

    @dg.op(out={"skip": dg.Out(is_required=False)})
    def skip(_):
        return
        yield  # pragma: no cover

    @dg.op
    def collect(items):
        return items

    @dg.job
    def my_job():
        collect([a(), skip(), a()])

    result = my_job.execute_in_process()
    assert result.success
    # The optional skips should be omitted from the list
    assert result.output_for_node("collect") == [1, 1]
