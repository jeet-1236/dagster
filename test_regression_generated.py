import dagster as dg
import pytest
from dagster._core.selector.subset_selector import (
    MAX_NUM,
    Traverser,
    fetch_connected,
    generate_dep_graph,
)


@dg.op
def return_one():
    return 1


@dg.op
def return_two():
    return 2


@dg.op(ins={"num1": dg.In(), "num2": dg.In()})
def add_nums(num1, num2):
    return num1 + num2


@dg.op(ins={"num": dg.In()})
def multiply_two(num):
    return num * 2


@dg.op(ins={"num": dg.In()})
def add_one(num):
    return num + 1


@dg.job(executor_def=dg.in_process_executor)
def foo_job():
    """return_one ---> add_nums --> multiply_two --> add_one
    return_two --|.
    """
    add_one(multiply_two(add_nums(return_one(), return_two())))


def _graph():
    return generate_dep_graph(foo_job)  # ty: ignore[invalid-argument-type]


def test_upstream_one_hop():
    """Depth=1 should return only the immediate upstream op, not its ancestors."""
    graph = _graph()
    traverser = Traverser(graph)
    assert traverser.fetch_upstream(item_name="multiply_two", depth=1) == {"add_nums"}


def test_downstream_two_hops():
    """Depth=2 downstream from a source should include ops up to two hops away, but not beyond."""
    graph = _graph()
    traverser = Traverser(graph)
    downstream_two = traverser.fetch_downstream(item_name="return_one", depth=2)
    assert downstream_two == {"add_nums", "multiply_two"}
    # Ensure the third hop is not included
    assert "add_one" not in downstream_two
