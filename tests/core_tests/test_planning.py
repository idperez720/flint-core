"""Complete-plan validation without dataset reads or transformations."""

from types import MappingProxyType
from unittest.mock import Mock

import pytest

from flint_core import FlintProject, Node, Output, Pipeline, WritePolicy
from flint_core.runtime.errors import PlanError

from .test_project import make_project


def configured(root, nodes):
    project = FlintProject.load(make_project(root))
    project._pipelines = MappingProxyType({"main": Pipeline("main", nodes)})
    return project


def test_two_arguments_can_consume_one_dataset(tmp_path):
    compute = Mock(side_effect=AssertionError("must not execute"))

    def combine(left, right):
        return compute(left, right)

    project = configured(tmp_path, [Node("combine", combine, {"left": "source", "right": "source"}, Output("result"))])
    plan = project.compile()
    assert plan.external_inputs == ("source",)
    assert plan.publications == ()
    compute.assert_not_called()


@pytest.mark.parametrize("failure", ["duplicate_object", "duplicate_name", "producer", "cycle", "signature", "unknown"])
def test_invalid_graph_is_rejected_before_execution(tmp_path, failure):
    first = Node("first", lambda: None, {}, Output("source"))
    second = Node("second", lambda value: value, {"value": "source"}, Output("result"))
    nodes = [first, second]
    if failure == "duplicate_object":
        nodes.append(first)
    elif failure == "duplicate_name":
        nodes[1] = Node("first", second.func, second.inputs, second.outputs)
    elif failure == "producer":
        nodes[1] = Node("second", lambda: None, {}, Output("source"))
    elif failure == "cycle":
        nodes[0] = Node("first", lambda value: value, {"value": "result"}, Output("source"))
    elif failure == "signature":
        nodes[1] = Node("second", lambda missing: missing, {"value": "source"}, Output("result"))
    else:
        nodes[1] = Node("second", second.func, {"value": "unknown"}, second.outputs)
    with pytest.raises(PlanError):
        configured(tmp_path, nodes).compile()
    assert not (tmp_path / "data").exists()


@pytest.mark.parametrize("signature", [lambda *args: None, lambda **kwargs: None])
def test_variadic_functions_are_rejected(tmp_path, signature):
    with pytest.raises(PlanError):
        configured(tmp_path, [Node("node", signature, {}, Output("result"))]).compile()


@pytest.mark.parametrize("mode", ["overwrite", "append", "error", "ignore"])
def test_unsupported_publication_modes_fail_at_compile(tmp_path, mode):
    node = Node("node", lambda: None, {}, Output("result", WritePolicy(mode=mode)))
    with pytest.raises(PlanError):
        configured(tmp_path, [node]).compile()


def test_unknown_or_mixed_selection_does_not_partially_compile(tmp_path):
    project = configured(tmp_path, [Node("node", lambda: None, {}, Output("result"))])
    for selection in ("missing", ["main", "missing"]):
        with pytest.raises(PlanError):
            project.compile(selection)


def test_stable_order_preserves_declaration_priority(tmp_path):
    later = Node("later", lambda source: source, {"source": "source"}, Output("result"))
    first = Node("first", lambda: None, {}, Output("source"))
    project = configured(tmp_path, [later, first])
    assert [node.name for node in project.compile().nodes] == ["first", "later"]


def test_explicit_publication_and_read_only_access(tmp_path):
    node = Node("node", lambda source: source, {"source": "source"}, Output("result", WritePolicy()))
    assert configured(tmp_path, [node]).compile().publications == ("result",)
    with pytest.raises(PlanError):
        configured(tmp_path, [Node("node", lambda: None, {}, Output("source", WritePolicy()))]).compile()
