"""Complete-plan validation without dataset reads or transformations."""

import copy
import sys
from functools import partial
from types import MappingProxyType
from unittest.mock import Mock

import pytest
import yaml

from flint_core import FlintProject, Node, Output, Pipeline, WritePolicy
from flint_core.runtime.errors import PlanError

from .test_project import make_project


@pytest.fixture(autouse=True, params=["spark", "pandas"])
def planning_engine(request, monkeypatch):
    original = make_project

    def configured_project(root, **options):
        result = original(root, **options)
        if request.param == "spark":
            path = root / "bindings.yaml"
            data = yaml.safe_load(path.read_text())
            for binding in data["bindings"].values():
                binding.update(adapter="spark.delta", format="delta")
                binding["target"]["kind"] = "delta_table"
            path.write_text(yaml.safe_dump(data), encoding="utf-8")
        return result

    module = sys.modules[__name__]
    monkeypatch.setattr(module, "make_project", configured_project)
    monkeypatch.setattr(module, "Pipeline", partial(Pipeline, engine=request.param))


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


def test_positional_only_and_optional_atomicity_are_rejected(tmp_path):
    def positional(source, /):
        return source

    with pytest.raises(PlanError):
        configured(tmp_path, [Node("node", positional, {"source": "source"}, Output("result"))]).compile()
    node = Node("node", lambda: None, {}, Output("result", WritePolicy(atomicity="optional")))
    with pytest.raises(PlanError):
        configured(tmp_path, [node]).compile()


def test_no_default_does_not_imply_run_all(tmp_path):
    project = FlintProject.load(make_project(tmp_path, default=False))
    with pytest.raises(PlanError):
        project.compile()


def test_newly_ready_node_keeps_priority_over_later_independent_node(tmp_path):
    project = FlintProject.load(make_project(tmp_path))
    manifest = tmp_path / "definitions.yaml"
    definitions = yaml.safe_load(manifest.read_text())
    for name in ("other.one", "other.two"):
        definitions["datasets"][name] = copy.deepcopy(definitions["datasets"]["result"])
    manifest.write_text(yaml.safe_dump(definitions), encoding="utf-8")
    project.catalog.reload()
    project._pipelines = MappingProxyType(
        {
            "main": Pipeline(
                "main",
                [
                    Node("b", lambda source: source, {"source": "source"}, Output("result")),
                    Node("c", lambda: None, {}, Output("other.one")),
                    Node("a", lambda: None, {}, Output("source")),
                    Node("d", lambda: None, {}, Output("other.two")),
                ],
            )
        }
    )
    assert [node.name for node in project.compile().nodes] == ["c", "a", "b", "d"]


@pytest.mark.parametrize("kind", ["coroutine", "generator", "async_generator"])
@pytest.mark.parametrize("wrapper", ["function", "partial", "instance", "method", "partial_instance"])
def test_deferred_transformations_fail_before_any_execution(tmp_path, kind, wrapper):
    async def coroutine():
        return None

    def generator():
        yield None

    async def async_generator():
        yield None

    function = {"coroutine": coroutine, "generator": generator, "async_generator": async_generator}[kind]

    class Callable:
        __call__ = staticmethod(function)

    transformation = {
        "function": function,
        "partial": partial(function),
        "instance": Callable(),
        "method": Callable().__call__,
        "partial_instance": partial(Callable()),
    }[wrapper]
    earlier = Node("earlier", lambda: None, {}, Output("source"))
    invalid = Node("deferred", transformation, {}, Output("result"))
    with pytest.raises(PlanError) as caught:
        configured(tmp_path, [earlier, invalid]).compile()
    assert caught.value.code == "unsupported_callable"
    assert caught.value.node == "deferred"
    assert not (tmp_path / "data").exists()


def test_signature_error_identifies_node_without_raw_function_error(tmp_path):
    with pytest.raises(PlanError) as caught:
        configured(tmp_path, [Node("bad_signature", lambda required: None, {}, Output("result"))]).compile()
    assert caught.value.code == "signature_mismatch"
    assert caught.value.node == "bad_signature"


def test_output_error_distinguishes_unknown_definition_from_duplicate_producer(tmp_path):
    with pytest.raises(PlanError) as caught:
        configured(tmp_path, [Node("unknown", lambda: None, {}, Output("missing"))]).compile()
    assert (caught.value.code, caught.value.node, caught.value.dataset) == ("unknown_output", "unknown", "missing")
    with pytest.raises(PlanError) as caught:
        configured(
            tmp_path,
            [
                Node("first", lambda: None, {}, Output("result")),
                Node("second", lambda: None, {}, Output("result")),
            ],
        ).compile()
    assert (caught.value.code, caught.value.node, caught.value.dataset) == ("duplicate_producer", "second", "result")
