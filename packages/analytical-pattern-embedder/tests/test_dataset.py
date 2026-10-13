import itertools
import json
import random

import pytest

from generate_dataset.config import REAL_APS_DIR
from generate_dataset.schemas.ap import AP, Task
from generate_dataset.schemas.operator import Operator, Port
from generate_dataset.step1_operators import _list_operator_in_aps
from generate_dataset.step2_aps import generate_operator_chains
from generate_dataset.step4_splits import _split_aps_and_tasks

TASK = ("text_to_sql", "run_sql", "explain")


def _ap_of(*labels: str, id: str = "") -> AP:
    """An AP of operators that have nothing but a label."""
    operators = [Operator(label=label, name=label, description="", inputs=[], outputs=[]) for label in labels]
    return AP(id=id, operators=operators, graph=None, description="")


def relation(task_chain, ap_chain):
    """What an AP made of `ap_chain` is to a task written for `task_chain`."""
    return _ap_of(*ap_chain).relation_to(_ap_of(*task_chain))


@pytest.mark.parametrize("ap, expected", [
    (("text_to_sql", "run_sql", "explain"), "exact"),
    # Building blocks: any contiguous part of the chain the task was written for.
    (("text_to_sql",), "building_block"),
    (("run_sql", "explain"), "building_block"),
    # Skipping a step, reordering, or branching off is a different pipeline.
    (("text_to_sql", "explain"), None),
    (("run_sql", "text_to_sql"), None),
    (("text_to_sql", "chart"), None),
    (("chart",), None),
    # Does what is asked and more: neither a match nor a mistake.
    (("text_to_sql", "run_sql", "explain", "report"), "superset"),
    (("translate", "text_to_sql", "run_sql", "explain"), "superset"),
])
def test_relation(ap, expected):
    assert relation(TASK, ap) == expected


def test_relation_to_a_task_written_for_a_single_operator():
    assert relation(("run_sql",), ("run_sql",)) == "exact"
    assert relation(("run_sql",), ("text_to_sql", "run_sql")) == "superset"
    assert relation(("run_sql",), ("text_to_sql",)) is None


def test_an_ap_takes_the_most_held_out_split_of_its_operators():
    tasks = lambda: [Task(text=str(i), style="user") for i in range(4)]
    aps = [_ap_of(*chain, id=id_)
           for id_, chain in (("1", ("a",)), ("2", ("a", "b")), ("3", ("a", "b", "c")))]
    by_ap = {ap.id: tasks() for ap in aps}
    training, evaluation = _split_aps_and_tasks(aps, by_ap, {"a": "train", "b": "dev", "c": "test"}, random.Random(0))
    assert [ap.split for ap in aps] == ["train", "dev", "test"]
    # A training AP also lends one task to dev and one to test: new wording, known AP.
    assert sorted(task.use for task in by_ap["1"]) == ["dev", "test", "train", "train"]
    assert {task.use for task in by_ap["2"]} == {"dev"}
    # The test APs are in the evaluation dataset only, with their test tasks.
    assert [ap.id for ap in training.aps] == ["1", "2"]
    assert [ap.id for ap in evaluation.aps] == ["1", "2", "3"]
    assert {task.use for task in evaluation.tasks["3"]} == {"test"}
    assert len(evaluation.tasks["1"]) == 1 and len(training.tasks["1"]) == 3


def _ap(*operators: dict) -> str:
    """An AP file with the given operators, as the smallest valid graph."""
    nodes = [{"id": f"00000000-0000-4000-8000-{i:012d}", **operator} for i, operator in enumerate(operators, start=2)]
    root = {"id": "00000000-0000-4000-8000-000000000001", "labels": ["Analytical_Pattern"],
            "properties": {"name": "An AP", "description": "Does something."}}
    edges = [{"from": root["id"], "to": node["id"], "labels": ["consist_of"]} for node in nodes]
    return json.dumps({"nodes": [root, *nodes], "edges": edges})


def _operator(label: str, **properties) -> dict:
    return {"labels": [label, "Operator"],
            "properties": {"description": "Does it.", "version": "1.0.0", "step": 1, **properties}}


def test_dropping_an_ap_file_adds_its_operators(tmp_path):
    ports = {"inputs": [{"name": "nl", "type": "string", "required": True, "default": ""}],
             "outputs": [{"name": "query", "type": "string", "required": True, "default": ""}]}
    (tmp_path / "a.json").write_text(_ap(
        _operator("Text_To_SQL_Operator", name="Text to SQL", **ports),
        _operator("Magic_Operator", name="a placeholder", **ports),
        _operator("SQL_Operator", name="no typed ports"),
    ))
    (tmp_path / "b.json").write_text(_ap(
        _operator("Text_To_SQL_Operator", name="a later definition, ignored", **ports),
        _operator("Explain_Operator", name="Explain", **ports),
    ))
    operators = _list_operator_in_aps(tmp_path)
    assert [(op.label, op.name) for op in operators] == [
        ("Text_To_SQL_Operator", "Text to SQL"), ("Explain_Operator", "Explain")]
    assert all(op.real for op in operators)


def test_a_file_that_is_not_an_ap_is_named_in_the_error(tmp_path):
    (tmp_path / "broken.json").write_text("{not json")
    with pytest.raises(SystemExit, match="broken.json"):
        _list_operator_in_aps(tmp_path)


def test_the_real_aps_of_the_dataset_can_be_read():
    assert _list_operator_in_aps(REAL_APS_DIR)


def _port(name: str, kind: str, type_: str = "string", **extra) -> Port:
    return Port(name=name, type=type_, kind=kind, **extra)


def test_operators_are_wired_by_kind_and_type_not_by_name():
    to_sql = Operator(label="Text_To_SQL_Operator", name="Text to SQL", description="Writes SQL.",
                      inputs=[_port("nl", "question")], outputs=[_port("query", "sql_query")])
    run = Operator(label="SQL_Query_Operator", name="SQL query", description="Runs SQL.",
                   inputs=[_port("sql", "sql_query"), _port("is_safe", "flag", "boolean", default=True)],
                   outputs=[_port("result", "table_rows", "array")])
    geocode = Operator(label="Geocode_Operator", name="Geocode", description="Locates a place.",
                       inputs=[_port("city", "place_name")], outputs=[_port("position", "coordinates", "object")])
    assert [(o.name, i.name) for o, i in to_sql.feeds(run)] == [("query", "sql")]
    # A string output does not feed a string input of another kind.
    assert to_sql.feeds(geocode) == []

    ap = AP.build([to_sql, run])
    assert ap.graph.wiring == [(to_sql.id, "query", run.id, "sql")]
    assert "takes sql (string) from step 1 and is_safe (boolean) as input" in ap.description
    # The kinds stay in the generator; `required` and `default` reach the graph.
    ports = next(op for op in ap.graph.operators if run.label in op.labels).properties["inputs"]
    assert ports == [{"name": "sql", "type": "string", "required": True, "default": ""},
                     {"name": "is_safe", "type": "boolean", "required": True, "default": True}]


def test_chains_are_one_per_operator_plus_count_composed_ones():
    port = Port(name="x", type="string", kind="x")
    operators = [Operator(label=label, name=label, description="", inputs=[port], outputs=[port])
                 for label in "ABC"]
    chains = generate_operator_chains(operators, 5, [], random.Random(0))
    assert chains[:3] == [("A",), ("B",), ("C",)]
    assert len(chains) == 3 + 5 and len(set(chains)) == len(chains)

    # A real question may name a chain the catalogue already has: nothing must change,
    # or the whole dataset would have to be written again. A chain it lacks is added.
    assert generate_operator_chains(operators, 5, [chains[-1]], random.Random(0)) == chains
    missing = next(chain for chain in itertools.permutations("ABC", 2) if chain not in chains)
    assert generate_operator_chains(operators, 5, [missing], random.Random(0)) == chains[:3] + [missing] + chains[3:]
