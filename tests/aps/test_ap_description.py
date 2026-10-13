"""
Unit tests for the description forged from an AP's operators.

No Neo4j container needed.
"""

from pathlib import Path

from moma_management.domain.analytical_pattern import AnalyticalPattern

ASSETS = Path(__file__).parent.parent.parent / "assets" / "aps"

TEXT_TO_SQL = (
    "translates a natural-language string into a safe SQL query, "
    "takes nl (string) as input and outputs query (string)"
)


def _id(n: int) -> str:
    return f"d0000000-0000-4000-8000-{n:012d}"


def _operator(n: int, name: str, description: str, inputs: list[str], outputs: list[str], **extra) -> dict:
    return {
        "id": _id(n),
        "labels": [f"{name.replace(' ', '_')}_Operator", "Operator"],
        "properties": {
            "name": name,
            "description": description,
            "inputs": [{"name": i, "type": "string"} for i in inputs],
            "outputs": [{"name": o, "type": "string"} for o in outputs],
            **extra,
        },
    }


def _text_to_sql(n: int, **extra) -> dict:
    return _operator(n, "Text to SQL", "Translates a natural-language string into a safe SQL query.",
                     ["nl"], ["query"], **extra)


def _sql_provenance(n: int, **extra) -> dict:
    return _operator(n, "SQL Provenance", "Analyzes an SQL query and returns provenance information.",
                     ["sql"], ["provenance"], **extra)


def _wire(source: int, output: str, result: int, target: int, input_: str) -> tuple[dict, list[dict]]:
    """A ResultType node and the output/input edges AP Management's Composer puts
    between two operators."""
    node = {"id": _id(result), "labels": ["ResultType", "string"],
            "properties": {"name": output}}
    edges = [
        {"from": _id(source), "to": _id(result), "labels": ["output"],
         "properties": {"mapping": {f"to['{output}']": f"from['outputs']['{output}']"}}},
        {"from": _id(result), "to": _id(target), "labels": ["input"],
         "properties": {"mapping": {f"to['inputs']['{input_}']": f"from['{output}']"}}},
    ]
    return node, edges


def _ap(operators: list[dict], extra_nodes: list[dict], extra_edges: list[dict]) -> AnalyticalPattern:
    root = {"id": _id(1), "labels": ["Analytical_Pattern"],
            "properties": {"name": "Text to SQL AP",
                           "description": "Takes a natural-language query and translates it into SQL."}}
    consist_of = [{"from": _id(1), "to": op["id"], "labels": ["consist_of"]}
                  for op in operators]
    return AnalyticalPattern.model_validate({
        "nodes": [root, *operators, *extra_nodes],
        "edges": [*consist_of, *extra_edges],
    })


def _composed() -> AnalyticalPattern:
    """Text to SQL composed with SQL Provenance, as the Composer leaves it: the root
    of the first AP, a ResultType in between, and `step: 1` on both operators."""
    node, edges = _wire(2, "query", 4, 3, "sql")
    follows = {"from": _id(3), "to": _id(2), "labels": ["follows"], "properties": {}}
    return _ap([_text_to_sql(2, step=1), _sql_provenance(3, step=1)], [node], [*edges, follows])


def test_single_operator_is_one_sentence():
    assert _ap([_text_to_sql(2)], [], []).describe() == (
        "Translates a natural-language string into a safe SQL query, "
        "takes nl (string) as input and outputs query (string)."
    )


def test_composed_ap_describes_every_step_not_just_the_first_ap():
    """The composed AP's root still says "translates it into SQL"; the description
    must cover the provenance step too."""
    assert _composed().describe() == (
        "Takes nl (string) and produces provenance (string) in 2 steps. "
        f"Step 1: {TEXT_TO_SQL}. "
        "Step 2: analyzes an SQL query and returns provenance information, "
        "takes sql (string) from step 1 as input and outputs provenance (string)."
    )


def test_same_structure_gives_same_description():
    assert _composed().describe() == _composed().model_copy(deep=True).describe()
    assert _composed().describe() != _ap([_text_to_sql(2)], [], []).describe()


def test_dataflow_decides_order_whatever_step_and_node_order_say():
    """The consumer comes first in the node list and claims step 1; it must still be
    described last."""
    node, edges = _wire(3, "query", 4, 2, "sql")
    ap = _ap([_sql_provenance(2, step=1), _text_to_sql(3, step=1)], [node], edges)
    assert ap.describe() == _composed().describe()


def test_unwired_operators_fall_back_to_step_order():
    ap = _ap([_sql_provenance(2, step=2), _text_to_sql(3, step=1)], [], [])
    description = ap.describe()
    assert description.startswith(
        "Takes nl (string) and sql (string) and produces query (string) and "
        "provenance (string) in 2 steps. Step 1: translates"
    )
    assert "takes sql (string) as input" in description


def test_unconsumed_output_of_an_inner_step_is_not_a_final_output():
    """`confidence` is never wired on: the AP's result is the provenance alone."""
    node, edges = _wire(2, "query", 4, 3, "sql")
    first = _operator(2, "Text to SQL", "Translates a question into SQL.", ["nl"], ["query", "confidence"])
    description = _ap([first, _sql_provenance(3)], [node], edges).describe()
    assert description.startswith("Takes nl (string) and produces provenance (string) in 2 steps.")
    assert "outputs query (string) and confidence (string)" in description


def test_two_producers_feeding_one_consumer():
    summary, summary_edges = _wire(2, "summary", 5, 4, "summary")
    tone, tone_edges = _wire(3, "tone", 6, 4, "tone")
    ap = _ap(
        [_operator(2, "Summarize", "Summarizes a topic.", ["topic"], ["summary"]),
         _operator(3, "Tone", "Picks a tone for an audience.", ["audience"], ["tone"]),
         _operator(4, "Combine", "Combines a summary and a tone into a message.",
                   ["summary", "tone"], ["message"])],
        [summary, tone],
        [*summary_edges, *tone_edges,
         {"from": _id(4), "to": _id(2), "labels": ["follows"]},
         {"from": _id(4), "to": _id(3), "labels": ["follows"]}],
    )
    description = ap.describe()
    assert description.startswith(
        "Takes topic (string) and audience (string) and produces message (string) in 3 steps.")
    assert description.endswith(
        "Step 3: combines a summary and a tone into a message, takes summary (string) from "
        "step 1 and tone (string) from step 2 as input and outputs message (string)."
    )


def test_hand_written_ap_with_datasets():
    """ap_sql_typed.json writes its `follows` edge earlier -> later, the opposite of
    the Composer, and reads and writes dataset nodes."""
    ap = AnalyticalPattern.model_validate_json((ASSETS / "ap_sql_typed.json").read_text())
    assert ap.describe() == (
        "Takes nl (string) and flag (boolean) and produces result (array) in 2 steps. "
        "Step 1: translates a natural-language string into a safe SQL query, takes nl (string) "
        "and flag (boolean) as input, outputs query (string) and is_read_only (boolean) and "
        "reads JSON data. "
        "Step 2: executes an SQL query against the input dataset and returns the result rows, "
        "takes sql (string) from step 1 and is_safe (boolean) from step 1 as input, outputs "
        "result (array) and writes JSON data."
    )


def test_operator_without_ports_is_described_by_the_data_it_touches():
    """The pre-typed AP syntax: no `inputs`/`outputs`, only edges to dataset nodes."""
    ap = AnalyticalPattern.model_validate_json((ASSETS / "ap_sql_select.json").read_text())
    assert ap.describe() == (
        "Query executed via DuckDB on two datasets, reads relational database data "
        "and writes relational database data."
    )


def test_operator_without_description_falls_back_to_its_name():
    operator = {"id": _id(2), "labels": ["Text_To_SQL_Operator", "Operator"],
                "properties": {"name": "Text to SQL"}}
    assert _ap([operator], [], []).describe() == "Text to SQL."


def test_ports_say_which_inputs_and_outputs_are_wired():
    """`confidence` is declared but no operator reads it."""
    node, edges = _wire(2, "query", 4, 3, "sql")
    first = _operator(2, "Text to SQL", "Translates a question into SQL.", ["nl"], ["query", "confidence"])
    ports = _ap([first, _sql_provenance(3)], [node], edges).ports()
    assert [(p.operator_id, p.name, p.type, p.is_input, p.wired) for p in ports] == [
        (_id(2), "nl", "string", True, False),
        (_id(2), "query", "string", False, True),
        (_id(2), "confidence", "string", False, False),
        (_id(3), "sql", "string", True, True),
        (_id(3), "provenance", "string", False, False),
    ]


def test_ports_follow_the_mappings_when_the_shared_node_renames_the_value():
    """ap_sql_typed.json: output `is_read_only` goes through a node keyed `is_safe`
    into input `is_safe`; the inputs fed by a dataset node are not wired."""
    ap = AnalyticalPattern.model_validate_json((ASSETS / "ap_sql_typed.json").read_text())
    assert [(p.name, p.is_input, p.wired) for p in ap.ports()] == [
        ("nl", True, False), ("flag", True, False),
        ("query", False, True), ("is_read_only", False, True),
        ("sql", True, True), ("is_safe", True, True),
        ("result", False, False),
    ]


def test_a_mapping_going_inside_a_port_still_wires_that_port():
    """`from['outputs']['payload']['query']` reads a field of the output `payload`,
    `to['inputs']['config']['sql']` fills a field of the input `config`."""
    first = _operator(2, "Text to SQL", "Writes SQL.", ["nl"], [])
    first["properties"]["outputs"] = [
        {"name": "payload", "type": "object", "properties": {"query": {"type": "string"}}}]
    node = {"id": _id(4), "labels": ["ResultType", "string"], "properties": {"name": "query"}}
    edges = [
        {"from": _id(2), "to": _id(4), "labels": ["output"],
         "properties": {"mapping": {"to['query']": "from['outputs']['payload']['query']"}}},
        {"from": _id(4), "to": _id(3), "labels": ["input"],
         "properties": {"mapping": {"to['inputs']['config']['sql']": "from['query']"}}},
    ]
    ap = _ap([first, _operator(3, "Run", "Runs it.", ["config"], ["rows"])], [node], edges)
    assert [(p.name, p.wired) for p in ap.ports()] == [
        ("nl", False), ("payload", True), ("config", True), ("rows", False)]
    assert "takes config (string) from step 1 as input" in ap.describe()


def test_operators_feeding_each_other_in_a_circle_are_still_ordered():
    first, first_edges = _wire(2, "query", 4, 3, "sql")
    back, back_edges = _wire(3, "provenance", 5, 2, "nl")
    ap = _ap([_text_to_sql(2), _sql_provenance(3)], [first, back], [*first_edges, *back_edges])
    assert [str(op.id) for op in ap.operators] == [_id(2), _id(3)]
