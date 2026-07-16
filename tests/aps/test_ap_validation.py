"""
Unit tests for the local schema validator and the /validate endpoints.

No Neo4j container needed — these tests validate raw dicts against JSON
schemas and structural rules.
"""

from uuid import uuid4

import pytest
from pydantic import ValidationError as PydanticValidationError

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.dataset import Dataset
from moma_management.domain.validation.schema_error import SchemaError
from moma_management.domain.validation.steps.mapping_step import _validate_mappings
from moma_management.domain.validation.steps.schema_step import SchemaStep

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_valid_ap() -> dict:
    """Return a minimal valid AnalyticalPattern PG-JSON dict."""
    root_id = str(uuid4())
    op_id = str(uuid4())
    return {
        "nodes": [
            {
                "id": root_id,
                "labels": ["Analytical_Pattern"],
                "properties": {"name": "test-ap"},
            },
            {
                "id": op_id,
                "labels": ["Operator"],
                "properties": {"name": "op-1"},
            },
        ],
        "edges": [
            {"from": root_id, "to": op_id, "labels": ["consist_of"]},
        ],
    }


def _make_valid_dataset() -> dict:
    """Return a minimal valid Dataset PG-JSON dict."""
    root_id = str(uuid4())
    data_id = str(uuid4())
    return {
        "nodes": [
            {
                "id": root_id,
                "labels": ["sc:Dataset"],
                "properties": {"name": "ds"},
            },
            {
                "id": data_id,
                "labels": ["Data"],
                "properties": {"name": "file.csv"},
            },
        ],
        "edges": [
            {"from": root_id, "to": data_id, "labels": ["distribution"]},
        ],
    }


# ===================================================================
# Model-based schema validation
# ===================================================================


class TestSchemaValidation:
    """Model parsing validates graph structure; Pydantic enforces field rules."""

    def test_valid_graph_parses_without_error(self):
        AnalyticalPattern.model_validate(_make_valid_ap())  # must not raise

    def test_missing_nodes_key_raises(self):
        with pytest.raises(PydanticValidationError):
            AnalyticalPattern.model_validate({"edges": []})

    def test_node_missing_required_field_raises(self):
        """A node without 'labels' should fail Pydantic validation."""
        data = {
            "nodes": [
                {"id": str(uuid4()), "properties": {"name": "x"}},
            ],
        }
        with pytest.raises(PydanticValidationError):
            AnalyticalPattern.model_validate(data)


# ===================================================================
# Edge-constraint validation
# ===================================================================


class TestEdgeConstraintValidation:
    """Tests for ``SchemaStep.validate_edge_constraints``."""

    def test_valid_edges_no_errors(self):
        errors = SchemaStep.validate_edge_constraints(_make_valid_ap())
        assert errors == []

    def test_invalid_edge_label(self):
        """An edge with an invalid label between two node types must error."""
        root_id = str(uuid4())
        op_id = str(uuid4())
        data = {
            "nodes": [
                {"id": root_id, "labels": [
                    "Analytical_Pattern"], "properties": {"name": "ap"}},
                {"id": op_id, "labels": ["Operator"],
                    "properties": {"name": "op"}},
            ],
            "edges": [
                {"from": root_id, "to": op_id, "labels": ["bad_edge"]},
            ],
        }
        errors = SchemaStep.validate_edge_constraints(data)
        assert len(errors) == 1
        assert errors[0].keyword == "edgeRelationship"
        assert "bad_edge" in errors[0].message

    def test_edge_referencing_missing_node(self):
        """An edge whose 'from' or 'to' references a nonexistent node must error."""
        root_id = str(uuid4())
        phantom_id = str(uuid4())
        data = {
            "nodes": [
                {"id": root_id, "labels": [
                    "Analytical_Pattern"], "properties": {"name": "ap"}},
            ],
            "edges": [
                {"from": root_id, "to": phantom_id, "labels": ["consist_of"]},
            ],
        }
        errors = SchemaStep.validate_edge_constraints(data)
        assert len(errors) >= 1
        assert any("does not exist" in e.message for e in errors)

    def test_valid_dataset_edges(self):
        errors = SchemaStep.validate_edge_constraints(_make_valid_dataset())
        assert errors == []


# ===================================================================
# Graph-structure validation
# ===================================================================


class TestApStructureValidation:
    """Tests for AP structural rules (root, connectivity)."""

    def test_valid_ap_structure(self):
        AnalyticalPattern.model_validate(_make_valid_ap())  # must not raise

    def test_no_root_node(self):
        data = {
            "nodes": [
                {"id": str(uuid4()), "labels": [
                    "Operator"], "properties": {"name": "op"}},
            ],
            "edges": [],
        }
        with pytest.raises(PydanticValidationError) as exc_info:
            AnalyticalPattern.model_validate(data)
        assert "No node with label" in str(exc_info.value)

    def test_multiple_root_nodes(self):
        data = {
            "nodes": [
                {"id": str(uuid4()), "labels": [
                    "Analytical_Pattern"], "properties": {"name": "ap1"}},
                {"id": str(uuid4()), "labels": [
                    "Analytical_Pattern"], "properties": {"name": "ap2"}},
            ],
            "edges": [],
        }
        with pytest.raises(PydanticValidationError) as exc_info:
            AnalyticalPattern.model_validate(data)
        assert "Multiple nodes" in str(exc_info.value)

    def test_incoming_edge_to_root(self):
        root_id = str(uuid4())
        op_id = str(uuid4())
        data = {
            "nodes": [
                {"id": root_id, "labels": [
                    "Analytical_Pattern"], "properties": {"name": "ap"}},
                {"id": op_id, "labels": ["Operator"],
                    "properties": {"name": "op"}},
            ],
            "edges": [
                {"from": root_id, "to": op_id, "labels": ["consist_of"]},
                {"from": op_id, "to": root_id, "labels": ["follows"]},
            ],
        }
        with pytest.raises(PydanticValidationError) as exc_info:
            AnalyticalPattern.model_validate(data)
        assert "must not have incoming edges" in str(exc_info.value)

    def test_disconnected_graph(self):
        root_id = str(uuid4())
        op_id = str(uuid4())
        isolated_id = str(uuid4())
        data = {
            "nodes": [
                {"id": root_id, "labels": [
                    "Analytical_Pattern"], "properties": {"name": "ap"}},
                {"id": op_id, "labels": ["Operator"],
                    "properties": {"name": "op"}},
                {"id": isolated_id, "labels": [
                    "Operator"], "properties": {"name": "isolated"}},
            ],
            "edges": [
                {"from": root_id, "to": op_id, "labels": ["consist_of"]},
            ],
        }
        with pytest.raises(PydanticValidationError) as exc_info:
            AnalyticalPattern.model_validate(data)
        assert "not fully connected" in str(exc_info.value)


class TestDatasetStructureValidation:
    """Tests for Dataset structural rules."""

    def test_valid_dataset_structure(self):
        Dataset.model_validate(_make_valid_dataset())  # must not raise

    def test_no_dataset_root(self):
        data = {
            "nodes": [
                {"id": str(uuid4()), "labels": [
                    "Data"], "properties": {"name": "file"}},
            ],
            "edges": [],
        }
        with pytest.raises(PydanticValidationError) as exc_info:
            Dataset.model_validate(data)
        assert "No node with label" in str(exc_info.value)


# ===================================================================
# Full orchestration via model parsing
# ===================================================================


class TestValidateGraph:
    """Tests for end-to-end validation via model parsing."""

    def test_valid_ap_parses_without_error(self):
        AnalyticalPattern.model_validate(_make_valid_ap())  # must not raise

    def test_valid_dataset_parses_without_error(self):
        Dataset.model_validate(_make_valid_dataset())  # must not raise

    def test_empty_nodes_produces_structural_error(self):
        with pytest.raises(PydanticValidationError) as exc_info:
            AnalyticalPattern.model_validate({"nodes": []})
        assert "AnalyticalPatternStructure" in str(exc_info.value)

    def test_missing_nodes_key_raises(self):
        with pytest.raises(PydanticValidationError):
            AnalyticalPattern.model_validate({})

    def test_error_message_contains_schema_error_fields(self):
        """Validation errors include SchemaError keyword and message."""
        with pytest.raises(PydanticValidationError) as exc_info:
            AnalyticalPattern.model_validate({"nodes": []})
        err_str = str(exc_info.value)
        assert "AnalyticalPatternStructure" in err_str
        assert "No node with label" in err_str


# ===================================================================
# Service-level validate (no DB needed)
# ===================================================================


class TestApServiceValidate:
    """Tests for AnalyticalPatternService.validate (uses MagicMock repo)."""

    def test_valid_ap(self):
        from unittest.mock import MagicMock

        from moma_management.services.analytical_pattern import AnalyticalPatternService

        svc = AnalyticalPatternService(MagicMock(), MagicMock())
        errors = svc.validate(_make_valid_ap())
        assert errors == []

    def test_invalid_ap(self):
        from unittest.mock import MagicMock

        from moma_management.services.analytical_pattern import AnalyticalPatternService

        svc = AnalyticalPatternService(MagicMock(), MagicMock())
        errors = svc.validate({"nodes": []})
        assert len(errors) >= 1


# ===================================================================
# Mapping validation
# ===================================================================

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_AP_ROOT_ID = "c1000000-0000-4000-a000-000000000001"
_OP_ID = "c1000000-0000-4000-a000-000000000002"
_RT_ID = "c1000000-0000-4000-a000-000000000003"
_DATA_ID = "c1000000-0000-4000-a000-000000000004"


def _make_op(inputs=None, outputs=None) -> dict:
    return {
        "id": _OP_ID,
        "labels": ["Operator"],
        "properties": {
            "name": "op",
            "inputs": inputs or [],
            "outputs": outputs or [],
        },
    }


def _make_rt(name: str, subtype: str) -> dict:
    return {
        "id": _RT_ID,
        "labels": ["ResultType", subtype],
        "properties": {"name": name},
    }


def _make_data(extra_props: dict | None = None) -> dict:
    props = {"name": "my_db", "description": "a db"}
    if extra_props:
        props.update(extra_props)
    return {
        "id": _DATA_ID,
        "labels": ["RelationalDatabase", "Data"],
        "properties": props,
    }


def _make_ap_with_edges(*edges) -> dict:
    """Minimal AP dict with op + rt/data nodes and the given edges."""
    nodes = [
        {
            "id": _AP_ROOT_ID,
            "labels": ["Analytical_Pattern"],
            "properties": {"name": "test-ap"},
        },
        _make_op(
            inputs=[{"name": "sql", "type": "string", "required": True}],
            outputs=[{"name": "result", "type": "string", "required": True}],
        ),
    ]
    # Collect extra node IDs needed from edges
    return {
        "nodes": nodes,
        "edges": [
            {"from": _AP_ROOT_ID, "to": _OP_ID, "labels": ["consist_of"]},
            *edges,
        ],
    }


def _ap_with_rt(
    subtype: str,
    rt_name: str,
    edge_label: str,
    mapping: dict,
    param_type: str = "string",
) -> dict:
    """AP with one Operator, one ResultType, and one mapping edge.

    For ``input`` edges the direction is RT → Operator.
    For ``output`` edges the direction is Operator → RT.
    """
    op = _make_op(
        inputs=[{"name": "sql", "type": param_type, "required": True}],
        outputs=[{"name": "result", "type": param_type, "required": True}],
    )
    rt = _make_rt(rt_name, subtype)
    if edge_label == "input":
        edge_from, edge_to = _RT_ID, _OP_ID
    else:
        edge_from, edge_to = _OP_ID, _RT_ID
    return {
        "nodes": [
            {"id": _AP_ROOT_ID, "labels": [
                "Analytical_Pattern"], "properties": {"name": "ap"}},
            op,
            rt,
        ],
        "edges": [
            {"from": _AP_ROOT_ID, "to": _OP_ID, "labels": ["consist_of"]},
            {
                "from": edge_from,
                "to": edge_to,
                "labels": [edge_label],
                "properties": {"mapping": mapping},
            },
        ],
    }


def _ap_with_data(edge_label: str, mapping: dict) -> dict:
    """AP with one Operator, one Data node, and one mapping edge.

    For ``input`` edges the direction is Data → Operator.
    For ``output`` edges the direction is Operator → Data.
    """
    op = _make_op(
        inputs=[{"name": "db_name", "type": "RelationalDatabase", "required": True}],
        outputs=[
            {"name": "db_name", "type": "RelationalDatabase", "required": True}],
    )
    data = _make_data()
    if edge_label == "input":
        edge_from, edge_to = _DATA_ID, _OP_ID
    else:
        edge_from, edge_to = _OP_ID, _DATA_ID
    return {
        "nodes": [
            {"id": _AP_ROOT_ID, "labels": [
                "Analytical_Pattern"], "properties": {"name": "ap"}},
            op,
            data,
        ],
        "edges": [
            {"from": _AP_ROOT_ID, "to": _OP_ID, "labels": ["consist_of"]},
            {
                "from": edge_from,
                "to": edge_to,
                "labels": [edge_label],
                "properties": {"mapping": mapping},
            },
        ],
    }


class TestMappingValidation:

    # ── Happy-path ────────────────────────────────────────────────────────

    def test_no_mappings_passes(self):
        """Edges without mapping dicts are skipped without error."""
        data = _make_ap_with_edges(
            {"from": _RT_ID, "to": _OP_ID, "labels": ["input"]}
        )
        # RT node not in graph here — but _validate_mappings only runs when
        # mapping is present, so no errors expected.
        assert _validate_mappings(data) == []

    def test_valid_input_edge_result_type(self):
        """input edge: correct param + matching RT name → no errors."""
        data = _ap_with_rt(
            subtype="string",
            rt_name="sql_query",
            edge_label="input",
            mapping={"to['inputs']['sql']": "from['sql_query']"},
        )
        assert _validate_mappings(data) == []

    def test_valid_output_edge_result_type(self):
        """output edge: correct output param + matching RT name → no errors."""
        data = _ap_with_rt(
            subtype="string",
            rt_name="result",
            edge_label="output",
            mapping={"to['result']": "from['outputs']['result']"},
        )
        assert _validate_mappings(data) == []

    def test_valid_input_edge_data_node(self):
        """input edge from Data node: property exists on node → no errors."""
        data = _ap_with_data(
            edge_label="input",
            mapping={"to['inputs']['db_name']": "from['name']"},
        )
        assert _validate_mappings(data) == []

    def test_valid_output_edge_data_node(self):
        """output edge to Data node: property exists → no errors."""
        data = _ap_with_data(
            edge_label="output",
            mapping={"to['name']": "from['outputs']['db_name']"},
        )
        assert _validate_mappings(data) == []

    # ── Step 8: Parameter name cross-reference failures ───────────────────

    def test_input_unknown_operator_param(self):
        """input edge key references a param not in Operator inputs → error."""
        data = _ap_with_rt(
            subtype="string",
            rt_name="sql_query",
            edge_label="input",
            mapping={"to['inputs']['nonexistent']": "from['sql_query']"},
        )
        errors = _validate_mappings(data)
        assert any(e.keyword == "mappingParameter" for e in errors)
        assert any("nonexistent" in e.message for e in errors)

    def test_output_unknown_operator_param(self):
        """output edge value references a param not in Operator outputs → error."""
        data = _ap_with_rt(
            subtype="string",
            rt_name="result",
            edge_label="output",
            mapping={"to['result']": "from['outputs']['ghost']"},
        )
        errors = _validate_mappings(data)
        assert any(e.keyword == "mappingParameter" for e in errors)
        assert any("ghost" in e.message for e in errors)

    # ── Step 7: Property existence failures ───────────────────────────────

    def test_input_rt_name_mismatch(self):
        """input edge value references a key that doesn't match RT name → error."""
        data = _ap_with_rt(
            subtype="string",
            rt_name="sql_query",
            edge_label="input",
            mapping={"to['inputs']['sql']": "from['wrong_name']"},
        )
        errors = _validate_mappings(data)
        assert any(e.keyword == "mappingProperty" for e in errors)
        assert any("wrong_name" in e.message for e in errors)

    def test_output_rt_name_mismatch(self):
        """output edge key references a key that doesn't match RT name → error."""
        data = _ap_with_rt(
            subtype="string",
            rt_name="result",
            edge_label="output",
            mapping={"to['bad_key']": "from['outputs']['result']"},
        )
        errors = _validate_mappings(data)
        assert any(e.keyword == "mappingProperty" for e in errors)
        assert any("bad_key" in e.message for e in errors)

    def test_input_data_property_missing(self):
        """input edge from Data node references non-existent property → error."""
        data = _ap_with_data(
            edge_label="input",
            mapping={"to['inputs']['db_name']": "from['no_such_prop']"},
        )
        errors = _validate_mappings(data)
        assert any(e.keyword == "mappingProperty" for e in errors)
        assert any("no_such_prop" in e.message for e in errors)

    def test_output_data_property_missing(self):
        """output edge to Data node references non-existent property → error."""
        data = _ap_with_data(
            edge_label="output",
            mapping={"to['no_such_prop']": "from['outputs']['db_name']"},
        )
        errors = _validate_mappings(data)
        assert any(e.keyword == "mappingProperty" for e in errors)

    # ── Step 9: Type-compatibility failures ───────────────────────────────

    def test_input_type_mismatch(self):
        """input edge: operator param type 'boolean' vs RT 'string' → error."""
        data = _ap_with_rt(
            subtype="string",
            rt_name="sql_query",
            edge_label="input",
            mapping={"to['inputs']['sql']": "from['sql_query']"},
            param_type="boolean",
        )
        errors = _validate_mappings(data)
        assert any(e.keyword == "mappingTypeCompatibility" for e in errors)
        assert any(
            "boolean" in e.message and "string" in e.message for e in errors)

    def test_output_type_mismatch(self):
        """output edge: operator param type 'array' vs RT 'string' → error."""
        data = _ap_with_rt(
            subtype="string",
            rt_name="result",
            edge_label="output",
            mapping={"to['result']": "from['outputs']['result']"},
            param_type="array",
        )
        errors = _validate_mappings(data)
        assert any(e.keyword == "mappingTypeCompatibility" for e in errors)

    def test_data_node_no_type_check(self):
        """Data nodes are exempt from type-compatibility checks → no error."""
        data = _ap_with_data(
            edge_label="input",
            mapping={"to['inputs']['db_name']": "from['name']"},
        )
        errors = _validate_mappings(data)
        assert not any(e.keyword == "mappingTypeCompatibility" for e in errors)

    def test_output_nested_property_type_match(self):
        """Nested property path from['outputs']['payload']['query'] resolves to string → no error."""
        op = {
            "id": _OP_ID,
            "labels": ["Operator"],
            "properties": {
                "name": "op",
                "inputs": [],
                "outputs": [
                    {
                        "name": "payload",
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "confidence": {"type": "number"},
                        },
                        "required": True,
                    }
                ],
            },
        }
        rt = _make_rt("query", "string")
        data = {
            "nodes": [
                {"id": _AP_ROOT_ID, "labels": ["Analytical_Pattern"], "properties": {"name": "ap"}},
                op,
                rt,
            ],
            "edges": [
                {"from": _AP_ROOT_ID, "to": _OP_ID, "labels": ["consist_of"]},
                {
                    "from": _OP_ID,
                    "to": _RT_ID,
                    "labels": ["output"],
                    "properties": {"mapping": {"to['query']": "from['outputs']['payload']['query']"}},
                },
            ],
        }
        errors = _validate_mappings(data)
        assert not any(e.keyword == "mappingTypeCompatibility" for e in errors)

    def test_output_nested_property_type_mismatch(self):
        """Nested property path from['outputs']['payload']['confidence'] (number) vs string RT → error."""
        op = {
            "id": _OP_ID,
            "labels": ["Operator"],
            "properties": {
                "name": "op",
                "inputs": [],
                "outputs": [
                    {
                        "name": "payload",
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "confidence": {"type": "number"},
                        },
                        "required": True,
                    }
                ],
            },
        }
        rt = _make_rt("query", "string")
        data = {
            "nodes": [
                {"id": _AP_ROOT_ID, "labels": ["Analytical_Pattern"], "properties": {"name": "ap"}},
                op,
                rt,
            ],
            "edges": [
                {"from": _AP_ROOT_ID, "to": _OP_ID, "labels": ["consist_of"]},
                {
                    "from": _OP_ID,
                    "to": _RT_ID,
                    "labels": ["output"],
                    "properties": {"mapping": {"to['query']": "from['outputs']['payload']['confidence']"}},
                },
            ],
        }
        errors = _validate_mappings(data)
        assert any(e.keyword == "mappingTypeCompatibility" for e in errors)
        assert any("number" in e.message and "string" in e.message for e in errors)

    # ── Regression ────────────────────────────────────────────────────────

    def test_ap_sql_typed_fixture(self):
        """The ap_sql_typed.json fixture must pass end-to-end."""
        import json
        from pathlib import Path

        fixture = Path(__file__).parents[2] / \
            "assets" / "aps" / "ap_sql_typed.json"
        data = json.loads(fixture.read_text())
        errors = _validate_mappings(data)
        assert errors == [], [e.message for e in errors]

    def test_ap_dataset_recommendations_fixture(self):
        """The updated ap_dataset_recommendations.json fixture must pass end-to-end."""
        import json
        from pathlib import Path

        fixture = Path(__file__).parents[2] / \
            "assets" / "aps" / "ap_dataset_recommendations.json"
        data = json.loads(fixture.read_text())
        errors = _validate_mappings(data)
        assert errors == [], [e.message for e in errors]


# ===================================================================
# Dataset node as Operator input / output
# ===================================================================

_DS_ID = "d1000000-0000-4000-a000-000000000005"


def _make_dataset_node(ds_id: str = _DS_ID) -> dict:
    return {
        "id": ds_id,
        "labels": ["sc:Dataset"],
        "properties": {"name": "My Dataset"},
    }


def _ap_with_dataset(edge_label: str, mapping: dict | None = None) -> dict:
    """Minimal AP with one Operator and one sc:Dataset node.

    For ``input`` edges the direction is sc:Dataset → Operator.
    For ``output`` edges the direction is Operator → sc:Dataset.
    """
    op = _make_op(
        inputs=[{"name": "seed", "type": "sc:Dataset", "required": True}],
        outputs=[{"name": "result", "type": "sc:Dataset", "required": True}],
    )
    ds = _make_dataset_node()
    if edge_label == "input":
        edge_from, edge_to = _DS_ID, _OP_ID
    else:
        edge_from, edge_to = _OP_ID, _DS_ID
    edge: dict = {"from": edge_from, "to": edge_to, "labels": [edge_label]}
    if mapping is not None:
        edge["properties"] = {"mapping": mapping}
    return {
        "nodes": [
            {"id": _AP_ROOT_ID, "labels": [
                "Analytical_Pattern"], "properties": {"name": "ap"}},
            op,
            ds,
        ],
        "edges": [
            {"from": _AP_ROOT_ID, "to": _OP_ID, "labels": ["consist_of"]},
            edge,
        ],
    }


class TestDatasetNodeIO:
    """Tests for sc:Dataset nodes as Operator inputs / outputs."""

    # ── Edge-constraint acceptance ────────────────────────────────────────

    def test_operator_input_from_dataset_node_passes_edge_constraint(self):
        """sc:Dataset -[input]-> Operator must be an allowed edge."""
        data = _ap_with_dataset(edge_label="input")
        errors = SchemaStep.validate_edge_constraints(data)
        assert errors == [], [e.message for e in errors]

    def test_operator_output_to_dataset_node_passes_edge_constraint(self):
        """Operator -[output]-> sc:Dataset must be an allowed edge."""
        data = _ap_with_dataset(edge_label="output")
        errors = SchemaStep.validate_edge_constraints(data)
        assert errors == [], [e.message for e in errors]

    # ── Full AP parse (all validation steps) ─────────────────────────────

    def test_dataset_input_edge_without_mapping_passes(self):
        """AP with sc:Dataset input edge and no mapping must validate."""
        AnalyticalPattern.model_validate(_ap_with_dataset(edge_label="input"))

    def test_dataset_output_edge_without_mapping_passes(self):
        """AP with sc:Dataset output edge and no mapping must validate."""
        AnalyticalPattern.model_validate(_ap_with_dataset(edge_label="output"))

    # ── Mapping is "Any" — silently ignored for sc:Dataset ───────────────

    def test_dataset_input_edge_with_mapping_is_any_no_errors(self):
        """A mapping on a sc:Dataset input edge is accepted without validation (Any semantics)."""
        data = _ap_with_dataset(
            edge_label="input",
            mapping={"to['inputs']['seed']": "from['whatever_prop']"},
        )
        errors = _validate_mappings(data)
        assert errors == [], [e.message for e in errors]

    def test_dataset_output_edge_with_mapping_is_any_no_errors(self):
        """A mapping on a sc:Dataset output edge is accepted without validation (Any semantics)."""
        data = _ap_with_dataset(
            edge_label="output",
            mapping={"to['anything']": "from['outputs']['result']"},
        )
        errors = _validate_mappings(data)
        assert errors == [], [e.message for e in errors]

    def test_dataset_node_no_type_compatibility_check(self):
        """sc:Dataset edges are exempt from type-compatibility checks."""
        data = _ap_with_dataset(
            edge_label="input",
            mapping={"to['inputs']['seed']": "from['name']"},
        )
        errors = _validate_mappings(data)
        assert not any(e.keyword == "mappingTypeCompatibility" for e in errors)


class TestDatasetServiceValidate:
    """Tests for DatasetService.validate (uses MagicMock repo)."""

    def test_valid_dataset(self):
        from pathlib import Path
        from unittest.mock import MagicMock

        from moma_management.services.dataset import DatasetService

        mapping_file = Path(__file__).resolve(
        ).parent.parent.parent / "moma_management" / "domain" / "mapping.yml"
        svc = DatasetService(MagicMock(), mapping_file, MagicMock())
        errors = svc.validate(_make_valid_dataset())
        assert errors == []

    def test_invalid_dataset(self):
        from pathlib import Path
        from unittest.mock import MagicMock

        from moma_management.services.dataset import DatasetService

        mapping_file = Path(__file__).resolve(
        ).parent.parent.parent / "moma_management" / "domain" / "mapping.yml"
        svc = DatasetService(MagicMock(), mapping_file, MagicMock())
        errors = svc.validate({"nodes": []})
        assert len(errors) >= 1
