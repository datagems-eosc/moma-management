"""
Unit tests for the local schema validator and the /validate endpoints.

No Neo4j container needed — these tests validate raw dicts against JSON
schemas and structural rules.
"""

from uuid import uuid4

import pytest

from moma_management.domain.schema_validator import (
    LocalSchemaValidator,
    SchemaError,
    _validate_structure,
)

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
# JSON-Schema validation (moma.schema.json)
# ===================================================================


class TestJsonSchemaValidation:
    """Tests for ``LocalSchemaValidator.validate()``."""

    def test_valid_graph_returns_no_errors(self):
        v = LocalSchemaValidator()
        errors = v.validate(_make_valid_ap())
        assert errors == []

    def test_missing_nodes_key(self):
        v = LocalSchemaValidator()
        errors = v.validate({"edges": []})
        assert len(errors) >= 1
        keywords = {e.keyword for e in errors}
        assert "required" in keywords

    def test_node_missing_required_field(self):
        """A node without 'labels' should trigger a validation error."""
        v = LocalSchemaValidator()
        data = {
            "nodes": [
                {"id": str(uuid4()), "properties": {"name": "x"}},
            ],
        }
        errors = v.validate(data)
        assert len(errors) >= 1
        assert any("labels" in e.message for e in errors)

    def test_node_bad_property_name(self):
        """Property names must match ^[a-z@][a-zA-Z0-9_]*$."""
        v = LocalSchemaValidator()
        data = {
            "nodes": [
                {
                    "id": str(uuid4()),
                    "labels": ["Analytical_Pattern"],
                    "properties": {"Invalid-Key!": "val"},
                },
            ],
        }
        errors = v.validate(data)
        assert len(errors) >= 1
        assert any("propertyNames" ==
                   e.keyword or "pattern" in e.keyword for e in errors)


# ===================================================================
# Edge-constraint validation
# ===================================================================


class TestEdgeConstraintValidation:
    """Tests for ``validate_edge_constraints``."""

    def test_valid_edges_no_errors(self):
        v = LocalSchemaValidator()
        errors = v.validate_edge_constraints(_make_valid_ap())
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
        v = LocalSchemaValidator()
        errors = v.validate_edge_constraints(data)
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
        v = LocalSchemaValidator()
        errors = v.validate_edge_constraints(data)
        assert len(errors) >= 1
        assert any("does not exist" in e.message for e in errors)

    def test_valid_dataset_edges(self):
        v = LocalSchemaValidator()
        errors = v.validate_edge_constraints(_make_valid_dataset())
        assert errors == []


# ===================================================================
# Graph-structure validation
# ===================================================================


class TestApStructureValidation:
    """Tests for AP structural rules (root, connectivity)."""

    def test_valid_ap_structure(self):
        errors = _validate_structure(
            _make_valid_ap(), "Analytical_Pattern", "ap")
        assert errors == []

    def test_no_root_node(self):
        data = {
            "nodes": [
                {"id": str(uuid4()), "labels": [
                    "Operator"], "properties": {"name": "op"}},
            ],
            "edges": [],
        }
        errors = _validate_structure(data, "Analytical_Pattern", "ap")
        assert len(errors) == 1
        assert errors[0].keyword == "apStructure"
        assert "No node with label" in errors[0].message

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
        errors = _validate_structure(data, "Analytical_Pattern", "ap")
        assert len(errors) == 1
        assert "Multiple nodes" in errors[0].message

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
        errors = _validate_structure(data, "Analytical_Pattern", "ap")
        assert any("must not have incoming edges" in e.message for e in errors)

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
        errors = _validate_structure(data, "Analytical_Pattern", "ap")
        assert any("not fully connected" in e.message for e in errors)


class TestDatasetStructureValidation:
    """Tests for Dataset structural rules."""

    def test_valid_dataset_structure(self):
        errors = _validate_structure(
            _make_valid_dataset(), "sc:Dataset", "dataset")
        assert errors == []

    def test_no_dataset_root(self):
        data = {
            "nodes": [
                {"id": str(uuid4()), "labels": [
                    "Data"], "properties": {"name": "file"}},
            ],
            "edges": [],
        }
        errors = _validate_structure(data, "sc:Dataset", "dataset")
        assert len(errors) == 1
        assert errors[0].keyword == "datasetStructure"


# ===================================================================
# Full orchestration: validate_graph
# ===================================================================


class TestValidateGraph:
    """Tests for the orchestrator ``validate_graph``."""

    def test_valid_ap_returns_empty(self):
        v = LocalSchemaValidator()
        errors = v.validate_graph(_make_valid_ap(), graph_type="ap")
        assert errors == []

    def test_valid_dataset_returns_empty(self):
        v = LocalSchemaValidator()
        errors = v.validate_graph(_make_valid_dataset(), graph_type="dataset")
        assert errors == []

    def test_invalid_ap_collects_all_error_types(self):
        """A payload with schema, edge, and structural errors should report all."""
        data = {
            # missing 'nodes' entirely -> schema error
            # also: no edges key
        }
        v = LocalSchemaValidator()
        errors = v.validate_graph(data, graph_type="ap")
        keywords = {e.keyword for e in errors}
        # At minimum: schema "required" error + apStructure error
        assert "required" in keywords
        assert "apStructure" in keywords

    def test_empty_nodes_produces_structural_error(self):
        v = LocalSchemaValidator()
        errors = v.validate_graph({"nodes": []}, graph_type="ap")
        assert any(e.keyword == "apStructure" for e in errors)

    def test_error_shape_matches_ajv(self):
        """Every error must have the five AJV fields."""
        v = LocalSchemaValidator()
        errors = v.validate_graph({}, graph_type="ap")
        for e in errors:
            assert isinstance(e.keyword, str)
            assert isinstance(e.instancePath, str)
            assert isinstance(e.schemaPath, str)
            assert isinstance(e.params, dict)
            assert isinstance(e.message, str)


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


class TestDatasetServiceValidate:
    """Tests for DatasetService.validate (uses MagicMock repo)."""

    def test_valid_dataset(self):
        from pathlib import Path
        from unittest.mock import MagicMock

        from moma_management.services.dataset import DatasetService

        mapping_file = Path(__file__).resolve(
        ).parent.parent.parent / "moma_management" / "domain" / "mapping.yml"
        svc = DatasetService(MagicMock(), mapping_file)
        errors = svc.validate(_make_valid_dataset())
        assert errors == []

    def test_invalid_dataset(self):
        from pathlib import Path
        from unittest.mock import MagicMock

        from moma_management.services.dataset import DatasetService

        mapping_file = Path(__file__).resolve(
        ).parent.parent.parent / "moma_management" / "domain" / "mapping.yml"
        svc = DatasetService(MagicMock(), mapping_file)
        errors = svc.validate({"nodes": []})
        assert len(errors) >= 1
