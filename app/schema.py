class PGSchema:
     # Schema + validator for PG-JSON graphs

    # All allowed labels in the MoMa
    ALLOWED_LABELS = {
        "Dataset",
        "Data",
        "DataPart",
        "FileSet",
        "FileObject",
        "DatabaseConnection",
        "RelationalDatabase",
        "TextSet",
        "ImageSet",
        "CSV",
        "Table",
        "Field",
        "Column",
        "PDF",
        "RecordSet",
        "Statistics",
        "User",
        "Task",
        "Analytical_Pattern",
        "Operator"
    }

    #  Valid label combinations (must ALL be present)
    VALID_NODE_TYPES = {
        "Dataset": {"Dataset"},
        "Data": {"Data", "FileSet"},
        "DataPart": {"DataPart", "FileObject"},
        "RelationalDatabase": {"RelationalDatabase", "DatabaseConnection", "Data"},
        "Table": {"Table", "DataPart", "FileObject"},
        "CSV": {"CSV", "DataPart", "FileObject"},
        "Column": {"Column", "DataPart", "Field"},
        "PDF": {"PDF", "DataPart", "Field"},
        "TextSet": {"TextSet", "Data", "FileSet"},
        "ImageSet": {"ImageSet", "Data", "FileSet"},
        "Statistics": {"Statistics"},
        "User": {"User"},
        "Task": {"Task"},
        "Analytical_Pattern": {"Analytical_Pattern"},
        "Operator": {"Operator"}
    }

    def __init__(self, strict: bool = True):
        # strict=True -> unknown labels or invalid combinations are errors
        # strict=False -> warnings only
        self.strict = strict


    #  Main validation
    def validate(self, pg_json: dict) -> dict:
        report = {
            "total_nodes": 0,
            "invalid_nodes": [],
            "unknown_labels": [],
            "nodes_without_labels": [],
            "is_valid": True
        }

        for node in pg_json.get("nodes", []):
            report["total_nodes"] += 1
            node_id = node.get("id")
            labels = set(node.get("labels", []))

            # No labels
            if not labels:
                report["nodes_without_labels"].append(node_id)
                report["is_valid"] = False
                continue

            #  Unknown labels
            unknown = labels - self.ALLOWED_LABELS
            if unknown:
                report["unknown_labels"].append({
                    "id": node_id,
                    "labels": list(unknown)
                })
                if self.strict:
                    report["is_valid"] = False

            #  Invalid label combination
            if not self._is_valid_combination(labels):
                report["invalid_nodes"].append({
                    "id": node_id,
                    "labels": list(labels)
                })
                if self.strict:
                    report["is_valid"] = False

        return report


    def _is_valid_combination(self, labels: set) -> bool:
        # Check if labels match at least one valid node type
        for valid_labels in self.VALID_NODE_TYPES.values():
            if valid_labels.issubset(labels):
                return True
        return False
