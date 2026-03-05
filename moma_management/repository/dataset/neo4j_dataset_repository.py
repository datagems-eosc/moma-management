from logging import getLogger
from typing import List, Optional

from neo4j import Session, Transaction

from moma_management.domain.dataset import Dataset
from moma_management.domain.filters import DatasetFilter
from moma_management.repository.neo4j_pgson_mixin import Neo4jPgJsonMixin

logger = getLogger(__name__)


class Neo4jDatasetRepository(Neo4jPgJsonMixin):
    """Neo4j-backed implementation of DatasetRepository."""

    def __init__(self, session: Session):
        self._session = session

    def create(self, dataset: Dataset) -> str:
        """Store a full PG-JSON graph (nodes + edges)."""
        try:
            self._session.execute_write(self.create_pgson, dataset)
            return "success"
        except Exception as e:
            logger.error("Neo4j upload failed: %s", e)
            return f"Error: {str(e)}"

    def create_nodes(self, dataset: Dataset) -> str:
        """Store only the nodes of a PG-JSON graph."""
        try:
            def _tx(tx: Transaction) -> None:
                for node in dataset.nodes:
                    self.create_pgson_node(tx, node)
            self._session.execute_write(_tx)
            return "success"
        except Exception as e:
            logger.error("Neo4j node upload failed: %s", e)
            return f"Error: {str(e)}"

    def create_edges(self, dataset: Dataset) -> str:
        """Store only the edges of a PG-JSON graph."""
        if not dataset.edges:
            return "Error: Dataset must contain edges"
        try:
            def _tx(tx: Transaction) -> None:
                for edge in dataset.edges:  # type: ignore[union-attr]
                    self.create_pgson_edge(tx, edge)
            self._session.execute_write(_tx)
            return "success"
        except Exception as e:
            logger.error("Neo4j edge upload failed: %s", e)
            return f"Error: {str(e)}"

    def delete(self, id: str) -> int:
        """
        Delete a dataset and its full connected subgraph by id.
        """
        query = """//cypher
            MATCH (d {id: $datasetId})
            OPTIONAL MATCH (d)-[*1..10]-(m)
            WITH d, collect(DISTINCT m) AS related
            FOREACH (n IN related | DETACH DELETE n)
            DETACH DELETE d
            RETURN 1 AS deletedRows
        """
        result = self._session.run(query, datasetId=id)
        record = result.single()
        return record["deletedRows"] if record else 0

    def get(self, id: str) -> Optional[Dataset]:
        """
        Retrieve the dataset with the given ID
        """
        query = """//cypher
            MATCH (root {id: $datasetId})
            OPTIONAL MATCH (root)-[r*1..4]-(m)
            RETURN root, m, r
        """
        rows = list(self._session.run(query, datasetId=id))

        if not rows:
            return None

        nodes: dict = {}
        edges: dict = {}

        # Always include the root node
        root = rows[0]["root"]
        nodes[root["id"]] = self._deserialize_node(root)

        for record in rows:
            m = record["m"]
            rels = record["r"] or []

            if m:
                mid = m["id"]
                if mid not in nodes:
                    nodes[mid] = self._deserialize_node(m)

            for rel in rels:
                key = (rel.start_node["id"], rel.end_node["id"], rel.type)
                if key not in edges:
                    edges[key] = self._deserialize_edge(rel)

        # Only keep edges whose both endpoints are in the collected node set
        valid_edges = [
            e for e in edges.values()
            if e["from"] in nodes and e["to"] in nodes
        ]

        return Dataset(nodes=list(nodes.values()), edges=valid_edges)

    def list(self, criteria: DatasetFilter) -> List[Dataset]:
        try:
            skip = (criteria.page - 1) * criteria.pageSize
            limit = criteria.pageSize

            order = criteria.direction.value.upper()
            order_clause = (
                ", ".join([f"n.`{k.value}` {order}" for k in criteria.orderBy])
                if criteria.orderBy else "n.id ASC"
            )

            types = criteria.resolved_types

            allowed_labels = [
                "cr:FileObject",
                "cr:FileSet",
                "cr:Field",
                "Statistics",
                "cr:RecordSet"
            ]

            params = dict(
                nodeIds=criteria.nodeIds or [],
                publishedDateFrom=criteria.publishedFrom,
                publishedDateTo=criteria.publishedTo,
                status=criteria.status.value if criteria.status is not None else None,
                types=types,
                allowedLabels=allowed_labels,
            )

            # ---------------- COUNT QUERY ----------------

            count_query = """//cypher
            MATCH (n:`sc:Dataset`)
            WHERE ($nodeIds = [] OR n.id IN $nodeIds)
            AND ($publishedDateFrom IS NULL OR date(n.datePublished) >= $publishedDateFrom)
            AND ($publishedDateTo IS NULL OR date(n.datePublished) <= $publishedDateTo)
            AND ($status IS NULL OR n.status = $status)

            WITH n
            WHERE $types = []
               OR EXISTS {
                   MATCH (n)-[*1..4]-(m)
                   WHERE ANY(l IN labels(m) WHERE l IN $allowedLabels)
                   AND ANY(t IN $types WHERE t IN labels(m))
               }

            RETURN count(DISTINCT n) AS total
            """

            total_record = self._session.run(count_query, **params).single()
            total = total_record["total"] if total_record else 0

            if total == 0:
                return {
                    "datasets": [],
                    "page": criteria.page,
                    "pageSize": criteria.pageSize,
                    "total": total,
                }

            # ---------------- DATA QUERY ----------------
            # SKIP/LIMIT is applied to dataset nodes (n) BEFORE the subgraph
            # traversal so pagination is dataset-accurate.

            query = f"""//cypher
            MATCH (n:`sc:Dataset`)
            WHERE ($nodeIds = [] OR n.id IN $nodeIds)
            AND ($publishedDateFrom IS NULL OR date(n.datePublished) >= $publishedDateFrom)
            AND ($publishedDateTo IS NULL OR date(n.datePublished) <= $publishedDateTo)
            AND ($status IS NULL OR n.status = $status)
            AND ($types = []
               OR EXISTS {{
                   MATCH (n)-[*1..4]-(m)
                   WHERE ANY(l IN labels(m) WHERE l IN $allowedLabels)
                   AND ANY(t IN $types WHERE t IN labels(m))
               }})

            WITH n
            ORDER BY {order_clause}
            SKIP $skip
            LIMIT $limit

            OPTIONAL MATCH p=(n)-[*1..4]-(m)
            WHERE ANY(l IN labels(m) WHERE l IN $allowedLabels)
            AND ($types = [] OR ANY(t IN $types WHERE t IN labels(m)))

            WITH n,
                collect(nodes(p))         AS node_lists,
                collect(relationships(p)) AS rel_lists

            RETURN n AS dataset, node_lists, rel_lists
            """

            records = list(self._session.run(
                query, **params, skip=skip, limit=limit))

            datasets = [
                self._build_dataset(
                    record["dataset"],
                    record["node_lists"],
                    record["rel_lists"],
                )
                for record in records
            ]

            return {
                "datasets": datasets,
                "page": criteria.page,
                "pageSize": criteria.pageSize,
                "total": total,
            }

        except Exception as e:
            logger.error("Neo4j retrieve failed: %s", e)
            return {"error": str(e)}

    def update(self, pg_json: Dataset) -> dict:
        """Update properties of existing nodes."""
        try:
            batch = [
                {
                    "id": node.id,
                    "properties": self._sanitize_properties(node.properties)
                }
                for node in pg_json.nodes
            ]

            cypher_query = """
                    UNWIND $batch AS row
                    MATCH (n { id: row.id })
                    SET n += row.properties
                    RETURN count(n) AS updated
                    """
            result = self._session.run(cypher_query, {"batch": batch})
            record = result.single()
            return {
                "status": "success",
                "updated": record["updated"]
            }
        except Exception as e:
            logger.error("Neo4j update failed: %s", e)
            return {"error": str(e), "updated": "0"}

    # def list_ordered_by(self, order_by: str) -> dict:
    #     """Return all datasets ordered by a given property."""
    #     try:
    #         query = f"""
    #             MATCH (c:Dataset)
    #             RETURN c.id AS id, labels(c) AS labels, properties(c) AS properties
    #             ORDER BY c.{order_by} DESC
    #         """
    #         result = self._session.run(query)
    #         return {
    #             "nodes": [
    #                 {"id": r["id"], "labels": r["labels"],
    #                     "properties": r["properties"]}
    #                 for r in result
    #             ]
    #         }
    #     except Exception as e:
    #         logger.error("Neo4j retrieve failed: %s", e)
    #         return {"error": str(e)}

    # def list_by_type(self, target_label: str) -> dict:
    #     """Return datasets that have a connected node of the given type/label."""
    #     try:
    #         query = f"""
    #             MATCH (c:Dataset)-[*]-(m:{target_label})
    #             RETURN DISTINCT {{
    #                 id: c.id,
    #                 labels: labels(c),
    #                 properties: properties(c)
    #             }} AS collection
    #         """
    #         result = self._session.run(query)
    #         return {"nodes": [record["collection"] for record in result]}
    #     except Exception as e:
    #         logger.error("Neo4j retrieve failed: %s", e)
    #         return {"error": str(e)}
