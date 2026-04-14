from logging import getLogger
from typing import List, Optional

from neo4j import AsyncManagedTransaction, AsyncSession

from moma_management.domain.dataset import Dataset
from moma_management.domain.filters import DatasetFilter, DatasetSortField
from moma_management.domain.generated.moma_schema import MoMaGraphModel
from moma_management.repository.neo4j_pgson_mixin import Neo4jPgJsonMixin

logger = getLogger(__name__)


class Neo4jDatasetRepository(Neo4jPgJsonMixin):
    """Neo4j-backed implementation of DatasetRepository."""

    # These edges links datasets to models or APs so must be excluded when maniulating datasets in isolation
    FORBIDDEN_EDGES: list[str] = ["fitted_on", "input",
                                  "output", "perform_inference", "trained_on"]

    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(self, dataset: Dataset) -> str:
        """Store a full PG-JSON graph (nodes + edges)."""
        try:
            await self._session.execute_write(self.create_pgson, dataset)
            return "success"
        except Exception as e:
            logger.error("Neo4j upload failed: %s", e)
            return f"Error: {str(e)}"

    async def delete(self, id: str) -> int:
        """
        Delete a dataset and its full connected subgraph by id.
        """
        query = """//cypher
            MATCH (d:`sc:Dataset` {id: $datasetId})
            OPTIONAL MATCH path=(d)-[*1..10]-(m)
            WHERE NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
            WITH d, collect(DISTINCT m) AS related
            FOREACH (n IN related | DETACH DELETE n)
            DETACH DELETE d
            RETURN 1 AS deletedRows
        """
        result = await self._session.run(
            query, datasetId=id, forbiddenEdges=self.FORBIDDEN_EDGES)
        record = await result.single()
        return record["deletedRows"] if record else 0

    async def has_referencing_aps(self, dataset_id: str) -> bool:
        """Return True if at least one AP references a node in this dataset."""
        query = """//cypher
            MATCH (d:`sc:Dataset` {id: $datasetId})
            MATCH path=(d)-[*1..4]-(data)
            WHERE NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
            WITH collect(DISTINCT data) AS data_nodes
            UNWIND data_nodes AS dn
            MATCH (:Operator)-[:input]->(dn)
            RETURN true AS referenced
            LIMIT 1
        """
        result = await self._session.run(
            query, datasetId=dataset_id, forbiddenEdges=self.FORBIDDEN_EDGES
        )
        record = await result.single()
        return record is not None

    async def get(self, id: str) -> Optional[Dataset]:
        """
        Retrieve the dataset with the given ID
        """
        query = """//cypher
            MATCH (root:`sc:Dataset` {id: $datasetId})
            OPTIONAL MATCH path=(root)-[*1..4]-(m)
            WHERE NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
            RETURN root, m, relationships(path) AS r
        """
        result = await self._session.run(query, datasetId=id,
                                         forbiddenEdges=self.FORBIDDEN_EDGES)
        rows = [record async for record in result]

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

    async def list(self, criteria: DatasetFilter) -> List[Dataset]:
        try:
            skip = (criteria.page - 1) * criteria.pageSize
            limit = criteria.pageSize

            order = criteria.direction.value.upper()
            # datePublished is stored as an ISO string; wrap it in date() so
            # that any pre-normalised values still sort chronologically, not
            # lexicographically (e.g. "10-03-2025" < "2025-03-10" as strings).

            def _order_expr(field: DatasetSortField) -> str:
                if field.value == "datePublished":
                    return f"date(coalesce(n.`datePublished`, '1970-01-01')) {order}"
                return f"n.`{field.value}` {order}"

            order_clause = (
                ", ".join([_order_expr(k) for k in criteria.orderBy])
                if criteria.orderBy else "n.id ASC"
            )

            types = criteria.resolved_types

            params = dict(
                nodeIds=criteria.nodeIds or [],
                publishedDateFrom=criteria.publishedFrom,
                publishedDateTo=criteria.publishedTo,
                status=criteria.status.value if criteria.status is not None else None,
                types=types,
                forbiddenEdges=self.FORBIDDEN_EDGES,
            )

            # ---------------- COUNT QUERY ----------------

            count_query = """//cypher
            MATCH (n:`sc:Dataset`)
            WHERE (
                $nodeIds = []
                OR EXISTS {
                    MATCH path=(n)-[*0..4]-(m)
                    WHERE m.id IN $nodeIds
                    AND NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
                }
            )
            AND ($publishedDateFrom IS NULL OR date(n.datePublished) >= $publishedDateFrom)
            AND ($publishedDateTo IS NULL OR date(n.datePublished) <= $publishedDateTo)
            AND ($status IS NULL OR n.status = $status)

            WITH n
            WHERE $types = []
               OR EXISTS {
                   MATCH path=(n)-[*1..4]-(m)
                   WHERE NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
                   AND ANY(t IN $types WHERE t IN labels(m))
               }

            RETURN count(DISTINCT n) AS total
            """

            count_result = await self._session.run(count_query, **params)
            total_record = await count_result.single()
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
            #
            # Subgraph expansion uses explicit hop-by-hop traversal with
            # DISTINCT at each level to avoid the path-explosion problem that
            # occurs when collect(nodes(p)) is used with variable-length paths.
            # Collecting entire PATH objects forces Neo4j to materialise every
            # distinct route to every node, which causes OOM on large subgraphs
            # (many fields × statistics).  Collecting DISTINCT node/rel objects
            # directly is O(|nodes| + |rels|) instead of O(|paths|).
            #
            # CASE WHEN … ELSE [null] END on each UNWIND ensures that datasets
            # with no neighbours at a given hop still produce a row so the root
            # node (n) is included in the final result.

            query = f"""//cypher
            MATCH (n:`sc:Dataset`)
            WHERE (
                $nodeIds = []
                OR EXISTS {{
                    MATCH path=(n)-[*0..4]-(m)
                    WHERE m.id IN $nodeIds
                    AND NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
                }}
            )
            AND ($publishedDateFrom IS NULL OR date(n.datePublished) >= $publishedDateFrom)
            AND ($publishedDateTo IS NULL OR date(n.datePublished) <= $publishedDateTo)
            AND ($status IS NULL OR n.status = $status)
            AND ($types = []
               OR EXISTS {{
                   MATCH path=(n)-[*1..4]-(m)
                   WHERE NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
                   AND ANY(t IN $types WHERE t IN labels(m))
               }})

            WITH n
            ORDER BY {order_clause}
            SKIP $skip
            LIMIT $limit

            // Hop 1 – direct neighbours of the dataset root.
            // NOTE: $types is intentionally NOT applied here.  The filter above
            // already selected only qualifying datasets; the subgraph expansion
            // must return ALL connected nodes (so a CSV-filtered dataset still
            // includes its PDF/Table/… siblings in the response).
            OPTIONAL MATCH (n)-[r1]-(h1)
            WHERE NOT type(r1) IN $forbiddenEdges
            WITH n,
                 collect(DISTINCT h1) AS h1n,
                 collect(DISTINCT r1) AS h1r

            // Hop 2 – expand from hop-1 nodes, skipping already-visited
            UNWIND CASE WHEN size(h1n) > 0 THEN h1n ELSE [null] END AS h1
            OPTIONAL MATCH (h1)-[r2]-(h2)
            WHERE NOT type(r2) IN $forbiddenEdges
              AND NOT h2 IN h1n
            WITH n, h1n, h1r,
                 collect(DISTINCT h2) AS h2n,
                 collect(DISTINCT r2) AS h2r

            // Hop 3 – expand from hop-2 nodes, skipping already-visited
            UNWIND CASE WHEN size(h2n) > 0 THEN h2n ELSE [null] END AS h2
            OPTIONAL MATCH (h2)-[r3]-(h3)
            WHERE NOT type(r3) IN $forbiddenEdges
              AND NOT h3 IN h1n AND NOT h3 IN h2n
            WITH n, h1n, h1r, h2n, h2r,
                 collect(DISTINCT h3) AS h3n,
                 collect(DISTINCT r3) AS h3r

            // Hop 4 – expand from hop-3 nodes, skipping already-visited
            UNWIND CASE WHEN size(h3n) > 0 THEN h3n ELSE [null] END AS h3
            OPTIONAL MATCH (h3)-[r4]-(h4)
            WHERE NOT type(r4) IN $forbiddenEdges
              AND NOT h4 IN h1n AND NOT h4 IN h2n AND NOT h4 IN h3n
            WITH n, h1n, h1r, h2n, h2r, h3n, h3r,
                 collect(DISTINCT h4) AS h4n,
                 collect(DISTINCT r4) AS h4r

            RETURN n AS dataset,
                   [h1n + h2n + h3n + h4n] AS node_lists,
                   [h1r + h2r + h3r + h4r] AS rel_lists
            """

            result = await self._session.run(
                query, **params, skip=skip, limit=limit)
            records = [record async for record in result]

            datasets = [
                self._build_dataset(
                    record["dataset"],
                    record["node_lists"],
                    record["rel_lists"],
                )
                for record in records
            ]

            if criteria.properties:
                prop_values = {p.value for p in criteria.properties}
                scalar_props = prop_values - {"distribution", "recordSet"}
                include_subgraph = bool(
                    prop_values & {"distribution", "recordSet"})

                filtered = []
                for ds in datasets:
                    nodes = []
                    for n in ds.nodes:
                        if "sc:Dataset" in n.labels:
                            filtered_props = {
                                k: v for k, v in n.properties.items()
                                if k in scalar_props
                            }
                            nodes.append(n.model_copy(
                                update={"properties": filtered_props}))
                        elif include_subgraph:
                            nodes.append(n)
                    edges = ds.edges if include_subgraph else None
                    filtered.append(MoMaGraphModel(nodes=nodes, edges=edges))
                datasets = filtered

            return {
                "datasets": datasets,
                "page": criteria.page,
                "pageSize": criteria.pageSize,
                "total": total,
            }

        except Exception as e:
            logger.error("Neo4j retrieve failed: %s", e)
            return {"error": str(e)}

    async def update(self, pg_json: Dataset) -> dict:
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
            result = await self._session.run(cypher_query, {"batch": batch})
            record = await result.single()
            return {
                "status": "success",
                "updated": record["updated"]
            }
        except Exception as e:
            logger.error("Neo4j update failed: %s", e)
            return {"error": str(e), "updated": "0"}
