import time
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

    _INDEX_STATEMENTS: list[str] = [
        "CREATE CONSTRAINT dataset_id_unique IF NOT EXISTS "
        "FOR (n:`sc:Dataset`) REQUIRE n.id IS UNIQUE",
        "CREATE INDEX dataset_id IF NOT EXISTS "
        "FOR (n:`sc:Dataset`) ON (n.id)",
        "CREATE INDEX dataset_date_published IF NOT EXISTS "
        "FOR (n:`sc:Dataset`) ON (n.datePublished)",
        "CREATE INDEX dataset_status IF NOT EXISTS "
        "FOR (n:`sc:Dataset`) ON (n.status)",
    ]
    _indexes_ensured: bool = False

    def __init__(self, session: AsyncSession):
        self._session = session

    @classmethod
    async def create_with_indexes(cls, session: AsyncSession) -> "Neo4jDatasetRepository":
        repo = cls(session)
        if not cls._indexes_ensured:
            for stmt in cls._INDEX_STATEMENTS:
                await session.run(stmt)
            cls._indexes_ensured = True
            logger.info("Neo4jDatasetRepository indexes ensured")
            # Warm-up: touch Dataset nodes so Neo4j loads them into page cache.
            # This avoids the cold-cache penalty on the first real query.
            await session.run(
                "MATCH (n:`sc:Dataset`) RETURN count(n) AS c"
            )
            logger.info("Neo4jDatasetRepository page-cache warm-up done")
        return repo

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
        """Fetch a single dataset and its full subgraph by root node id."""
        query = """//cypher
        MATCH (n:`sc:Dataset` {id: $datasetId})

        // Hop 1
        OPTIONAL MATCH (n)-[r1]-(h1)
        WHERE NOT type(r1) IN $forbiddenEdges
        WITH n, collect(DISTINCT h1) AS h1n

        // Hop 2
        UNWIND CASE WHEN size(h1n) > 0 THEN h1n ELSE [null] END AS h1
        OPTIONAL MATCH (h1)-[r2]-(h2)
        WHERE NOT type(r2) IN $forbiddenEdges
          AND NOT h2 IN h1n AND h2 <> n
        WITH n, h1n, collect(DISTINCT h2) AS h2n

        // Hop 3
        UNWIND CASE WHEN size(h2n) > 0 THEN h2n ELSE [null] END AS h2
        OPTIONAL MATCH (h2)-[r3]-(h3)
        WHERE NOT type(r3) IN $forbiddenEdges
          AND NOT h3 IN h1n AND NOT h3 IN h2n AND h3 <> n
        WITH n, h1n, h2n, collect(DISTINCT h3) AS h3n

        // Hop 4
        UNWIND CASE WHEN size(h3n) > 0 THEN h3n ELSE [null] END AS h3
        OPTIONAL MATCH (h3)-[r4]-(h4)
        WHERE NOT type(r4) IN $forbiddenEdges
          AND NOT h4 IN h1n AND NOT h4 IN h2n AND NOT h4 IN h3n AND h4 <> n
        WITH n, h1n, h2n, h3n, collect(DISTINCT h4) AS h4n

        WITH n, [n] + h1n + h2n + h3n + h4n AS allNodes

        // Collect edges as lightweight maps instead of full Relationship objects
        UNWIND allNodes AS a
        OPTIONAL MATCH (a)-[r]->(b)
        WHERE b IN allNodes AND NOT type(r) IN $forbiddenEdges
        WITH allNodes,
             collect(DISTINCT {from: startNode(r).id, to: endNode(r).id, type: type(r), props: properties(r)}) AS edgeMaps

        // Project nodes as lightweight maps instead of full Node objects
        RETURN [x IN allNodes | {id: x.id, labels: labels(x), props: properties(x)}] AS node_maps,
               [e IN edgeMaps WHERE e.from IS NOT NULL] AS edge_maps
        """
        try:
            result = await self._session.run(
                query,
                datasetId=id,
                forbiddenEdges=self.FORBIDDEN_EDGES,
            )
            record = await result.single()
            if not record:
                return None
            graph = self._build_dataset_from_maps(
                record["node_maps"],
                record["edge_maps"],
            )
            return Dataset(nodes=graph.nodes, edges=graph.edges)
        except Exception as e:
            logger.error("Neo4j get failed: %s", e)
            return None

    async def list(self, criteria: DatasetFilter) -> List[Dataset]:
        try:
            t0 = time.monotonic()

            skip = (criteria.page - 1) * criteria.pageSize
            limit = criteria.pageSize
            order = criteria.direction.value.upper()

            def _order_expr(field: DatasetSortField) -> str:
                if field.value == "datePublished":
                    return f"n.`datePublished` {order}"
                return f"n.`{field.value}` {order}"

            order_clause = (
                ", ".join([_order_expr(k) for k in criteria.orderBy])
                if criteria.orderBy else "n.id ASC"
            )

            types = criteria.resolved_types

            params = dict(
                nodeIds=criteria.nodeIds or [],
                publishedDateFrom=criteria.publishedFrom.isoformat(
                ) if criteria.publishedFrom else None,
                publishedDateTo=criteria.publishedTo.isoformat() if criteria.publishedTo else None,
                status=criteria.status.value if criteria.status is not None else None,
                types=types,
                forbiddenEdges=self.FORBIDDEN_EDGES,
                skip=skip,
                limit=limit,
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
            AND ($publishedDateFrom IS NULL OR n.datePublished >= $publishedDateFrom)
            AND ($publishedDateTo IS NULL OR n.datePublished <= $publishedDateTo)
            AND ($status IS NULL OR n.status = $status)
            AND ($types = []
            OR EXISTS {
                MATCH path=(n)-[*1..4]-(m)
                WHERE NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
                AND ANY(t IN $types WHERE t IN labels(m))
            })
            RETURN count(DISTINCT n) AS total
            """

            # ---------------- ID QUERY (PAGINATED) ----------------
            id_query = f"""//cypher
            MATCH (n:`sc:Dataset`)
            WHERE (
                $nodeIds = []
                OR EXISTS {{
                    MATCH path=(n)-[*0..4]-(m)
                    WHERE m.id IN $nodeIds
                    AND NONE(r IN relationships(path) WHERE type(r) IN $forbiddenEdges)
                }}
            )
            AND ($publishedDateFrom IS NULL OR n.datePublished >= $publishedDateFrom)
            AND ($publishedDateTo IS NULL OR n.datePublished <= $publishedDateTo)
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

            RETURN n.id AS id
            """

            # Execute both queries
            t1 = time.monotonic()

            count_result = await self._session.run(count_query, **params)
            count_record = await count_result.single()
            total = count_record["total"] if count_record else 0

            id_result = await self._session.run(id_query, **params)
            ids = [record["id"] async for record in id_result]

            t2 = time.monotonic()

            if not ids:
                return {
                    "datasets": [],
                    "page": criteria.page,
                    "pageSize": criteria.pageSize,
                    "total": total,
                }

            # ---------------- FETCH SUBGRAPHS (PARALLEL) ----------------
            datasets = []
            for id_ in ids:
                ds = await self.get(id_)
                if ds is not None:
                    datasets.append(ds)

            # Filter None (safety)
            datasets = [ds for ds in datasets if ds is not None]

            t3 = time.monotonic()

            logger.info(
                "Dataset list: filter+ids=%.3fs  subgraphs=%.3fs  total=%d",
                t2 - t1, t3 - t2, len(datasets),
            )

            # ---------------- PROPERTY FILTERING (UNCHANGED) ----------------
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
