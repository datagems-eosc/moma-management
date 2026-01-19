import os
from typing import List
from datetime import date
from neo4j import GraphDatabase
import logging

# credentials for Neo4j
#NEO4J_URI = "bolt://localhost:7687"
#NEO4J_USER = "neo4j"
#NEO4J_PASSWORD = "datagems"

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "datagems")

def clean_keys(props):
    cleaned = {}
    for k, v in props.items():
        new_key = k.strip().replace(" ", "_")
        if isinstance(v, list):
            cleaned[new_key] = v if len(v) > 0 else None
        else:
            cleaned[new_key] = v
    return cleaned


def upload_all_nodes(tx, data):
    # Upload nodes
    for node in data["nodes"]:
        node_id = node["id"]
        labels = ":".join(node["labels"])
        props = clean_keys(node["properties"])
        props["id"] = node_id

        prop_keys = ", ".join(f"{k}: ${k}" for k in props.keys())
        query = f"MERGE (n:{labels} {{id: $id}}) SET n += {{{prop_keys}}}"

        tx.run(query, props)


def upload_all_edges(tx, nodes):
    # Upload edges
    for edge in nodes["edges"]:
        from_id = edge["start"]
        to_id = edge["end"]
        labels = ":".join(edge["labels"])
        props = clean_keys(edge.get("properties", {}))

        prop_keys = ", ".join(f"{k}: ${k}" for k in props.keys()) if props else ""

        query = f"""
                MATCH (a {{id: $from_id}})
                MATCH (b {{id: $to_id}})
                MERGE (a)-[r:{labels}]->(b)
                """
        if props:
            query += f"\nSET r += {{{prop_keys}}}"

        parameters = {"from_id": from_id, "to_id": to_id}
        parameters.update(props)
        tx.run(query, parameters)

def upload_nodes(pg_json: dict) -> str:
    if "nodes" not in pg_json:
        return "Error: PG-JSON must contain 'nodes'"

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD)
    )
    try:
        with driver.session() as session:
            session.execute_write(upload_all_nodes, pg_json)

        return "success"

    except Exception as e:
        logging.error(f"Neo4j node upload failed: {e}")
        return f"Error: {str(e)}"

    finally:
        driver.close()

def upload_edges(pg_json: dict) -> str:
    if "edges" not in pg_json:
        return "Error: PG-JSON must contain 'edges'"

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD)
    )
    try:
        with driver.session() as session:
            session.execute_write(upload_all_edges, pg_json)

        return "success"

    except Exception as e:
        logging.error(f"Neo4j edge upload failed: {e}")
        return f"Error: {str(e)}"

    finally:
        driver.close()

def pgjson2Neo4j(pg_json: dict) -> str:
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            session.execute_write(upload_all_nodes, pg_json)
            session.execute_write(upload_all_edges, pg_json)
        driver.close()
        return "success"

    except Exception as e:
        logging.error(f"Neo4j upload failed: {e}")
        return f"Error: {str(e)}"

def deleteDatasetsByIds(datasetIds: list[str]) -> dict:
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        query = """
                MATCH (d:Dataset)
                WHERE $datasetIds = [] OR d.id IN $datasetIds
                OPTIONAL MATCH p = (d)-[*]-(m)
                WHERE m:Data OR m:DataPart
                FOREACH (x IN nodes(p) | DETACH DELETE x)
                DETACH DELETE d
                RETURN count(*) AS deletedNodes
               """

        with driver.session() as session:
            result = session.run(query, datasetIds=datasetIds)
            record = result.single()

        driver.close()

        return {
            "status": "success",
            "deletedDatasets": record["deletedNodes"]
        }

    except Exception as e:
        return {
            "error": str(e),
            "deletedDatasets": 0
        }


def retrieveMetadata(nodeId: str) -> dict:
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        query = """
                MATCH (n)
                WHERE n.id = $nodeId
                RETURN {
                    id: n.id,
                    labels: labels(n),
                    properties: properties(n)
                } AS nodeInfo
               """
        with driver.session() as session:
            result = session.run(query, nodeId=nodeId)
            record = result.single()
        driver.close()

        if record:
            nodeMetadata = record["nodeInfo"]
            result = {
                "nodes": [nodeMetadata],
                "edges": []
            }
        else:
            result = {
                "nodes": [],
                "edges": []
            }
        return result

    except Exception as e:
        logging.error(f"Neo4j retrieve failed: {e}")
        return f"Error: {str(e)}"


#Retrieve a Dataset nodes and all transitively connected nodes that has label Dataset or datasetPart based on the filtering criteria (parameters)
def retrieveDatasets(nodeIds: List[str], properties: List[str], types: List[str], orderBy: List[str], direction: int, publishedDateFrom: date, publishedDateTo: date) -> dict:
    driver = None
    try:
        driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )

        order = "DESC" if direction == -1 else "ASC"
        order_clause = (
            ", ".join([f"n.`{k}` {order}" for k in orderBy])
            if orderBy else ""
        )

        query = f"""
                MATCH (n:Dataset)
                WHERE ($nodeIds = [] OR n.id IN $nodeIds)
                  AND ($publishedDateFrom IS NULL OR n.datePublished >= $publishedDateFrom)
                  AND ($publishedDateTo IS NULL OR n.datePublished <= $publishedDateTo)

                OPTIONAL MATCH (n)-[r*1..3]-(m)
                WHERE m:DataPart OR m:FileObject OR m:FileSet

                RETURN n, m, r
                {f"ORDER BY {order_clause}" if order_clause else ""}
            """

        # ----  Preferred Sets ----
        allowed_labels = {"DataPart", "Data"}
        distribution_labels = {"FileObject", "FileSet"}
        recordset_labels = {"Field"}

        properties_set = set(properties or [])
        types_set = set(types or [])

        with driver.session() as session:
            result = session.run(
                query,
                nodeIds=nodeIds or [],
                publishedDateFrom=publishedDateFrom,
                publishedDateTo=publishedDateTo
            )

            #  Phase 1: COLLECT
            dataset_nodes = {}
            dataset_to_connected = {}
            dataset_to_edges = {}

            for record in result:
                n = record["n"]
                m = record["m"]
                rels = record["r"] or []

                dataset_id = n["id"]
                dataset_nodes[dataset_id] = n

                dataset_to_connected.setdefault(dataset_id, [])
                dataset_to_edges.setdefault(dataset_id, [])

                if m:
                    dataset_to_connected[dataset_id].append(m)

                for rel in rels:
                    dataset_to_edges[dataset_id].append({
                        "start": rel.start_node["id"],
                        "end": rel.end_node["id"],
                        "type": rel.type,
                        "properties": dict(rel)
                    })

            #  Phase 2: FILTER & EMIT
            nodes_dict = {}
            edges = []

            for dataset_id, connected_nodes in dataset_to_connected.items():

                # ---- STRICT types filter (Dataset-level) ----
                if types_set:
                    if not any(types_set & set(m_node.labels) for m_node in connected_nodes):
                        continue  # Dataset excluded

                # ---- Add Dataset node ----
                n = dataset_nodes[dataset_id]
                nodes_dict[dataset_id] = {
                    "id": n["id"],
                    "labels": list(n.labels),
                    "properties": {
                        k: v for k, v in dict(n).items()
                        if not properties_set or k in properties_set
                    }
                }

                # ----  Distribution / RecordSet filtering ----
                for m_node in connected_nodes:
                    node_labels = set(m_node.labels)

                    # Only allowed labels
                    if not node_labels & allowed_labels:
                        continue

                    if properties_set:
                        if (
                                ("distribution" in properties_set and node_labels & distribution_labels)
                                or ("recordSet" in properties_set and node_labels & recordset_labels)
                        ):
                            mid = m_node["id"]
                            if mid not in nodes_dict:
                                nodes_dict[mid] = {
                                    "id": mid,
                                    "labels": list(m_node.labels),
                                    "properties": dict(m_node)
                                }
                    else:
                        mid = m_node["id"]
                        if mid not in nodes_dict:
                            nodes_dict[mid] = {
                                "id": mid,
                                "labels": list(m_node.labels),
                                "properties": dict(m_node)
                            }

                for e in dataset_to_edges[dataset_id]:
                    if e["start"] in nodes_dict and e["end"] in nodes_dict:
                        edges.append(e)

            return {
                "nodes": list(nodes_dict.values()),
                "edges": edges
            }

    except Exception as e:
        logging.error(f"Neo4j retrieve failed: {e}")
        return {"error": str(e)}

    finally:
        if driver:
            driver.close()


def retrieveAllDatasets() -> dict:
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        query = """
            MATCH (c:Dataset)
            RETURN c.id AS id, labels(c) AS labels, properties(c) AS properties
        """
        with driver.session() as session:
            result = session.run(query)
            collections = []
            for record in result:
                collections.append({
                    "id": record["id"],
                    "labels": record["labels"],
                    "properties": record["properties"]
                })

        driver.close()
        return {"nodes": collections}

    except Exception as e:
        logging.error(f"Neo4j retrieve failed: {e}")
        return {"error": str(e)}

from neo4j import GraphDatabase

def updateNodeProperties(pg_json: dict):
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

        nodes = pg_json.get("nodes", [])

        batch = []
        for node in nodes:
            node_id = node.get("id")  # or node.get("@id") depending on your input
            if not node_id:
                continue

            props = node.get("properties", {})
            batch.append({
                "id": node_id,
                "properties": props
            })

        cypher_query = """
                UNWIND $batch AS row
                MATCH (n { id: row.id })
                SET n += row.properties
                RETURN count(n) AS updated
                """

        with driver.session() as session:
            result = session.run(cypher_query, {"batch": batch})
            record = result.single()

        driver.close()

        return {
            "status": "success",
            "updated": record["updated"]
        }

    except Exception as e:
        logging.error(f"Neo4j retrieve failed: {e}")
        return {"error": str(e), "updated": "0"}


def retrieveDatasetsOrderedBy(orderBy: str) -> dict:
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        query = f"""
            MATCH (c:Dataset)
            RETURN c.id AS id, labels(c) AS labels, properties(c) AS properties
            ORDER BY c.{orderBy} DESC
        """

        with driver.session() as session:
            result = session.run(query)
            collections = []
            for record in result:
                collections.append({
                    "id": record["id"],
                    "labels": record["labels"],
                    "properties": record["properties"]
                })

        driver.close()
        return {"nodes": collections}

    except Exception as e:
        logging.error(f"Neo4j retrieve failed: {e}")
        return {"error": str(e)}


#Retrieve all Datasets nodes that are transitively connected to at least one node with the given targetLabel.
def retrieveDatasetsByType(targetLabel: str) -> dict:
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

        # Inject targetLabel carefully (validate input before using in prod!)
        query = f"""
            MATCH (c:Dataset)-[*]-(m:{targetLabel})
            RETURN DISTINCT {{
                id: c.id,
                labels: labels(c),
                properties: properties(c)
            }} AS collection
        """

        with driver.session() as session:
            result = session.run(query)
            collections = [record["collection"] for record in result]

        driver.close()
        return {"nodes": collections}

    except Exception as e:
        logging.error(f"Neo4j retrieve failed: {e}")
        return {"error": str(e)}


