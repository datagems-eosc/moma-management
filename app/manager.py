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


#Retrieve a Collection node and all transitively connected nodes that has label Dataset or DatasetPart
def retrieveDatasets(nodeIds: List[str], properties: List[str], types: List[str], orderBy: List[str], direction: int, publishedDateFrom: date, publishedDateTo: date,  status: str) -> dict:
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

        order = "DESC" if direction == -1 else "ASC"
        query = """
            MATCH (n:Dataset)
            WHERE ($nodeIds = [] OR n.id IN $nodeIds)
            AND n.status = $status
            AND ($publishedDateFrom IS NULL OR n.datePublished >= $publishedDateFrom)
            AND ($publishedDateTo IS NULL OR n.datePublished <= $publishedDateTo)
            
            // Find all paths from the Dataset to any node with label Data or DataPart
            OPTIONAL MATCH path = (n)-[*]-(m:Data|DataPart)

            // Collect all m nodes and relationships
            WITH n, collect(DISTINCT m) AS allM, collect(DISTINCT relationships(path)) AS paths

            // Keep n only if $types is empty OR at least one m matches $types
            WHERE size($types) = 0 OR size([r IN allM WHERE ANY(t IN labels(r) WHERE t IN $types)]) > 0

            // Prepare reachableNodes for return (only include distribution / recordSet if present)
            WITH n,
            CASE WHEN ("distribution" IN $properties OR "recordSet" IN $properties)
                THEN [r IN allM WHERE 
                    ("distribution" IN $properties AND (r:FileObject OR r:FileSet)) OR
                    ("recordSet" IN $properties AND r:RecordSet)
                ]
                ELSE []
            END AS reachableNodes,
            paths AS originalPaths

            WITH n, reachableNodes,
                CASE WHEN size(reachableNodes) = 0
                    THEN []   // no reachableNodes -> no edges
                    ELSE originalPaths
                END AS paths

            // Prepare sort keys
            WITH n, reachableNodes, paths,
                [key IN $orderBy | n[key]] AS sortKeys
            ORDER BY sortKeys {_ORDER_}
            
            // Compute properties map 
            WITH n, reachableNodes, paths,
            [ key IN (CASE WHEN size($properties)=0 THEN keys(n) ELSE $properties END)
                WHERE n[key] IS NOT NULL
                | { key: key, value: n[key] }
            ] AS nodeProperties
            
            RETURN {
                id: n.id,
                labels: labels(n),
                properties: nodeProperties
            } AS nodeInfo,
            [node IN reachableNodes | {
                id: node.id,
                labels: labels(node),
                properties: properties(node)
            }] AS nodes,
            reduce(acc=[], rels IN paths | acc + [r IN rels | {
                start: startNode(r).id,
                end: endNode(r).id,
                type: type(r),
                properties: properties(r)
            }]) AS edges
        """
        query = query.replace("{_ORDER_}", order)

        with driver.session() as session:
            records = session.run(
                query,
                nodeIds=nodeIds or [],
                properties=properties or [],
                publishedDateFrom=publishedDateFrom,
                publishedDateTo=publishedDateTo,
                types=types or [],
                orderBy=orderBy or [],
                status=status
            ).data()

        driver.close()

        # Final merged output
        resultNodes = []
        resultEdges = []
        for record in records:
            # SAFE parsing version
            nodeMetadata = record["nodeInfo"]
            reachableNodes = [n for n in record["nodes"] if n.get("id") is not None]
            edges = [e for e in record["edges"] if e.get("type") is not None]

            # Convert dynamic key/value list -> real dict
            prop_list = nodeMetadata.get("properties", [])
            nodeMetadata["properties"] = {
                item["key"]: item["value"] for item in prop_list
            }

            resultNodes.append(nodeMetadata)
            resultNodes.extend(reachableNodes)
            resultEdges.extend(edges)

            result = {
                "nodes": resultNodes,
                "edges": resultEdges
            }
        if not records:
            result = {"nodes": [], "edges": []}

        return result

    except Exception as e:
        logging.error(f"Neo4j retrieve failed: {e}")
        return {"error": str(e)}

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


