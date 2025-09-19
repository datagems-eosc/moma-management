import os
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

def retrieveCollection(nodeId: str) -> dict:
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        query = """
            MATCH (n:Collection {id: $nodeId})
            OPTIONAL MATCH (n)-[r]-(m)
            WHERE m:Dataset OR m:DatasetPart
            OPTIONAL MATCH (m)-[r2]-(d)
            WHERE (d:Dataset OR d:DatasetPart) 
              AND labels(m) = labels(d)
            RETURN {
                id: n.id,
                labels: labels(n),
                properties: properties(n)
            } AS nodeInfo,
            collect(DISTINCT {
                id: m.id,
                labels: labels(m),
                properties: properties(m)
            }) AS connectedNodes,
            collect(DISTINCT {
                id: d.id,
                labels: labels(d),
                properties: properties(d)
            }) AS datasetNodes,
            collect(DISTINCT {
                start: startNode(r).id,
                end: endNode(r).id,
                type: type(r),
                properties: properties(r)
            }) AS edges1,
            collect(DISTINCT {
                start: startNode(r2).id,
                end: endNode(r2).id,
                type: type(r2),
                properties: properties(r2)
            }) AS edges2
        """

        with driver.session() as session:
            record = session.run(query, nodeId=nodeId).single()
        driver.close()

        if record:
            nodeMetadata = record["nodeInfo"]

            connectedNodes = [
                n for n in record["connectedNodes"] if n.get("id") is not None
            ]
            datasetNodes = [
                n for n in record["datasetNodes"] if n.get("id") is not None
            ]
            edges1 = [e for e in record["edges1"] if e.get("type") is not None]
            edges2 = [e for e in record["edges2"] if e.get("type") is not None]

            result = {
                "nodes": [nodeMetadata] + connectedNodes + datasetNodes,
                "edges": edges1 + edges2
            }
        else:
            result = {"nodes": [], "edges": []}

        return result

    except Exception as e:
        logging.error(f"Neo4j retrieve failed: {e}")
        return {"error": str(e)}