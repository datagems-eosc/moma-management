from fastapi import FastAPI, HTTPException
from typing import Any, Dict, List, Optional
from datetime import date
from fastapi import Query
from app.converters import Croissant2PGjson, heavyProfiling2PGjson, lightProfiling2PGjson
from app.manager import pgjson2Neo4j, retrieveMetadata, retrieveDatasets, updateNodeProperties, deleteDatasetsByIds, upload_nodes, upload_edges, retrieveDatasetsByType
from app.schema import PGSchema
import json

app = FastAPI(title="MoMa API")

@app.get("/")
async def root():
    return {"message": "MoMa API is up and running"}

@app.post("/ingestProfile2MoMa")
async def ingestProfile2MoMa(input_data: Dict[str, Any]):
    try:
        pg_json = Croissant2PGjson(input_data)
        neo4j_result = pgjson2Neo4j(pg_json)
        return {
            "status": neo4j_result,
            "metadata": pg_json
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.post("/ingestLightProfiling")
async def ingestLightProfiling(input_data: dict):
    pg_json = lightProfiling2PGjson(input_data)
    neo4j_result = pgjson2Neo4j(pg_json)
    return {
        "status": neo4j_result,
        "metadata": pg_json
    }

@app.post("/ingestHeavyProfiling")
async def ingestHeavyProfiling(input_data: dict):
    pg_json = heavyProfiling2PGjson(input_data)
    neo4j_result = pgjson2Neo4j(pg_json)
    return {
        "status": neo4j_result,
        "metadata": pg_json
    }

@app.post("/convertLightProfiling2PGjson")
async def convertLightProfiling2PGjson(input_data: dict):
    pg_json = lightProfiling2PGjson(input_data)
    return {
        "metadata": pg_json
    }

@app.post("/convertHeavyProfiling2PGjson")
async def convertHeavyProfiling2PGjson(input_data: dict):
    pg_json = heavyProfiling2PGjson(input_data)
    return {
        "metadata": pg_json
    }

@app.post("/addMoMaNodes")
async def addMoMaNodes(pg_json: dict):
    schema = PGSchema()
    report = schema.validate(pg_json)

    # If not valid, return report and message
    if not report["is_valid"]:
        return {
            "status": "error: PG-JSON validation failed",
            "report": report
        }
    # If valid, proceed to upload to Neo4j
    neo4j_result = upload_nodes(pg_json)
    return {
        "status": neo4j_result,
        "report": report
    }

@app.post("/addMoMaEdjes")
async def addMoMaEdjes(pg_json: dict):
    neo4j_result = upload_edges(pg_json)
    return {
        "status": neo4j_result
    }

@app.post("/addMoMaGraph")
async def addMoMaGraph(pg_json: dict):
    schema = PGSchema()
    report = schema.validate(pg_json)

    # If not valid, return report and message
    if not report["is_valid"]:
        return {
            "status": "error: PG-JSON validation failed",
            "report": report
        }
    # If valid, proceed to upload to Neo4j
    neo4j_result = pgjson2Neo4j(pg_json)
    return {
        "status": neo4j_result,
        "report": report
    }

@app.post("/validatePGjson")
async def validatePGjson(pg_json: dict):
    # strict = True
    schema = PGSchema()
    report = schema.validate(pg_json)
    return {
        # "strict": strict,
        "is_valid": report["is_valid"],
        "report": report
    }

@app.post("/updateNodes")
async def updateNodes(pg_json: Dict[str, Any]):
    try:
        metadata = updateNodeProperties(pg_json=pg_json)
        return metadata

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

"""
@app.get("/deleteDatasets")
async def deleteDatasets(ids: List[str] = Query(default=[])):
    try:
        metadata = deleteDatasetsByIds(ids)
        return metadata

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
"""

@app.get("/getMoMaObject")
async def getMoMaObject(id: str):
    try:
        metadata = retrieveMetadata(id)
        return {
           "metadata": metadata
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.get("/getDatasets")
async def getDatasets(
    nodeIds: List[str] = Query(default=[]),
    properties: List[str] = Query(default=[]),
    types: List[str] = Query(default=[]),
    orderBy: List[str] = Query(default=[]),
    direction: int = 1,
    publishedDateFrom: Optional[date] = None,
    publishedDateTo: Optional[date] = None,
    status: Optional[str] = None
):
    try:
        metadata = retrieveDatasets(
            nodeIds=nodeIds or [],
            properties=properties or [],
            types=types or [],
            orderBy=orderBy or [],
            direction=direction,
            publishedDateFrom=publishedDateFrom,
            publishedDateTo=publishedDateTo,
            status=status
        )

        return {
            "metadata": metadata
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


#uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
#gunicorn -k uvicorn.workers.UvicornWorker app.main:app --bind 0.0.0.0:8000
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
