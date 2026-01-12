from fastapi import FastAPI, HTTPException
from typing import Any, Dict, List, Optional
from app.converters import Croissant2PGjson
from datetime import date
from fastapi import Query
from app.manager import pgjson2Neo4j, retrieveMetadata, retrieveDatasets, updateNodeProperties, deleteDatasetsByIds, upload_nodes, upload_edges
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
@app.post("/addMoMaNodes")
def addMoMaNodes(pg_json: dict):
    neo4j_result = upload_nodes(pg_json)
    return {
        "status": neo4j_result
    }
@app.post("/addMoMaEdjes")
def addMoMaEdjes(pg_json: dict):
    neo4j_result = upload_edges(pg_json)
    return {
        "status": neo4j_result
    }
@app.post("/addMoMaGraph")
def addMoMaGraph(pg_json: dict):
    neo4j_result = pgjson2Neo4j(pg_json)
    return {
        "status": neo4j_result
    }

@app.post("/updateNodes")
async def updateNodes(pg_json: Dict[str, Any]):
    try:
        metadata = updateNodeProperties(pg_json=pg_json)
        return metadata

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.get("/deleteDatasets")
async def deleteDatasets(ids: List[str] = Query(default=[])):
    try:
        metadata = deleteDatasetsByIds(ids)
        return metadata

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

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
    status: str = "ready"
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

@app.get("/test_id")
async def test_id(id: str):
    return {"id": id}

#uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
#gunicorn -k uvicorn.workers.UvicornWorker app.main:app --bind 0.0.0.0:8000
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
