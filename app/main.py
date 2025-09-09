from fastapi import FastAPI, HTTPException
from typing import Any, Dict
from app.converters import Croissant2PGjson
from app.manager import pgjson2Neo4j, retrieveMetadata
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
            "status": neo4j_result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.get("/retrieveMoMaMetadata")
async def retrieveMoMaMetadata(id: str):
    try:
        #metadata = retrieveMetadata(id)
        return {"metadata": "retrieveMoMaMetadata  is running"}
        #return {
        #    "metadata": metadata
        #}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


#uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
#gunicorn -k uvicorn.workers.UvicornWorker app.main:app --bind 0.0.0.0:8000
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
