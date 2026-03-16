import logging
from os import getenv
from pathlib import Path
from tomllib import loads as loads_toml

import uvicorn
from fastapi import FastAPI

from moma_management.api.v1.routes import router
from moma_management.di import container_lifespan

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pyproject = loads_toml(Path("pyproject.toml").read_text())
project_version = pyproject["project"]["version"]
ROOT_PATH = getenv("ROOT_PATH", "")


app = FastAPI(
    title="MoMa API",
    description="""
## MoMa Management API

REST API for ingesting, querying, and managing **MoMa** (Model of Models and Artifacts) graph datasets.
""",
    lifespan=container_lifespan,
    version=project_version,
    root_path=ROOT_PATH,
    openapi_tags=[
        {
            "name": "datasets",
            "description": "Ingest, retrieve, convert, validate, and delete dataset profiles.",
        },
        {
            "name": "nodes",
            "description": "Retrieve and update individual graph nodes within a dataset subgraph.",
        },
        {
            "name": "health",
            "description": "Service liveness probe.",
        },
    ],
)


@app.get("/")
async def root():
    return {"message": "MoMa API is up and running"}


app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
