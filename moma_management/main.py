import logging
from os import getenv
from pathlib import Path
from tomllib import loads as loads_toml

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from moma_management.api.v1.routes import router
from moma_management.di import container_lifespan
from moma_management.domain.exceptions import (
    ConversionError,
    MomaError,
    NotFoundError,
    ValidationError,
)

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


app.include_router(router, prefix="/api/v1")


@app.exception_handler(NotFoundError)
async def not_found_handler(request: Request, exc: NotFoundError) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": exc.message})


@app.exception_handler(ConversionError)
async def conversion_error_handler(request: Request, exc: ConversionError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"detail": exc.message})


@app.exception_handler(ValidationError)
async def validation_error_handler(request: Request, exc: ValidationError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"detail": exc.message})


@app.exception_handler(MomaError)
async def moma_error_handler(request: Request, exc: MomaError) -> JSONResponse:
    logger.exception("Unexpected domain error")
    return JSONResponse(status_code=500, content={"detail": exc.message})


@app.exception_handler(Exception)
async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled exception")
    return JSONResponse(status_code=500, content={"detail": "An unexpected error occurred."})


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
