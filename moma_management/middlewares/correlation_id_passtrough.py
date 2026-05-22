from uuid import uuid4

import structlog
from fastapi import Request

logger = structlog.get_logger(__name__)


async def correlation_id_passtrough(request: Request, call_next):
    """
    This middleware echoes the correlation ID from the request headers in the response.
    If the incoming request doesn't have a correlation ID, a new one is generated.
    """
    correlation_id = request.headers.get(
        "x-tracking-correlation") or str(uuid4())

    # The correlation_id is required for logs, we can set it in the contextvars for this request here.
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(DGCorrelationId=correlation_id)

    response = await call_next(request)
    response.headers["x-tracking-correlation"] = correlation_id
    return response
