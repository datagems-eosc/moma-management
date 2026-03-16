async def health_check() -> dict:
    """Return the current liveness status of the service."""
    return {"status": "healthy"}
