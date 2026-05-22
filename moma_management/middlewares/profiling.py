from fastapi import Request
from fastapi.responses import HTMLResponse
from pyinstrument import Profiler


async def profile_request(request: Request, call_next):
    if not request.url.path.startswith("/api/v1"):
        return await call_next(request)

    profiler = Profiler()
    profiler.start()
    await call_next(request)
    profiler.stop()
    return HTMLResponse(profiler.output_html())
