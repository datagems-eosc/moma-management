#!/usr/bin/env python3
"""Send n concurrent POST requests to the /api/v1/datasets/convert endpoint."""

import argparse
import asyncio
import json
import time
from pathlib import Path

import aiohttp


async def send_request(
    session: aiohttp.ClientSession,
    url: str,
    payload: dict,
    index: int,
) -> dict:
    start = time.perf_counter()
    try:
        async with session.post(url, json=payload, headers={"Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJyajRqaXViektlZEw0b1d2Q3ZLZXJUM2Jub2V4QTQ5SVVuN0I3OVg4TV9ZIn0.eyJleHAiOjE3NzYxNzM1NjYsImlhdCI6MTc3NjE3MzI2NiwianRpIjoib25ydHJvOmI1ZTUwOTQ0LTU1ZGMtNDBkZC1iYmZmLTMzNDcyYzkzYzljZCIsImlzcyI6Imh0dHBzOi8vZGF0YWdlbXMtZGV2LnNjYXlsZS5lcy9vYXV0aC9yZWFsbXMvZGV2IiwiYXVkIjpbImluLWRhdGEtZXhwbG9yYXRpb24iLCJkYXRhLW1vZGVsLW1hbmFnZW1lbnQtYXBpIiwiZGF0YXNldC1wYWNrYWdpbmciLCJjcm9zcy1kYXRhc2V0LWRpc2NvdmVyeS1hcGkiLCJtb21hLW1hbmFnZW1lbnQtYXBpIiwiYWlyZmxvdyIsInF1ZXJ5LXJlY29tbWVuZGVyIiwiaW4tZGF0YXNldC1kaXNjb3ZlcnkiLCJkYXRhc2V0LXByb2ZpbGVyIiwiZGctYXBwLWFwaSIsImFjY291bnRpbmdfd2ViYXBwIiwiYWNjb3VudCJdLCJzdWIiOiIxNmE1MzgwZi00Nzg3LTQ0ZGQtYTU1Mi1jNzA5NWYyZDEyZDEiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJzd2FnZ2VyLWNsaWVudCIsInNpZCI6IjI1YTUwYmY4LWNkMDctNGI2Zi1iM2YwLThjMjkxMWI3YTFhZCIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsiaHR0cDovL2xvY2FsaG9zdDo1MDAwMCIsImh0dHBzOi8vZGF0YWdlbXMtZGV2LnNjYXlsZS5lcyJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsiZGVmYXVsdC1yb2xlcy1kZXYiLCJkZ191c2VyIiwib2ZmbGluZV9hY2Nlc3MiLCJkZ19hZG1pbiIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiYWNjb3VudGluZ193ZWJhcHAiOnsicm9sZXMiOlsiYWRtaW4iLCJ1c2VyIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6InByb2ZpbGUgZW1haWwgZGF0YWdlbXMiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwibmFtZSI6ImRnIHVzZXItYWRtaW4iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJkZy11c2VyLWFkbWluIiwiZ2l2ZW5fbmFtZSI6ImRnIiwiZmFtaWx5X25hbWUiOiJ1c2VyLWFkbWluIiwiZW1haWwiOiJkZy11c2VyLWFkbWluQGRhdGFnZW1zLmV1In0.yObtF7SGguCtblgePHkc2CF15WvHVfouWY637r-OQLgSwFln7i-DpZQw1Ld21zx8srDpLp40Pp8_RnT78t__Z5OvdEdXLO0PabBulqu--3zfDiVIYt5EFKLasVEP7d-zXR1g6gjWbDLaltsWYCOXf8szZMlu-ZHk5Rz3I7M1mU_kJv7VQgbhdYPxxqEFGfTLxLzuTeqiOfqpFlgG0kQ6z1aq9m5oDo9dk0mZXYaRZoeCdar6gWS9UbGw9ccnWfXdgVkVolKlCBfsYV_q7m3OPXtfxqyQY62Jtcdokxy9ZGpuxVCwjtQEW15Kx_ASbJ9rcE81_Z_lD4FDmIimZiwdaA"}) as resp:
            elapsed = time.perf_counter() - start
            body = await resp.text()
            return {
                "index": index,
                "status": resp.status,
                "elapsed_s": round(elapsed, 3),
                "ok": resp.status == 200,
                "error": None if resp.status == 200 else body[:200],
            }
    except Exception as exc:
        elapsed = time.perf_counter() - start
        return {
            "index": index,
            "status": None,
            "elapsed_s": round(elapsed, 3),
            "ok": False,
            "error": str(exc),
        }


async def run(n: int, url: str, payload: dict) -> None:
    print(f"Sending {n} concurrent requests to {url} ...")
    wall_start = time.perf_counter()

    async with aiohttp.ClientSession() as session:
        tasks = [send_request(session, url, payload, i) for i in range(n)]
        results = await asyncio.gather(*tasks)

    wall_elapsed = time.perf_counter() - wall_start

    successes = [r for r in results if r["ok"]]
    failures = [r for r in results if not r["ok"]]
    times = [r["elapsed_s"] for r in results]

    print(f"\n{'='*50}")
    print(f"Total requests : {n}")
    print(f"Successes      : {len(successes)}")
    print(f"Failures       : {len(failures)}")
    print(f"Wall time      : {wall_elapsed:.3f}s")
    if times:
        print(f"Min latency    : {min(times):.3f}s")
        print(f"Max latency    : {max(times):.3f}s")
        print(f"Avg latency    : {sum(times)/len(times):.3f}s")
    print(f"{'='*50}")

    if failures:
        print("\nFailed requests:")
        for r in failures:
            print(f"  [{r['index']}] status={r['status']} error={r['error']}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Concurrent requests to the MoMa /convert endpoint"
    )
    parser.add_argument(
        "-n",
        "--count",
        type=int,
        default=10,
        help="Number of concurrent requests (default: 10)",
    )
    parser.add_argument(
        "--host",
        default="http://localhost:5000",
        help="Base URL of the server (default: http://localhost:5000)",
    )
    parser.add_argument(
        "--payload",
        default="/workspaces/moma-management/assets/profiles/heavy/esco_heavy.json",
        help="Path to the JSON payload file",
    )
    args = parser.parse_args()

    payload_path = Path(args.payload)
    if not payload_path.is_file():
        raise SystemExit(f"Payload file not found: {payload_path}")

    payload = json.loads(payload_path.read_text())
    url = f"https://datagems-dev.scayle.es/moma2/v1/api/datasets/convert"

    asyncio.run(run(args.count, url, payload))


if __name__ == "__main__":
    main()
