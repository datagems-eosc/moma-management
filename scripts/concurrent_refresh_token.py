#!/usr/bin/env python3
"""Send n concurrent GET requests to the Keycloak JWKS certs endpoint."""

import argparse
import asyncio
import time

import aiohttp


async def send_request(
    session: aiohttp.ClientSession,
    url: str,
    index: int,
) -> dict:
    start = time.perf_counter()
    try:
        async with session.get(url) as resp:
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


async def run(n: int, url: str) -> None:
    print(f"Sending {n} concurrent requests to {url} ...")
    wall_start = time.perf_counter()

    async with aiohttp.ClientSession() as session:
        tasks = [send_request(session, url, i) for i in range(n)]
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
        description="Concurrent GET requests to the Keycloak JWKS certs endpoint"
    )
    parser.add_argument(
        "-n",
        "--count",
        type=int,
        default=10,
        help="Number of concurrent requests (default: 10)",
    )
    parser.add_argument(
        "--issuer",
        default="https://datagems-dev.scayle.es/oauth/realms/dev",
        help="Keycloak realm issuer URL",
    )
    args = parser.parse_args()

    url = f"{args.issuer.rstrip('/')}/protocol/openid-connect/certs"

    asyncio.run(run(args.count, url))


if __name__ == "__main__":
    main()
