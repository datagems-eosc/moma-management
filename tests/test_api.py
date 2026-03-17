"""
NOTE: THIS FILE IS GENERATED, DO NOT EDIT MANUALLY.
High-level REST API integration tests driven by tests/api/moma_api.postman_collection.json.

Each parametrised test case:
  1. Spins up a throwaway Neo4j container (shared for the whole test module).
  2. Starts a real FastAPI/uvicorn server on a random free port.
  3. Replays every request in the Postman collection in order, substituting
     environment variables ({{baseUrl}}, {{profileBody}}, {{rootId}}, …).
  4. Asserts each request's HTTP status matches the value declared in the
     Postman test script (``pm.response.to.have.status(NNN)``).

Non-standard keys embedded in the Postman collection items:
  ``_pytest_capture``  – rules for extracting env variables from responses.
  ``_pytest_assert``   – extra response-body assertions beyond the status check.
Both are silently ignored by Postman Desktop.

All tests carry the ``xdist_group`` marker so that pytest-xdist routes them to
the same worker, allowing the module-scoped server fixture to be shared.
"""

from __future__ import annotations

import json
import re
import socket
import threading
import time
from pathlib import Path
from typing import Any

import pytest
import requests as http_requests
import uvicorn
from testcontainers.neo4j import Neo4jContainer

COLLECTION_PATH = Path(__file__).parent / "api" / \
    "moma_api.postman_collection.json"
PROFILES_DIR = Path(__file__).parent.parent / "assets" / "profiles" / "light"

# Route every test in this module to the same xdist worker so the
# module-scoped API server fixture is reused rather than duplicated.
pytestmark = pytest.mark.xdist_group("api_integration")


# ---------------------------------------------------------------------------
# Infrastructure fixtures
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def api_server():
    """
    Start a throwaway Neo4j container and a real FastAPI/uvicorn server for the
    duration of this test module.  Yields the server's base URL string.

    Patches the module-level DI variables in ``moma_management.di`` so the
    server connects to the throwaway container rather than the default URI.
    """
    container = Neo4jContainer("neo4j:latest")
    container.start()

    # Patch module-level connection variables before the app's lifespan creates
    # the driver, so it connects to the throwaway container.
    import moma_management.di as di

    di.NEO4J_URI = container.get_connection_url()
    di.NEO4J_USER = container.username
    di.NEO4J_PASSWORD = container.password

    from moma_management.main import app

    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1",
                            port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            http_requests.get(f"{base_url}/api/v1/health", timeout=1)
            break
        except Exception:
            time.sleep(0.3)
    else:
        server.should_exit = True
        container.stop()
        raise RuntimeError(
            "API server did not become healthy within 30 seconds")

    yield base_url

    server.should_exit = True
    thread.join(timeout=5)
    container.stop()


@pytest.fixture(scope="module")
def postman_collection() -> dict:
    return json.loads(COLLECTION_PATH.read_text())


# ---------------------------------------------------------------------------
# Postman collection runner
# ---------------------------------------------------------------------------


def _parse_expected_status(events: list[dict]) -> int:
    """Extract expected HTTP status from ``pm.response.to.have.status(NNN)``."""
    for event in (events or []):
        if event.get("listen") == "test":
            for line in event.get("script", {}).get("exec", []):
                m = re.search(r"pm\.response\.to\.have\.status\((\d+)\)", line)
                if m:
                    return int(m.group(1))
    return 200


def _resolve_url(url_obj: Any, sub) -> str:
    if isinstance(url_obj, str):
        return sub(url_obj)
    if isinstance(url_obj, dict):
        return sub(url_obj.get("raw", ""))
    return ""


class PostmanRunner:
    """
    Minimal Postman v2.1 collection runner for pytest.

    Supports:
    - ``{{variable}}`` substitution throughout URLs, headers, and bodies.
    - Nested item folders (depth-first, in order).
    - Status-code assertions from Postman test-script ``exec`` strings.
    - ``_pytest_capture`` rules for extracting values from responses into env vars.
    - ``_pytest_assert`` rules for simple response-body assertions.
    """

    def __init__(self, base_url: str, initial_env: dict[str, str] | None = None):
        self.env: dict[str, str] = {"baseUrl": base_url, **(initial_env or {})}

    # -- variable resolution -------------------------------------------------

    def _sub(self, text: str) -> str:
        for k, v in self.env.items():
            if isinstance(v, str):
                text = text.replace(f"{{{{{k}}}}}", v)
        return text

    def _resolve_body(self, body_cfg: dict) -> Any:
        if not body_cfg or body_cfg.get("mode") != "raw":
            return None
        raw = self._sub(body_cfg.get("raw", ""))
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    # -- capture & assertions ------------------------------------------------

    def _apply_captures(
        self, rules: list[dict], response: http_requests.Response
    ) -> None:
        """
        Post-request variable captures (``_pytest_capture``).

        Supported ``from_response`` strategies:
        - ``"root_dataset_id"`` – id of the first node whose labels include ``sc:Dataset``.
        - ``"entire_body"``     – raw response text (useful as the body of the next request).
        """
        for rule in rules or []:
            var = rule.get("var", "")
            strategy = rule.get("from_response", "")
            try:
                if strategy == "root_dataset_id":
                    node = next(
                        n
                        for n in response.json().get("nodes", [])
                        if "sc:Dataset" in n.get("labels", [])
                    )
                    self.env[var] = node["id"]
                elif strategy == "entire_body":
                    self.env[var] = response.text
            except Exception:
                pass  # failures will surface as downstream assertion errors

    def _check_assertions(
        self, rules: list[dict], response: http_requests.Response
    ) -> list[str]:
        """
        Extra body assertions (``_pytest_assert``).

        Supported ``op`` values:
        - ``"non_empty_list"`` – the key's value is a non-empty list.
        - ``"gte"``            – the key's value >= ``value``.
        - ``"gt"``             – the key's value >  ``value``.
        """
        failures: list[str] = []
        try:
            data = response.json()
        except Exception:
            data = {}
        for rule in rules or []:
            key = rule.get("key", "")
            op = rule.get("op", "")
            expected = rule.get("value")
            actual = data.get(key)
            if op == "non_empty_list":
                if not (isinstance(actual, list) and len(actual) > 0):
                    failures.append(
                        f"'{key}' is not a non-empty list (got {actual!r})")
            elif op == "gte":
                if not (actual is not None and actual >= expected):
                    failures.append(
                        f"'{key}': expected >= {expected}, got {actual!r}")
            elif op == "gt":
                if not (actual is not None and actual > expected):
                    failures.append(
                        f"'{key}': expected > {expected}, got {actual!r}")
        return failures

    # -- request execution ---------------------------------------------------

    def run_item(
        self, item: dict, session: http_requests.Session
    ) -> tuple[str, bool, str]:
        """
        Execute one Postman request item.

        Returns ``(name, passed, detail)`` where ``detail`` is non-empty only
        on failure.
        """
        name = item.get("name", "unnamed")
        req = item.get("request", {})
        method = req.get("method", "GET")
        url = _resolve_url(req.get("url", ""), self._sub)
        headers = {h["key"]: self._sub(h["value"])
                   for h in req.get("header", [])}
        body = self._resolve_body(req.get("body", {}))
        expected_status = _parse_expected_status(item.get("event", []))

        try:
            resp = session.request(
                method, url, json=body, headers=headers, timeout=30)
        except Exception as exc:
            return name, False, f"Request failed: {exc}"

        self._apply_captures(item.get("_pytest_capture", []), resp)

        issues: list[str] = []
        if resp.status_code != expected_status:
            issues.append(
                f"HTTP {resp.status_code} (expected {expected_status})")
        issues.extend(self._check_assertions(
            item.get("_pytest_assert", []), resp))

        return name, not issues, "; ".join(issues)

    # -- collection traversal ------------------------------------------------

    def run_collection(
        self, collection: dict, session: http_requests.Session
    ) -> list[tuple[str, bool, str]]:
        """Recursively run all items in the collection, top-to-bottom."""
        results: list[tuple[str, bool, str]] = []
        for item in collection.get("item", []):
            if "item" in item:
                results.extend(self._run_folder(item, session))
            else:
                results.append(self.run_item(item, session))
        return results

    def _run_folder(
        self, folder: dict, session: http_requests.Session
    ) -> list[tuple[str, bool, str]]:
        results: list[tuple[str, bool, str]] = []
        for item in folder.get("item", []):
            if "item" in item:
                results.extend(self._run_folder(item, session))
            else:
                results.append(self.run_item(item, session))
        return results


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _light_profile_paths() -> list[Path]:
    return sorted(PROFILES_DIR.glob("*.json"))


@pytest.mark.parametrize(
    "profile_path",
    _light_profile_paths(),
    ids=lambda p: p.stem,
)
def test_dataset_lifecycle(
    api_server: str,
    postman_collection: dict,
    profile_path: Path,
):
    """
    Drive the full dataset lifecycle using one light Croissant profile:

      Health Check → Convert → Validate → Ingest → List → Get → Delete → 404

    Each step is defined as a request item in the Postman collection.
    Variable values captured from one response (e.g. ``rootId``) are
    automatically substituted into subsequent requests.
    """
    runner = PostmanRunner(
        base_url=api_server,
        initial_env={"profileBody": profile_path.read_text()},
    )
    with http_requests.Session() as session:
        results = runner.run_collection(postman_collection, session)

    failures = [
        f"  [{name}] {detail}"
        for name, passed, detail in results
        if not passed
    ]
    assert not failures, "Postman collection failures:\n" + "\n".join(failures)


# ---------------------------------------------------------------------------
# List endpoint tests  (GH#15 regression: pagination consistency)
# ---------------------------------------------------------------------------


def _paginate_all(
    base_url: str,
    session: http_requests.Session,
    page_size: int = 1,
    **filter_params,
) -> tuple[list, int]:
    """
    Fetch every page of GET /datasets with the given filter parameters.

    Returns ``(all_datasets, reported_total)`` where ``all_datasets`` is the
    flat list of dataset dicts aggregated across all pages.  Asserts that
    ``total`` is identical on every page so that a changing total is caught
    immediately rather than only at the end.
    """
    datasets: list = []
    total: int | None = None
    page = 1
    while True:
        resp = session.get(
            f"{base_url}/api/v1/datasets",
            params={"page": page, "pageSize": page_size, **filter_params},
            timeout=30,
        )
        assert resp.status_code == 200, (
            f"GET /datasets page {page} failed ({resp.status_code}): {resp.text}"
        )
        data = resp.json()
        if total is None:
            total = data["total"]
        else:
            assert data["total"] == total, (
                f"total changed between pages: was {total}, now {data['total']} "
                f"(page={page}, pageSize={page_size}, filters={filter_params})"
            )
        page_datasets = data["datasets"]
        datasets.extend(page_datasets)
        if not page_datasets:
            break
        page += 1
    return datasets, total or 0


def _root_ids_from(datasets: list) -> list[str]:
    """Extract the ``sc:Dataset`` root-node IDs from a list of dataset dicts."""
    return [
        n["id"]
        for ds in datasets
        for n in ds.get("nodes", [])
        if "sc:Dataset" in n.get("labels", [])
    ]


class TestListEndpoint:
    """
    Regression tests for GET /datasets – pagination and filter consistency.

    GH#15 describes a bug where ``total`` from the COUNT query did not match
    the number of datasets actually returned when combining a ``mimeTypes``
    filter with pagination (total=13 reported, only 11 or 8 returned).

    Seed: all three light profiles are ingested once for the class and cleaned
    up afterwards via the ``ingested_datasets`` class-scoped fixture.
    """

    @pytest.fixture(scope="class")
    def ingested_datasets(self, api_server: str):
        """Ingest all light profiles, yield (base_url, root_ids), then delete them."""
        root_ids: list[str] = []
        with http_requests.Session() as session:
            for profile_path in sorted(PROFILES_DIR.glob("*.json")):
                resp = session.post(
                    f"{api_server}/api/v1/datasets/croissant",
                    json=json.loads(profile_path.read_text()),
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                assert resp.status_code == 200, (
                    f"Setup: failed to ingest {profile_path.name}: {resp.text}"
                )
                root_node = next(
                    n
                    for n in resp.json()["nodes"]
                    if "sc:Dataset" in n["labels"]
                )
                root_ids.append(root_node["id"])
        yield api_server, root_ids
        with http_requests.Session() as session:
            for root_id in root_ids:
                session.delete(
                    f"{api_server}/api/v1/datasets/{root_id}", timeout=30)

    # -- GH#15 core: total == actual count when paginating with pageSize=1 --

    def test_total_matches_paginated_count(self, ingested_datasets):
        """
        Paginate through ALL datasets one-at-a-time and verify that the number
        of datasets received equals the ``total`` field.

        This is the exact regression for GH#15: total=13 reported but only 11
        (or 8 with count=20) datasets were actually returned.
        """
        base_url, _ = ingested_datasets
        with http_requests.Session() as session:
            datasets, total = _paginate_all(base_url, session, page_size=1)
        assert len(datasets) == total, (
            f"GH#15 regression: total={total} but received {len(datasets)} "
            "datasets across all pages with pageSize=1"
        )

    def test_no_duplicates_across_pages(self, ingested_datasets):
        """No dataset ID should appear on more than one page."""
        base_url, _ = ingested_datasets
        with http_requests.Session() as session:
            datasets, _ = _paginate_all(base_url, session, page_size=1)
        ids = _root_ids_from(datasets)
        duplicates = [x for x in ids if ids.count(x) > 1]
        assert len(ids) == len(set(ids)), (
            f"Duplicate dataset IDs across pages: {list(set(duplicates))}"
        )

    def test_total_stable_across_page_sizes(self, ingested_datasets):
        """
        ``total`` must be the same regardless of the page size used.

        GH#15 variant: with count=20 the old implementation returned a
        different total than with the default page size.
        """
        base_url, _ = ingested_datasets
        with http_requests.Session() as session:
            t1 = session.get(
                f"{base_url}/api/v1/datasets", params={"pageSize": 1}, timeout=30
            ).json()["total"]
            t5 = session.get(
                f"{base_url}/api/v1/datasets", params={"pageSize": 5}, timeout=30
            ).json()["total"]
            t100 = session.get(
                f"{base_url}/api/v1/datasets", params={"pageSize": 100}, timeout=30
            ).json()["total"]
        assert t1 == t5 == t100, (
            f"total differs across page sizes: "
            f"pageSize=1→{t1}, pageSize=5→{t5}, pageSize=100→{t100}"
        )

    def test_page_sum_equals_total(self, ingested_datasets):
        """Sum of datasets across all pages with pageSize=2 must equal total."""
        base_url, _ = ingested_datasets
        with http_requests.Session() as session:
            datasets, total = _paginate_all(base_url, session, page_size=2)
        assert len(datasets) == total, (
            f"Sum of paginated datasets ({len(datasets)}) != total ({total})"
        )

    def test_known_ids_all_appear_in_listing(self, ingested_datasets):
        """Every ingested root ID must appear in the unfiltered list results."""
        base_url, root_ids = ingested_datasets
        with http_requests.Session() as session:
            datasets, _ = _paginate_all(base_url, session, page_size=100)
        found = set(_root_ids_from(datasets))
        missing = [rid for rid in root_ids if rid not in found]
        assert not missing, (
            f"Ingested datasets missing from list results: {missing}"
        )

    def test_page_beyond_total_returns_empty_with_correct_total(
        self, ingested_datasets
    ):
        """An out-of-range page returns an empty list but the correct total."""
        base_url, _ = ingested_datasets
        with http_requests.Session() as session:
            first = session.get(
                f"{base_url}/api/v1/datasets",
                params={"pageSize": 100, "page": 1},
                timeout=30,
            ).json()
            beyond = session.get(
                f"{base_url}/api/v1/datasets",
                params={"pageSize": 100, "page": 9999},
                timeout=30,
            ).json()
        assert beyond["datasets"] == [
        ], "Expected empty list for out-of-range page"
        assert beyond["total"] == first["total"], (
            f"total changed on out-of-range page: {first['total']} → {beyond['total']}"
        )

    def test_nodeids_filter_total_consistent(self, ingested_datasets):
        """Filtering by nodeIds returns exactly those datasets; total matches."""
        base_url, root_ids = ingested_datasets
        target = root_ids[:2] if len(root_ids) >= 2 else root_ids
        with http_requests.Session() as session:
            datasets, total = _paginate_all(
                base_url, session, page_size=1, nodeIds=target
            )
        assert total == len(target), (
            f"nodeIds filter: expected total={len(target)}, got {total}"
        )
        assert len(datasets) == total

    def test_mimetype_filter_total_matches_paginated_count(self, ingested_datasets):
        """
        GH#15 core with mimeType filter: paginating with pageSize=1 must yield
        exactly ``total`` datasets for every supported mimeType value.

        This directly replicates the reported scenario:
          curl …/datasets?mimeTypes=text/csv → total=13 but 11 returned
        """
        base_url, _ = ingested_datasets
        for mime in ["text/csv", "text/sql", "application/pdf"]:
            with http_requests.Session() as session:
                datasets, total = _paginate_all(
                    base_url, session, page_size=1, mimeTypes=mime
                )
            assert len(datasets) == total, (
                f"GH#15 regression for mimeTypes={mime!r}: "
                f"total={total} but received {len(datasets)} datasets "
                "via pageSize=1 pagination"
            )
