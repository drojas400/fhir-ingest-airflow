"""Minimal FHIR R4 search client that follows Bundle 'next' links."""
from __future__ import annotations

import logging
from collections.abc import Iterator

import requests

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://hapi.fhir.org/baseR4"
HEADERS = {
    "Accept": "application/fhir+json",
    "User-Agent": "fhir-ingest-airflow/0.1 (portfolio project; github.com/drojas400)",
}


def iter_search_pages(
    resource_type: str,
    params: dict,
    *,
    base_url: str = DEFAULT_BASE_URL,
    max_pages: int = 20,
    timeout: int = 60,
    session: requests.Session | None = None,
) -> Iterator[tuple[int, list[dict], bool]]:
    """Yield (page_number, resources, has_more) for each Bundle page.

    Only entries whose resourceType matches are returned (HAPI may include
    OperationOutcome entries). Stops after max_pages to protect the server.
    """
    session = session or requests.Session()
    url: str | None = f"{base_url}/{resource_type}"
    query: dict | None = params
    page = 0
    while url and page < max_pages:
        resp = session.get(url, params=query, headers=HEADERS, timeout=timeout)
        resp.raise_for_status()
        bundle = resp.json()
        page += 1
        resources = [
            e["resource"]
            for e in bundle.get("entry", [])
            if e.get("resource", {}).get("resourceType") == resource_type
        ]
        url = next(
            (l["url"] for l in bundle.get("link", []) if l.get("relation") == "next"),
            None,
        )
        query = None  # 'next' URLs already carry the paging token
        log.info("%s page %d: %d resources (more=%s)", resource_type, page, len(resources), bool(url))
        yield page, resources, url is not None
