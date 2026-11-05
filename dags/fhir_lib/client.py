"""FHIR R4 search client: retrying HTTP session + Bundle 'next' paging."""
from __future__ import annotations

import logging
from collections.abc import Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://hapi.fhir.org/baseR4"
HEADERS = {
    "Accept": "application/fhir+json",
    "User-Agent": "fhir-ingest-airflow/0.1 (portfolio project; github.com/drojas400)",
}
RETRY_STATUSES = (429, 500, 502, 503, 504)


def build_session(total_retries: int = 5, backoff_factor: float = 2.0) -> requests.Session:
    """Session that retries transient failures with exponential backoff.

    Retries connection errors and 429/5xx on GET, honouring Retry-After.
    After the last attempt the response is returned and raise_for_status()
    turns it into a task failure, where Airflow's task-level retries apply.
    """
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=RETRY_STATUSES,
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    session.headers.update(HEADERS)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def iter_search_pages(
    resource_type: str,
    params: dict,
    *,
    base_url: str = DEFAULT_BASE_URL,
    max_pages: int = 20,
    timeout: tuple[int, int] = (10, 60),
    session: requests.Session | None = None,
) -> Iterator[tuple[int, list[dict], bool]]:
    """Yield (page_number, resources, has_more) for each Bundle page.

    Only entries whose resourceType matches are returned (HAPI may include
    OperationOutcome entries). Stops after max_pages to protect the server.
    """
    session = session or build_session()
    url: str | None = f"{base_url}/{resource_type}"
    query: dict | None = params
    page = 0
    while url and page < max_pages:
        resp = session.get(url, params=query, timeout=timeout)
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
