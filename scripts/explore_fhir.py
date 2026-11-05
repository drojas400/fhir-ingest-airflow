"""Quick, read-only exploration of the public HAPI FHIR R4 server.

Prints server capabilities, resource volumes, paging links, a sample
resource structure and whether _lastUpdated filtering works.
Uses small page sizes to be gentle with a shared public server.
"""
import json
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

BASE = "https://hapi.fhir.org/baseR4"
HEADERS = {
    "Accept": "application/fhir+json",
    "User-Agent": "fhir-ingest-airflow/0.1 (portfolio project)",
}
RESOURCES = ["Patient", "Observation"]


def get(path, params=None):
    url = f"{BASE}/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)


def safe(label, fn):
    try:
        fn()
    except Exception as exc:  # exploration script: report and continue
        print(f"  [{label}] ERROR: {exc}")


def server_info():
    meta = get("metadata", {"_summary": "true"})
    sw = meta.get("software", {})
    print(f"FHIR version: {meta.get('fhirVersion')} | "
          f"software: {sw.get('name')} {sw.get('version')}")


def explore(rt):
    print(f"\n=== {rt} ===")

    def total():
        b = get(rt, {"_summary": "count"})
        print(f"  total: {b.get('total')}")

    def sample():
        b = get(rt, {"_count": 3, "_sort": "-_lastUpdated"})
        print(f"  bundle type: {b.get('type')}")
        print(f"  links: {[l['relation'] for l in b.get('link', [])]}")
        for e in b.get("entry", []):
            r = e["resource"]
            print(f"  - id={r.get('id')} lastUpdated={r.get('meta', {}).get('lastUpdated')} "
                  f"keys={sorted(r.keys())}")
        if b.get("entry"):
            first = json.dumps(b["entry"][0]["resource"], indent=2)
            print("  sample (truncated):")
            print("\n".join("    " + line for line in first[:1500].splitlines()))

    def incremental():
        since = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        b = get(rt, {"_lastUpdated": f"gt{since}", "_summary": "count"})
        print(f"  updated in last 24h (_lastUpdated=gt{since}): {b.get('total')}")

    safe("total", total)
    safe("sample", sample)
    safe("incremental", incremental)


if __name__ == "__main__":
    safe("metadata", server_info)
    for rt in RESOURCES:
        explore(rt)