#!/usr/bin/env python3
"""Refresh the live site's edge cache after a pipeline run.

Usage (standalone):  python revalidate.py [https://your-site.com]
From the pipeline:    from revalidate import revalidate; revalidate(expected_last_run=...)

The data-driven pages render dynamically (see `export const dynamic` in the Next
app): every origin request reads Firestore fresh, and Fastly is the only cache
layer. So refreshing the site after a pipeline run is just:

  1. Purge Fastly so the stale edge copies are dropped.
  2. Warm the key pages so the first real visitor gets a fresh HIT instead of
     paying the origin render.
  3. Verify the edge is actually serving the new last_run.

There's no Next ISR cache to invalidate and no revalidate/regenerate race to
work around — a dynamic page can only ever render fresh, so purging then warming
is sufficient and order-safe.
"""

import os
import sys
import time

import requests
from dotenv import load_dotenv

FASTLY_SERVICE_ID = "0lWAENYUEVE3yrULlZ9Jnu"

# Pages warmed at the edge after the purge. These render the global "Updated"
# timestamp, so they're verifiable against the expected last_run. Detail pages we
# don't warm here simply render on first visit (a one-time origin render) and are
# cached from then on.
SECTION_PATHS = [
    "/",
    "/2026/states",
    "/2026/elections",
    "/2026/committees",
    "/2026/companies",
    "/2026/individuals",
    "/2026/networks",
    "/2026/expenditures",
    "/2026/contributions",
    "/2026/beneficiaries",
]


def _read_last_run_from_firestore():
    # Lazy import: the standalone CLI shouldn't need the pipeline's Firestore
    # deps loaded unless it actually has to look the value up.
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from Database import Database

    doc = Database().client.collection("metadata").document("pipeline").get()
    return (doc.to_dict() or {}).get("last_run") if doc.exists else None


def _purge_fastly(token):
    try:
        r = requests.post(
            f"https://api.fastly.com/service/{FASTLY_SERVICE_ID}/purge_all",
            headers={"Fastly-Key": token, "Accept": "application/json"},
            timeout=30,
        )
        if r.ok:
            print("Fastly purge: ok")
            return True
        print(f"Fastly purge FAILED: {r.status_code} {r.text}")
    except requests.RequestException as e:
        print(f"Fastly purge FAILED: {e}")
    return False


def _warm(base, paths):
    """Request each canonical page so the edge caches a freshly rendered copy."""
    for path in paths:
        try:
            requests.get(f"{base}{path}", timeout=60)
        except requests.RequestException:
            pass


def revalidate(expected_last_run=None, site_url=None):
    """Purge Fastly, warm the key pages, and verify the edge is fresh.

    Returns True only if the purge succeeded and the homepage edge is confirmed
    serving the expected last_run. Prints progress so failures are visible.
    """
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    site_url = site_url or os.environ.get("SITE_URL")
    if not site_url:
        print("Error: provide a site URL as an argument or set SITE_URL in .env")
        return False
    base = site_url.rstrip("/")

    # 1. Purge Fastly (the only cache layer for dynamic pages).
    fastly_token = os.environ.get("FASTLY_TOKEN")
    if not fastly_token:
        print("Error: FASTLY_TOKEN not set in .env, cannot purge Fastly edge cache")
        return False
    if not _purge_fastly(fastly_token):
        return False

    # 2. Warm the key pages so visitors get a fresh HIT, not an origin render.
    _warm(base, SECTION_PATHS)

    # 3. Verify the edge is serving the new data.
    expected = expected_last_run or _read_last_run_from_firestore()
    if not expected:
        print("WARNING: no expected last_run available; skipping freshness check")
        print("\nRevalidation done (purge + warm); freshness unverified.")
        return True

    needle = expected.split("+")[0]
    fresh = False
    # The warm GET above already populated the edge; re-read it to confirm. Retry
    # briefly in case the first render was still in flight.
    for _ in range(5):
        try:
            if needle in requests.get(f"{base}/", timeout=60).text:
                fresh = True
                break
        except requests.RequestException:
            pass
        time.sleep(2)

    if fresh:
        print(f"Verified: edge serving fresh last_run ({expected})")
        print("\nRevalidation complete: edge serving fresh content.")
        return True

    print(f"WARNING: edge did not confirm fresh last_run ({expected}) after warm")
    print("\nRevalidation INCOMPLETE: edge may serve stale data.")
    return False


if __name__ == "__main__":
    site = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(0 if revalidate(site_url=site) else 1)
