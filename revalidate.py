#!/usr/bin/env python3
"""Revalidate the Next.js cache. Usage: python revalidate.py https://your-site.com"""

import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv()

site_url = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("SITE_URL")
if not site_url:
    print("Error: provide a site URL as an argument or set SITE_URL in .env")
    sys.exit(1)

secret = os.environ.get("REVALIDATE_SECRET")
if not secret:
    print("Error: REVALIDATE_SECRET not set in .env")
    sys.exit(1)

url = f"{site_url.rstrip('/')}/api/revalidate"
response = requests.post(url, headers={"x-revalidate-secret": secret})

# Revalidate Next's route cache first, then purge Fastly. Order matters:
# purging the edge before the origin is fresh just re-pulls stale HTML. If the
# Next call fails, fall through to the Fastly purge anyway so the site still
# recovers (via time-based ISR) rather than skipping invalidation entirely.
next_ok = response.ok
if next_ok:
    print(f"Revalidated successfully ({url})")
else:
    print(f"Next revalidate failed: {response.status_code} {response.text}")

fastly_ok = True
fastly_token = os.environ.get("FASTLY_TOKEN")
if not fastly_token:
    # Without the purge, the edge keeps serving stale HTML for up to s-maxage
    # (7 days, set by `export const revalidate` in the Next root layout). A
    # skipped purge is a hard failure, not a warning — otherwise revalidate
    # reports success while visitors keep seeing old data.
    fastly_ok = False
    print("Error: FASTLY_TOKEN not set in .env, cannot purge Fastly edge cache")
else:
    fastly_response = requests.post(
        "https://api.fastly.com/service/0lWAENYUEVE3yrULlZ9Jnu/purge_all",
        headers={"Fastly-Key": fastly_token, "Accept": "application/json"},
    )
    if fastly_response.ok:
        print("Fastly cache purged successfully")
    else:
        fastly_ok = False
        print(f"Fastly purge failed: {fastly_response.status_code} {fastly_response.text}")

if not (next_ok and fastly_ok):
    failed = [
        name
        for name, ok in (("Next revalidate", next_ok), ("Fastly purge", fastly_ok))
        if not ok
    ]
    print(f"\nRevalidation FAILED: {', '.join(failed)}. Edge may serve stale data.")
    sys.exit(1)
