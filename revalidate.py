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

if response.ok:
    print(f"Revalidated successfully ({url})")
else:
    print(f"Failed: {response.status_code} {response.text}")
    sys.exit(1)
