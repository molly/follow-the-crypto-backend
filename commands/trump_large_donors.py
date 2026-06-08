#!/usr/bin/env python3
"""
One-time fetch of LARGE (>= $100k) contributions to Trump-aligned committees, for the
/analysis/trump page. Frozen, one-and-done: the 2024 cycle is closed and the Trump-Vance
Inaugural Committee filing is final, so the output never changes.

This does NOT try to guess which donors are "tech". It fetches every big contribution, dedupes
them into a per-donor list (name, employer(s), occupation(s), dates, amounts, recipient
committee), and writes a REVIEW file. You look through it and mark the tech-connected donors
(`techConnected: true`, optionally a `label`/`sector`); re-run with --from-review and it
regenerates the committed dataset from your marks.

Nothing is written to Firestore. Two JSON files are emitted into the frontend repo:
  - trumpLargeDonors.review.json  -- EVERY large donor, for you to mark. (source of truth)
  - trumpLargeDonors2024.json     -- the marked tech-connected subset, imported by the page.

Approach: query Schedule A *to* each committee with an API-side `min_amount` filter (only large
contributions come back -- no paging through millions of small ones). The inaugural committee
does not itemize in Schedule A; its donations live on Form 13, downloaded as the raw .fec filing.

Usage:
    python -m commands.trump_large_donors                 # fetch + (re)write review, rebuild final
    python -m commands.trump_large_donors --min-amount 50000
    python -m commands.trump_large_donors --from-review   # skip fetch; rebuild final from review marks
"""

import argparse
import json
import logging
import os
import re
from collections import defaultdict

from dotenv import load_dotenv
from requests_cache import CachedSession

from utils import FEC_fetch

load_dotenv()

# --- Recipient committees -----------------------------------------------------------------
# Trump leadership PACs / JFC / MAGA Inc super PAC + Musk's pro-Trump America PAC. The principal
# campaign committee is intentionally excluded: contributions there are capped at $3,300, so
# nothing clears the large-donor floor. The inaugural committee is handled separately (Form 13).
SCHEDULE_A_COMMITTEES = {
    "C00892471": "MAGA Inc.",
    "C00828541": "Never Surrender, Inc.",
    "C00762591": "Save America",
    "C00873893": "Trump National Committee JFC",
    "C00879510": "America PAC",  # Elon Musk's pro-Trump super PAC
    "C00770941": "Trump Save America Joint Fundraising Committee",
    "C00867937": "Trump 47 Committee, Inc.",
    "C00825851": "Make America Great Again Inc.",
}

INAUGURAL_COMMITTEE_ID = "C00894162"
INAUGURAL_COMMITTEE_NAME = "Trump Vance Inaugural Committee"
# Latest non-supplement "Post Inaugural 2025" report (full itemized donor schedule).
INAUGURAL_FEC_FILE_ID = "1910509"
INAUGURAL_FEC_URL = f"https://docquery.fec.gov/dcdev/posted/{INAUGURAL_FEC_FILE_ID}.fec"

COMMITTEE_NAMES = {**SCHEDULE_A_COMMITTEES, INAUGURAL_COMMITTEE_ID: INAUGURAL_COMMITTEE_NAME}

DEFAULT_MIN_AMOUNT = 100_000

# America PAC stopped being a pro-Trump vehicle after the 2024 election (Musk's break with Trump).
# Contributions to it dated after election day are excluded from the dataset -- they're Musk
# funding his own super PAC's later, non-Trump activity, not Trump support.
AMERICA_PAC_ID = "C00879510"
ELECTION_DATE = "2024-11-05"


def is_post_election_america_pac(committee_id, date):
    return committee_id == AMERICA_PAC_ID and bool(date) and date > ELECTION_DATE

FRONTEND_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "follow-the-crypto",
    "src",
    "app",
    "data",
)
FINAL_PATH = os.path.join(FRONTEND_DATA_DIR, "trumpLargeDonors2024.json")
REVIEW_PATH = os.path.join(FRONTEND_DATA_DIR, "trumpLargeDonors.review.json")


def donor_key(name):
    """Stable key for grouping a donor's contributions across records."""
    return re.sub(r"\s+", " ", (name or "").upper()).strip().strip(".,").strip()


# --- Schedule A fetch ---------------------------------------------------------------------
def fetch_committee_large_contributions(session, committee_id, committee_name, min_amount):
    """Page through Schedule A receipts >= min_amount for one committee.

    Drops memo rows (memo_code == 'X'): those are in-kind/earmark attribution memos already
    counted in their parent transaction (e.g. America PAC's "petition incentive" attributions
    to Musk that mirror the "United States of America Inc." line). Dedupes by transaction id.
    """
    records = []
    seen = set()
    last_index = None
    last_amount = None
    fetched = 0

    while True:
        data = FEC_fetch(
            session,
            f"large contributions to {committee_name}",
            "https://api.open.fec.gov/v1/schedules/schedule_a",
            params={
                "committee_id": committee_id,
                "min_amount": min_amount,
                "per_page": 100,
                "sort": "-contribution_receipt_amount",
                "last_index": last_index,
                "last_contribution_receipt_amount": last_amount,
            },
        )
        if not data:
            continue

        for c in data["results"]:
            tid = c.get("transaction_id")
            if tid and tid in seen:
                continue
            if tid:
                seen.add(tid)
            if c.get("memo_code") == "X":
                continue
            records.append(
                {
                    "name": c.get("contributor_name"),
                    "employer": c.get("contributor_employer"),
                    "occupation": c.get("contributor_occupation"),
                    "amount": c.get("contribution_receipt_amount"),
                    "date": c.get("contribution_receipt_date"),
                    "committee_id": committee_id,
                    "committee_name": committee_name,
                    "source": "schedule_a",
                    "note": None,
                }
            )

        fetched += data["pagination"]["per_page"]
        if fetched >= data["pagination"]["count"]:
            break
        last_index = data["pagination"]["last_indexes"]["last_index"]
        last_amount = data["pagination"]["last_indexes"]["last_contribution_receipt_amount"]

    logging.info("  %s: %d large contributions (after dedup)", committee_name, len(records))
    return records


# --- Inaugural Form 13 parse --------------------------------------------------------------
# Field offsets within an F132 ("Schedule of donations") row in the raw .fec filing.
F132_ENTITY = 5
F132_ORG_NAME = 6
F132_LAST = 7
F132_FIRST = 8
F132_DATE = 17
F132_AMOUNT = 18
F132_DESC = 21


def fetch_inaugural_large_donations(session, min_amount):
    """Download and parse the inaugural committee's Form 13 itemized donations >= min_amount.

    The raw .fec is \\x1c-delimited; rows are split on '\\n' only (Python's splitlines also
    breaks on \\x1c). Each F132 line is one donation; a donor may have several (cash + in-kind).
    """
    resp = session.get(INAUGURAL_FEC_URL, timeout=60)
    resp.raise_for_status()
    text = resp.content.decode("utf-8", errors="replace")

    records = []
    for line in text.split("\n"):
        if not line:
            continue
        f = line.split("\x1c")
        if not f or f[0] != "F132":
            continue

        amount = float(f[F132_AMOUNT]) if len(f) > F132_AMOUNT and f[F132_AMOUNT] else 0.0
        if abs(amount) < min_amount:
            continue

        entity = f[F132_ENTITY] if len(f) > F132_ENTITY else ""
        if entity == "ORG":
            name = f[F132_ORG_NAME].strip()
        else:
            last = f[F132_LAST].strip() if len(f) > F132_LAST else ""
            first = f[F132_FIRST].strip() if len(f) > F132_FIRST else ""
            name = f"{last}, {first}".strip(", ").strip()

        raw_date = f[F132_DATE] if len(f) > F132_DATE else ""
        date = (
            f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
            if len(raw_date) == 8
            else raw_date
        )
        desc = f[F132_DESC].strip() if len(f) > F132_DESC else ""

        records.append(
            {
                "name": name,
                "employer": None,  # F13 donors are the entity/person directly
                "occupation": None,
                "amount": amount,
                "date": date,
                "committee_id": INAUGURAL_COMMITTEE_ID,
                "committee_name": INAUGURAL_COMMITTEE_NAME,
                "source": "f13",
                "note": desc or None,  # e.g. "IN-KIND: DIGITAL SERVICES & ADVERTISING"
            }
        )

    logging.info("  %s: %d donations >= %s", INAUGURAL_COMMITTEE_NAME, len(records), min_amount)
    return records


# --- Aggregation --------------------------------------------------------------------------
def aggregate_donors(records):
    """Group raw contribution records into one entry per donor (by normalized name)."""
    grouped = defaultdict(list)
    for r in records:
        grouped[donor_key(r["name"])].append(r)

    donors = []
    for key, recs in grouped.items():
        # Prefer the longest name variant seen as the display name.
        display = max((r["name"] or "" for r in recs), key=len)
        employers = sorted({(r["employer"] or "").strip() for r in recs} - {""})
        occupations = sorted({(r["occupation"] or "").strip() for r in recs} - {""})
        contributions = sorted(
            (
                {
                    "amount": round(r["amount"], 2) if r["amount"] is not None else None,
                    "date": r["date"],
                    "committee_id": r["committee_id"],
                    "committee_name": r["committee_name"],
                    "source": r["source"],
                    "note": r["note"],
                }
                for r in recs
            ),
            key=lambda c: c["amount"] or 0,
            reverse=True,
        )
        donors.append(
            {
                "key": key,
                "name": display,
                "employers": employers,
                "occupations": occupations,
                "total": round(sum(r["amount"] or 0 for r in recs), 2),
                "num_contributions": len(recs),
                "contributions": contributions,
            }
        )

    donors.sort(key=lambda d: d["total"], reverse=True)
    return donors


# --- Review file (manual tagging) merge ---------------------------------------------------
def load_existing_marks():
    """Read prior review marks so re-fetching doesn't wipe manual tagging.

    Returns {key: {techConnected, tracked, label, sector}}.
    """
    if not os.path.exists(REVIEW_PATH):
        return {}
    try:
        with open(REVIEW_PATH) as fh:
            prev = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {}
    marks = {}
    for d in prev.get("donors", []):
        marks[d.get("key")] = {
            "reviewed": d.get("reviewed", False),
            "techConnected": d.get("techConnected", False),
            "id": d.get("id", ""),
            "label": d.get("label", ""),
            "sector": d.get("sector", ""),
        }
    return marks


def write_review(donors, min_amount):
    marks = load_existing_marks()
    marked = 0
    unreviewed = 0
    for d in donors:
        m = marks.get(d["key"], {})
        d["reviewed"] = m.get("reviewed", False) or m.get("techConnected", False)
        d["techConnected"] = m.get("techConnected", False)
        d["id"] = m.get("id", "")
        d["label"] = m.get("label", "")
        d["sector"] = m.get("sector", "")
        if d["techConnected"]:
            marked += 1
        if not d["reviewed"]:
            unreviewed += 1
    # Float not-yet-reviewed donors to the top so re-runs after adding committees only surface the
    # new work; within each group keep the largest donors first.
    donors.sort(key=lambda d: (d["reviewed"], -d["total"]))
    payload = {
        "min_amount": min_amount,
        "instructions": (
            "NEW/unreviewed donors are sorted to the TOP (reviewed=false). Go through those only; "
            "donors you've already cleared keep reviewed=true and stay at the bottom. After looking "
            "at a donor, set reviewed=true. Set techConnected=true for tech-connected donors (false "
            "drops noise / Trump-internal transfers / non-tech). For donors that are TRACKED (a "
            "company or individual with a page on the site), set `id` to that entity's id (e.g. "
            "crypto-com, elon-musk) -- the page uses it to link to their page AND to dedupe against "
            "the live tracker by id. Leave `id` blank for untracked donors (they get a 'not tracked' "
            "badge, no link). Set `label` (display name) and `sector` (crypto|ai|tech). Then: "
            "python -m commands.trump_large_donors --from-review"
        ),
        "donor_count": len(donors),
        "unreviewed_count": unreviewed,
        "donors": donors,
    }
    with open(REVIEW_PATH, "w") as fh:
        json.dump(payload, fh, indent=2)
    logging.info(
        "Wrote review file: %s (%d donors, %d marked tech, %d still unreviewed)",
        REVIEW_PATH,
        len(donors),
        marked,
        unreviewed,
    )


# --- Final dataset (committed) ------------------------------------------------------------
def build_final(min_amount):
    """Build the committed dataset from the tech-connected donors marked in the review file."""
    if not os.path.exists(REVIEW_PATH):
        logging.warning("No review file at %s; nothing to build.", REVIEW_PATH)
        return
    with open(REVIEW_PATH) as fh:
        review = json.load(fh)

    tech = [d for d in review.get("donors", []) if d.get("techConnected")]
    # Merge curated rows that share a tracked id (or, if untracked, a display label) into one donor
    # -- e.g. "MUSK, ELON" + "UNITED STATES OF AMERICA INC" both tagged id=elon-musk. NOTE: no
    # overlap dedup happens here. Whether a curated donor's 2025-26 Schedule A duplicates the live
    # tracker depends on the live data, which only the frontend has, so the frontend dedupes by id
    # at merge time. The `id` is passed through for both that dedup and for linking to the entity page.
    merged = {}
    for d in tech:
        donor_id = (d.get("id") or "").strip()
        label = (d.get("label") or "").strip() or d["name"]
        key = donor_id or label
        bucket = merged.setdefault(
            key,
            {
                "id": donor_id or None,
                "label": label,
                "sector": (d.get("sector") or "").strip() or None,
                "total": 0.0,
                "donor_names": [],
                "contributions": [],
            },
        )
        bucket["donor_names"].append(d["name"])
        for c in d["contributions"]:
            # Defensive: skip post-election America PAC even if a stale review file still lists it.
            # Recompute the bucket total from the contributions actually kept rather than trusting
            # the donor's stored aggregate total.
            if is_post_election_america_pac(c["committee_id"], c["date"]):
                continue
            bucket["contributions"].append({**c, "donor": d["name"]})
            bucket["total"] += c["amount"] or 0
        if not bucket["sector"] and d.get("sector"):
            bucket["sector"] = d["sector"].strip()

    donors = sorted(merged.values(), key=lambda x: x["total"], reverse=True)
    for d in donors:
        d["total"] = round(d["total"], 2)
        d["contributions"].sort(key=lambda c: c["amount"] or 0, reverse=True)

    final = {
        "min_amount": min_amount,
        "grand_total": round(sum(d["total"] for d in donors), 2),
        "donor_count": len(donors),
        "committee_names": COMMITTEE_NAMES,
        "donors": donors,
    }
    with open(FINAL_PATH, "w") as fh:
        json.dump(final, fh, indent=2)
    logging.info(
        "Wrote final dataset: %s (%d tech donors, $%s)",
        FINAL_PATH,
        len(donors),
        f"{final['grand_total']:,.0f}",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-amount", type=int, default=DEFAULT_MIN_AMOUNT)
    parser.add_argument(
        "--from-review",
        action="store_true",
        help="Skip fetching; rebuild the final dataset from review-file marks.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    if args.from_review:
        build_final(args.min_amount)
        return

    session = CachedSession("cache", backend="filesystem")
    records = []
    logging.info("Fetching Schedule A large contributions (>= $%s)...", args.min_amount)
    for cid, cname in SCHEDULE_A_COMMITTEES.items():
        records.extend(
            fetch_committee_large_contributions(session, cid, cname, args.min_amount)
        )
    logging.info("Parsing inaugural Form 13 donations...")
    records.extend(fetch_inaugural_large_donations(session, args.min_amount))

    before = len(records)
    records = [
        r for r in records if not is_post_election_america_pac(r["committee_id"], r["date"])
    ]
    dropped = before - len(records)
    if dropped:
        logging.info("Dropped %d post-election America PAC contributions (excluded by design)", dropped)
    logging.info("Total large contributions: %d", len(records))

    donors = aggregate_donors(records)
    logging.info("Distinct large donors: %d", len(donors))
    write_review(donors, args.min_amount)
    build_final(args.min_amount)


if __name__ == "__main__":
    main()
