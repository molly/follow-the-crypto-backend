#!/usr/bin/env python3
"""
Command to refetch and reprocess contribution data for a single existing committee,
without running the full pipeline over every committee.

Usage:
    python -m commands.fetch_committee --id "C00949487"
    python -m commands.fetch_committee --id "C00949487" --incremental

By default this does a FULL refetch (full=True), which rebuilds the committee's pre-aggregated
records from scratch. That's what you want when reconciling bad data (e.g. individuals lumped
under a non-employer "employer"), since an incremental run skips already-known transaction IDs
and preserves stale aggregates. Pass --incremental for a routine early-stop refresh instead.

Note: this refreshes rawContributions/<id> and contributions/<id> only. Cross-committee
summaries (summarize_pacs, summarize_committee_transfers_by_party) are NOT updated — run
those pipeline tasks if the change needs to flow into the party/PAC rollups.
"""

import argparse
import logging
import requests
from Database import Database
from fetch_committee_contributions import update_committee_contributions
from process_committee_contributions import process_committee_contributions


def fetch_committee_data(committee_id: str, full: bool = True):
    """
    Refetch and reprocess contribution data for a single committee.

    Args:
        committee_id: The FEC committee ID to refetch (must exist in constants.committees).
        full: Full re-fetch/overwrite (default) vs. incremental early-stop refresh.

    Returns:
        dict: Summary of the operation.
    """
    db = Database()
    db.get_constants()

    match = [c for c in db.committees.values() if c["id"] == committee_id]
    if not match:
        raise ValueError(f"Committee '{committee_id}' not found in constants")

    # Scope the fetch to just this committee by swapping db.committees, mirroring the
    # per-individual pattern in commands/fetch_individual.py, then restore it.
    original_committees = db.committees
    db.committees = {committee_id: match[0]}

    session = requests.Session()
    try:
        logging.info(
            f"Fetching contributions for {committee_id} (full={full})"
        )
        new_contributions = update_committee_contributions(db, session, full=full)
    finally:
        db.committees = original_committees

    # Reprocess only this committee's rawContributions doc.
    logging.info(f"Processing contributions for {committee_id}")
    process_committee_contributions(db, committee_id=committee_id)

    return {
        "committee_id": committee_id,
        "full": full,
        "new_contributions_count": len(new_contributions),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Refetch and reprocess contributions for a single committee"
    )
    parser.add_argument("--id", required=True, help="FEC committee ID to refetch")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Incremental early-stop refresh instead of the default full re-fetch",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    try:
        result = fetch_committee_data(args.id, full=not args.incremental)
        mode = "incremental" if args.incremental else "full"
        print(f"✅ Refetched ({mode}) and reprocessed '{args.id}'")
        print(f"📊 {result['new_contributions_count']} new contributions")
    except Exception as e:
        print(f"❌ Error refetching committee data: {e}")
        raise


if __name__ == "__main__":
    main()
