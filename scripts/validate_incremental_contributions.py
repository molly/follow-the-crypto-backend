"""
Validate the incremental committee-contributions path against the full re-fetch path.

For a single committee, this runs update_committee_contributions twice back-to-back:
  1. full=True   -> complete re-fetch + overwrite (the ground truth)
  2. full=False  -> incremental (early-stop + merge into what step 1 just wrote)

Because nothing new lands in the few seconds between the two runs, the incremental run should
find no new transactions and reproduce step 1's document exactly. We assert that the resulting
rawContributions doc matches on the things that must be exact: the overall total, every large
(individual) record, and every aggregate's summed amount + count. (Aggregate line_number/date can
drift by design; those are intentionally not asserted.)

Pick a SMALL committee to keep request volume (and rate-limit exposure) low.

Usage (run from the repo root):
    python scripts/validate_incremental_contributions.py <committee_id>

NOTE: this writes the committee's real rawContributions doc, same as the pipeline does. It ends in
a correct (full) state, so it's safe, but it is not a dry run.
"""

import os
import sys

# This script lives in scripts/ but imports project modules from the repo root, so put the
# repo root on the path. Run it from the repo root: python scripts/validate_incremental_contributions.py <id>
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from dotenv import load_dotenv

from Database import Database
from fetch_committee_contributions import update_committee_contributions

load_dotenv()


def summarize(doc):
    txns = doc.get("transactions", [])
    total = round(sum(t["contribution_receipt_amount"] for t in txns), 2)
    larges = {
        t["transaction_id"]: t["contribution_receipt_amount"]
        for t in txns
        if not t.get("pre_aggregated")
    }
    aggs = {
        t["transaction_id"]: (
            t["contribution_receipt_amount"],
            t.get("pre_aggregated_count"),
        )
        for t in txns
        if t.get("pre_aggregated")
    }
    known = set(doc.get("known_transaction_ids", []))
    return total, larges, aggs, known


def read_doc(db, committee_id):
    return (
        db.client.collection("rawContributions").document(committee_id).get().to_dict()
        or {}
    )


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        return 1
    committee_id = sys.argv[1]

    db = Database()
    db.get_constants()

    # Restrict the run to just the target committee.
    match = [c for c in db.committees.values() if c["id"] == committee_id]
    if not match:
        print(f"Committee {committee_id} not found in db.committees")
        return 1
    db.committees = {committee_id: match[0]}

    session = requests.Session()

    print(f"[1/2] Full re-fetch for {committee_id} ...")
    update_committee_contributions(db, session, full=True)
    full_doc = read_doc(db, committee_id)
    f_total, f_large, f_aggs, f_known = summarize(full_doc)
    print(
        f"      full: total={f_total} large_records={len(f_large)} "
        f"aggregates={len(f_aggs)} known_ids={len(f_known)}"
    )

    print(f"[2/2] Incremental run for {committee_id} (should find nothing new) ...")
    update_committee_contributions(db, session, full=False)
    incr_doc = read_doc(db, committee_id)
    i_total, i_large, i_aggs, i_known = summarize(incr_doc)
    print(
        f"      incr: total={i_total} large_records={len(i_large)} "
        f"aggregates={len(i_aggs)} known_ids={len(i_known)}"
    )

    ok = True
    if f_total != i_total:
        ok = False
        print(f"  MISMATCH total: full={f_total} incr={i_total}")
    if f_large != i_large:
        ok = False
        only_full = set(f_large) - set(i_large)
        only_incr = set(i_large) - set(f_large)
        print(f"  MISMATCH large records: only_full={only_full} only_incr={only_incr}")
    if f_aggs != i_aggs:
        ok = False
        for key in set(f_aggs) | set(i_aggs):
            if f_aggs.get(key) != i_aggs.get(key):
                print(f"  MISMATCH aggregate {key}: full={f_aggs.get(key)} incr={i_aggs.get(key)}")
    if f_known != i_known:
        ok = False
        print(
            f"  MISMATCH known_transaction_ids: only_full={len(f_known - i_known)} "
            f"only_incr={len(i_known - f_known)}"
        )

    print("\nRESULT:", "PASS — incremental matches full" if ok else "FAIL — see mismatches above")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
