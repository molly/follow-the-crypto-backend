from collections import defaultdict
from utils import FEC_fetch, pick
import re

# Contributions below this amount (per transaction) will be aggregated by contributor
# to keep rawContributions documents under Firestore's 1MB limit.
PRE_AGGREGATE_THRESHOLD = 1000

# During an incremental run, stop paginating a committee once this many consecutive pages
# (sorted newest-first) contain no transaction IDs we don't already have stored. The small
# buffer (>1) absorbs minor reordering between adjacent pages.
EARLY_STOP_PATIENCE = 2

CONTRIBUTION_FIELDS = [
    "contributor_first_name",
    "contributor_middle_name",
    "contributor_last_name",
    "contributor_suffix",
    "contributor_name",
    "contributor_occupation",
    "contributor_employer",
    "entity_type",
    "contributor_aggregate_ytd",
    "contribution_receipt_amount",
    "contribution_receipt_date",
    "line_number",
    "pdf_url",
    "receipt_type",
    "receipt_type_full",
    "transaction_id",
]


def aggregate_group_key(contrib, individual_employers=None):
    """The employer (or contributor name) under which a small contribution is grouped.

    An "employer" that's actually a non-employer marker (NOT EMPLOYED, RETIRED, SELF, etc.,
    per db.individual_employers) is not a company, so contributions carrying it are keyed by
    contributor name instead — otherwise distinct people get lumped into one bogus company
    rollup. Grouping here happens before the process-time individual_employers guard runs, so
    the guard can't undo it; the check has to live at the fetch layer.
    """
    individual_employers = individual_employers or set()
    employer = (contrib.get("contributor_employer") or "").strip().upper()
    if not employer or employer == "N/A" or employer in individual_employers:
        employer = (contrib.get("contributor_name") or "UNKNOWN").strip().upper()
    return employer


def build_aggregate_record(group_name, contribs):
    """Build a single aggregate record summarizing a group of small contributions."""
    most_recent_date = max(
        (c.get("contribution_receipt_date") or "" for c in contribs), default=""
    )
    # Use the most common line_number to preserve transfer-vs-contribution classification
    line_numbers = [c.get("line_number") for c in contribs if c.get("line_number")]
    line_number = max(set(line_numbers), key=line_numbers.count) if line_numbers else None

    return {
        "contributor_name": group_name,
        "contributor_employer": group_name,
        "entity_type": "ORG",
        "contribution_receipt_amount": round(
            sum(c["contribution_receipt_amount"] for c in contribs), 2
        ),
        "contribution_receipt_date": most_recent_date,
        "line_number": line_number,
        "transaction_id": f"empgroup_{group_name}",
        "pre_aggregated": True,
        "pre_aggregated_count": len(contribs),
    }


def pre_aggregate_small_contributions(contributions, individual_employers=None):
    """
    Contributions below PRE_AGGREGATE_THRESHOLD are grouped by employer (or contributor_name
    if no employer) and stored as a single aggregate record per group. This keeps rawContributions
    documents under Firestore's 1MB limit for large PACs with many small employee contributions.

    Contributions >= PRE_AGGREGATE_THRESHOLD are kept as individual records.
    """
    large = [c for c in contributions if c["contribution_receipt_amount"] >= PRE_AGGREGATE_THRESHOLD]
    small = [c for c in contributions if c["contribution_receipt_amount"] < PRE_AGGREGATE_THRESHOLD]

    by_group = defaultdict(list)
    for contrib in small:
        by_group[aggregate_group_key(contrib, individual_employers)].append(contrib)

    aggregated = [
        build_aggregate_record(group_name, contribs)
        for group_name, contribs in by_group.items()
    ]

    return large + aggregated


def merge_incremental_contributions(old_transactions, new_contributions, individual_employers=None):
    """
    Fold newly-fetched raw contributions into the already-stored (aggregated) transaction list,
    so an incremental run doesn't have to re-fetch and re-aggregate a committee's full history.

    Large contributions (>= PRE_AGGREGATE_THRESHOLD) are appended as individual records. Small
    ones are folded into their employer's aggregate: an existing empgroup_ record has its amount,
    count, and most-recent-date updated; otherwise a new aggregate is created. The aggregate's
    line_number can drift slightly between full refreshes (we can't recompute the true mode without
    the original raw rows); the periodic --full-contributions run rebuilds it exactly.
    """
    by_id = {t["transaction_id"]: t for t in old_transactions}

    small = [c for c in new_contributions if c["contribution_receipt_amount"] < PRE_AGGREGATE_THRESHOLD]
    large = [c for c in new_contributions if c["contribution_receipt_amount"] >= PRE_AGGREGATE_THRESHOLD]

    by_group = defaultdict(list)
    for contrib in small:
        by_group[aggregate_group_key(contrib, individual_employers)].append(contrib)

    for group_name, contribs in by_group.items():
        agg_id = f"empgroup_{group_name}"
        existing = by_id.get(agg_id)
        if existing and existing.get("pre_aggregated"):
            existing["contribution_receipt_amount"] = round(
                existing["contribution_receipt_amount"]
                + sum(c["contribution_receipt_amount"] for c in contribs),
                2,
            )
            existing["pre_aggregated_count"] = (
                existing.get("pre_aggregated_count", 0) + len(contribs)
            )
            new_date = max(
                (c.get("contribution_receipt_date") or "" for c in contribs), default=""
            )
            if new_date > (existing.get("contribution_receipt_date") or ""):
                existing["contribution_receipt_date"] = new_date
        else:
            by_id[agg_id] = build_aggregate_record(group_name, contribs)

    for contrib in large:
        by_id[contrib["transaction_id"]] = contrib

    return list(by_id.values())


def get_ids_to_omit(contribs):
    """Dedupe contributions, refunds, etc."""
    to_omit = set()
    # Some FEC records come through with a null transaction_id. They have no ".N" suffix to
    # parse, so they can't take part in the parent/child dedup below — and re.match would
    # raise on None.
    transaction_ids = set(
        x["transaction_id"] for x in contribs if x.get("transaction_id")
    )
    for t_id in transaction_ids:
        # There are sometimes 2+ transactions with IDs like SA17.4457 and SA17.4457.0, in which case we omit the former.
        # These are typically instances in which the committee has reported the dollar equivalent and the in-kind
        # contribution separately for the same contribution, or where a transaction from multiple people has been
        # reported as a group and then individually.
        m = re.match(r"^(.*?)\.\d$", t_id)
        if m:
            if m.group(1) in transaction_ids:
                to_omit.add(m.group(1))
    return to_omit


def should_omit(contrib, other_contribs, ids_to_omit):
    """Omit any duplicate contributions, refunds, etc."""
    transaction_id = contrib.get("transaction_id")
    # A null transaction_id identifies nothing, so it can't mark a manual exclusion or a
    # duplicate. Two null-id records are not the same contribution; matching them against
    # each other would silently drop all but the first.
    if transaction_id:
        if transaction_id in ids_to_omit:
            # Manually excluded transaction, or a parent of a more granularly reported transaction
            return True
        if transaction_id in other_contribs:
            # Duplicate of a transaction we've already encountered
            return True
    if contrib["line_number"] in ["15", "16"]:
        return True
    if contrib["line_number"] == "17":
        # Line 17 is "Other Federal Receipts" (dividends, interest, offsets, etc.), which are
        # generally not contributions and so omitted. The exceptions we keep are real
        # contributions that get reported here: money into a hybrid PAC's non-contribution
        # ("Carey") account, and any record whose receipt type explicitly names it a contribution.
        if "receipt_type_full" not in contrib:
            # Efiled records don't carry a receipt_type_full, so we can't classify them here.
            # SA17.5207 was manually verified as a real contribution.
            return contrib.get("transaction_id", "") != "SA17.5207"
        receipt_type_full = (contrib.get("receipt_type_full", "") or "").upper()
        if "CAREY" in receipt_type_full:
            return False
        if "CONTRIBUTION" in receipt_type_full and "INTEREST" not in receipt_type_full:
            return False
        return True
    memo = (contrib.get("memo_text", "") or "").upper()
    receipt_type = (contrib.get("receipt_type_full", "") or "").upper()
    # Skip the LLC/partnership parent records that say "SEE ATTRIBUTION BELOW" — those are
    # duplicates of the individual partner attribution records which follow them. Keep the
    # "PARTNERSHIP ATTRIBUTION" records since those are the actual attributed contributions.
    if "ATTRIBUTION" in receipt_type:
        return True
    if "SEE ATTRIBUTION" in memo:
        return True
    return False


def _normalize_efiled(picked):
    """Apply efile-specific cleanup (efilings are lowercased and have trailing commas)."""
    picked["efiled"] = True
    # Name/employer/etc fields are lowercased in efilings data, so uppercase them for consistency.
    for key in CONTRIBUTION_FIELDS[:7]:
        if key in picked and isinstance(picked[key], str):
            picked[key] = picked[key].upper()
    # When the contributor name is a company, it has trailing commas. Strip them.
    picked["contributor_name"] = picked["contributor_name"].strip(",")
    return picked


def update_committee_contributions(db, session, full=False):
    """
    This stores contributions (with a trimmed set of fields) in the "rawContributions" collection in Firestore. Those
    contributions will later be processed in process_committee_contributions.py into a format that saves computation
    on the frontend (doing rollups, redactions, etc.)

    This function fetches both processed and efiled contributions.

    By default the run is INCREMENTAL: contributions are sorted newest-first, so once we've seen
    EARLY_STOP_PATIENCE consecutive pages containing no transaction IDs we don't already have stored,
    we stop paginating and merge the newly-found transactions into the existing document. This keeps a
    routine run to a handful of requests per committee instead of re-walking the full two-year history.

    Pass full=True (the --full-contributions flag) to force a complete re-fetch and overwrite. That
    rebuilds the aggregates exactly and reconciles amendments/deletions to old transactions that an
    incremental run can't see; run it periodically.
    """

    committee_ids = [committee["id"] for committee in db.committees.values()]
    new_contributions = {}
    for committee_id in committee_ids:
        old = (
            db.client.collection("rawContributions")
            .document(committee_id)
            .get()
            .to_dict()
        )
        if old:
            old_transactions = old.get("transactions", [])
            old_known_ids = set(
                old.get(
                    "known_transaction_ids",
                    [x["transaction_id"] for x in old_transactions],
                )
            )
        else:
            old_transactions = []
            old_known_ids = set()

        had_old_doc = old is not None
        # An incremental run only makes sense when we have a baseline to diff against.
        incremental = not full and bool(old_known_ids)

        fetched = []  # contributions kept this run (everything if full; only-new if incremental)
        fetched_ids = set()  # transaction ids seen this run (for cross-page dedup)
        ids_to_omit = (
            set(db.duplicate_contributions[committee_id])
            if committee_id in db.duplicate_contributions
            else set()
        )

        def handle_page(results, efiled):
            """Process one page of results. Returns True if the page held no new transaction IDs."""
            ids_to_omit.update(get_ids_to_omit(results))
            page_has_new = False
            for contrib in results:
                tid = contrib["transaction_id"]
                if tid not in old_known_ids:
                    page_has_new = True
                if should_omit(contrib, fetched_ids, ids_to_omit):
                    continue
                # In incremental mode, skip transactions we've already stored — but still
                # record the id so should_omit's cross-page dedup keeps working.
                if incremental and tid in old_known_ids:
                    fetched_ids.add(tid)
                    continue
                picked = pick(contrib, CONTRIBUTION_FIELDS)
                if efiled:
                    _normalize_efiled(picked)
                fetched.append(picked)
                fetched_ids.add(tid)
            return not page_has_new

        # First fetch processed contributions
        last_index = None
        last_contribution_receipt_date = None
        contribs_count = 0
        dry_streak = 0
        while True:
            data = FEC_fetch(
                session,
                "committee contributions",
                "https://api.open.fec.gov/v1/schedules/schedule_a",
                params={
                    "committee_id": committee_id,
                    "two_year_transaction_period": 2026,
                    "per_page": 100,
                    "sort": "-contribution_receipt_date",
                    "last_index": last_index,
                    "last_contribution_receipt_date": last_contribution_receipt_date,
                },
            )

            if not data:
                continue

            contribs_count += data["pagination"]["per_page"]
            page_dry = handle_page(data["results"], efiled=False)

            if incremental:
                dry_streak = dry_streak + 1 if page_dry else 0
                if dry_streak >= EARLY_STOP_PATIENCE:
                    break

            # Fetch more pages if they exist, or break
            if contribs_count >= data["pagination"]["count"]:
                break
            else:
                last_index = data["pagination"]["last_indexes"]["last_index"]
                last_contribution_receipt_date = data["pagination"]["last_indexes"][
                    "last_contribution_receipt_date"
                ]

        # Now fetch efiled contributions that may have not yet been processed
        page = 1
        contribs_count = 0
        dry_streak = 0
        while True:
            data = FEC_fetch(
                session,
                "unprocessed committee contributions",
                "https://api.open.fec.gov/v1/schedules/schedule_a/efile",
                params={
                    "committee_id": committee_id,
                    "min_date": "2025-01-01",
                    "per_page": 100,
                    "sort": "-contribution_receipt_date",
                    "page": page,
                },
            )

            if not data:
                continue

            contribs_count += data["pagination"]["per_page"]
            page_dry = handle_page(data["results"], efiled=True)

            if incremental:
                dry_streak = dry_streak + 1 if page_dry else 0
                if dry_streak >= EARLY_STOP_PATIENCE:
                    break

            # Fetch more pages if they exist, or break
            if page >= data["pagination"]["pages"]:
                break
            else:
                page += 1

        # Record genuinely new transactions (for the run's new-contribution count). As before,
        # only diff against a committee that already had a stored document.
        if had_old_doc:
            for contrib in fetched:
                if contrib["transaction_id"] not in old_known_ids:
                    new_contributions[contrib["transaction_id"]] = contrib

        # Build the document. Incremental merges new transactions into the stored aggregates;
        # full re-aggregates everything from scratch. known_transaction_ids is stored alongside so
        # real IDs survive even after small contributions are folded into empgroup_ records.
        if incremental:
            transactions_for_storage = merge_incremental_contributions(
                old_transactions, fetched, db.individual_employers
            )
            known_ids = old_known_ids.union(fetched_ids)
        else:
            transactions_for_storage = pre_aggregate_small_contributions(
                fetched, db.individual_employers
            )
            known_ids = set(fetched_ids)

        db.client.collection("rawContributions").document(committee_id).set(
            {
                "transactions": transactions_for_storage,
                "known_transaction_ids": list(known_ids),
            }
        )
    return new_contributions
