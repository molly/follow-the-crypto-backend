from utils import FEC_fetch, pick
import re

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


def get_ids_to_omit(contribs):
    """Dedupe contributions, refunds, etc."""
    to_omit = set()
    transaction_ids = set([x["transaction_id"] for x in contribs])
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
    if contrib["line_number"] in ["15", "16", "17"]:
        return True
    if contrib["transaction_id"] in ids_to_omit:
        # Manually excluded transaction, or a parent of a more granularly reported transaction
        return True
    if contrib["transaction_id"] in other_contribs:
        # Duplicate of a transaction we've already encountered
        return True
    if (
        "ATTRIBUTION" in (contrib.get("memo_text", "") or "").upper()
        or "ATTRIBUTION" in (contrib.get("receipt_type_full", "") or "").upper()
    ):
        return True
    return False


def update_committee_contributions(db):
    """
    This stores contributions (with a trimmed set of fields) in the "rawContributions" collection in Firestore. Those
    contributions will later be processed in process_committee_contributions.py into a format that saves computation
    on the frontend (doing rollups, redactions, etc.)

    This function fetches both processed and efiled contributions.
    """

    committee_ids = [committee["id"] for committee in db.committees.values()]
    new_contributions = {}
    for committee_id in committee_ids:
        contributions = []
        last_index = None
        last_contribution_receipt_date = None
        contribs_count = 0
        contrib_ids = set()
        ids_to_omit = (
            set(db.duplicate_contributions[committee_id])
            if committee_id in db.duplicate_contributions
            else set()
        )

        # First fetch processed contributions
        while True:
            data = FEC_fetch(
                "committee contributions",
                "https://api.open.fec.gov/v1/schedules/schedule_a",
                params={
                    "committee_id": committee_id,
                    "two_year_transaction_period": 2024,
                    "per_page": 100,
                    "sort": "-contribution_receipt_date",
                    "last_index": last_index,
                    "last_contribution_receipt_date": last_contribution_receipt_date,
                },
            )

            if not data:
                continue

            contribs_count += data["pagination"]["per_page"]
            results = data["results"]
            ids_to_omit = ids_to_omit.union(get_ids_to_omit(results))
            # TODO Edge case with duplicates that exist across pages?

            for contrib in results:
                if should_omit(contrib, contrib_ids, ids_to_omit):
                    continue
                contributions.append(pick(contrib, CONTRIBUTION_FIELDS))
                contrib_ids.add(contrib["transaction_id"])

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
        while True:
            data = FEC_fetch(
                "unprocessed committee contributions",
                "https://api.open.fec.gov/v1/schedules/schedule_a/efile",
                params={
                    "committee_id": committee_id,
                    "min_date": "2023-01-01",
                    "per_page": 100,
                    "sort": "-contribution_receipt_date",
                    "page": page,
                },
            )

            if not data:
                continue

            contribs_count += data["pagination"]["per_page"]
            results = data["results"]
            ids_to_omit = ids_to_omit.union(get_ids_to_omit(results))
            for contrib in results:
                if should_omit(contrib, contrib_ids, ids_to_omit):
                    continue
                picked = pick(contrib, CONTRIBUTION_FIELDS)
                picked["efiled"] = True

                # Name/employer/etc fields are lowercased in efilings data, so uppercase them for consistency.
                for key in CONTRIBUTION_FIELDS[:7]:
                    if key in picked and isinstance(picked[key], str):
                        picked[key] = picked[key].upper()

                # When the contributor name is a company, it has trailing commas. Strip them.
                picked["contributor_name"] = picked["contributor_name"].strip(",")
                contributions.append(picked)

            # Fetch more pages if they exist, or break
            if page >= data["pagination"]["pages"]:
                break
            else:
                page += 1

        # Diff with previously stored transactions and store any new transactions
        old = (
            db.client.collection("rawContributions")
            .document(committee_id)
            .get()
            .to_dict()
        )
        if old:
            old_ids = set([x["transaction_id"] for x in old["transactions"]])
            diff_ids = contrib_ids.difference(old_ids)
            if diff_ids:
                for diff_id in diff_ids:
                    new_contributions[diff_id] = next(
                        x for x in contributions if x["transaction_id"] == diff_id
                    )
        db.client.collection("rawContributions").document(committee_id).set(
            {"transactions": contributions}
        )
    return new_contributions
