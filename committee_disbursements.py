from get_missing_recipients import get_missing_recipient_data
from recipient_utils import (
    get_all_recipients,
    resolve_recipient_party,
    set_all_recipients,
)
from utils import FEC_fetch, get_sector_keys, pick

DISBURSEMENT_FIELDS = [
    "disbursement_amount",
    "disbursement_date",
    "pdf_url",
    "recipient_committee_id",
    "transaction_id",
]

# F3X-22: transfers to affiliated/authorized committees
# F3X-23: contributions to other political committees (e.g. contributions to non-affiliated PACs)
DISBURSEMENT_LINE_NUMBERS = ["F3X-22", "F3X-23"]


def _fetch_disbursements_for_line(session, committee_id, line_number, disbursements):
    """Fetch all Schedule B disbursements for a given line number and merge into disbursements dict."""
    last_disbursement_date = None
    last_index = None
    disbursements_count = 0
    while True:
        data = FEC_fetch(
            session,
            "committee disbursements",
            "https://api.open.fec.gov/v1/schedules/schedule_b",
            params={
                "committee_id": committee_id,
                "two_year_transaction_period": 2026,
                "line_number": line_number,
                "last_index": last_index,
                "last_disbursement_date": last_disbursement_date,
                "per_page": 100,
            },
        )
        if not data:
            continue
        disbursements_count += len(data["results"])
        for disbursement in data["results"]:
            if not disbursement.get("recipient_committee_id"):
                continue
            recipient_id = disbursement["recipient_committee_id"]
            if recipient_id not in disbursements:
                disbursements[recipient_id] = {
                    "total": disbursement["disbursement_amount"],
                    "recipient_name": disbursement["recipient_name"],
                    "disbursements": [pick(disbursement, DISBURSEMENT_FIELDS)],
                }
            else:
                disbursements[recipient_id]["total"] += disbursement["disbursement_amount"]
                disbursements[recipient_id]["disbursements"].append(
                    pick(disbursement, DISBURSEMENT_FIELDS)
                )

        if disbursements_count >= data["pagination"]["count"]:
            break
        else:
            last_index = data["pagination"]["last_indexes"]["last_index"]
            last_disbursement_date = data["pagination"]["last_indexes"][
                "last_disbursement_date"
            ]


def update_committee_disbursements(db, session):
    committees = db.client.collection("committees").stream()
    new_disbursements = {}
    total_receipts = {"all": 0, "crypto": 0, "ai": 0}
    for committee_snapshot in committees:
        committee = committee_snapshot.to_dict()
        committee_id = committee["id"]
        if committee["committee_type"] in ["N", "O", "Q", "V", "W"]:
            disbursements = {}
            for line_number in DISBURSEMENT_LINE_NUMBERS:
                _fetch_disbursements_for_line(session, committee_id, line_number, disbursements)

            if disbursements:
                old_disbursements = committee.get("disbursements_by_committee", {})
                for recipient_committee_id in disbursements:
                    if recipient_committee_id not in old_disbursements:
                        # All disbursements to this committee are new, add them to new_disbursements
                        for disbursement in disbursements[recipient_committee_id][
                            "disbursements"
                        ]:
                            if committee_id not in new_disbursements:
                                new_disbursements[committee_id] = {}
                            new_disbursements[committee_id][
                                disbursement["transaction_id"]
                            ] = disbursement
                    else:
                        old_disbursement_ids = set(
                            [
                                d["transaction_id"]
                                for d in old_disbursements[recipient_committee_id][
                                    "disbursements"
                                ]
                            ]
                        )
                        for disbursement in disbursements[recipient_committee_id][
                            "disbursements"
                        ]:
                            if (
                                disbursement["transaction_id"]
                                not in old_disbursement_ids
                            ):
                                if committee_id not in new_disbursements:
                                    new_disbursements[committee_id] = {}
                                new_disbursements[committee_id][
                                    disbursement["transaction_id"]
                                ] = disbursement

            db.client.collection("committees").document(committee_id).set(
                {"disbursements_by_committee": disbursements}, merge=True
            )

            contributions = (
                db.client.collection("contributions")
                .document(committee_id)
                .get()
                .to_dict()
            )
            if contributions:
                net = contributions.get("total_contributed", 0)
                for key in get_sector_keys(committee.get("sector")):
                    total_receipts[key] += net
    db.client.collection("totals").document("committees").update({
        "all.net_receipts": total_receipts["all"],
        "crypto.net_receipts": total_receipts["crypto"],
        "ai.net_receipts": total_receipts["ai"],
    })
    return new_disbursements


def _build_schedule_a_transfers(db):
    """Recipient-reported transfers between committees, keyed by
    (sender_committee_id, recipient_committee_id) -> total.

    Built from each committee's contributions doc: a donor group whose link
    points back to /committees/{id} is a transfer received from that committee.
    group["total"] is already deduped/summed by process_committee_contributions."""
    transfers = {}
    for snapshot in db.client.collection("contributions").stream():
        recipient_id = snapshot.id
        if recipient_id == "recent":
            continue
        data = snapshot.to_dict() or {}
        for group in data.get("groups", []):
            link = group.get("link") or ""
            if not link.startswith("/committees/"):
                continue
            sender_id = link.rsplit("/", 1)[-1]
            if not sender_id or sender_id == recipient_id:
                continue
            key = (sender_id, recipient_id)
            transfers[key] = transfers.get(key, 0) + group.get("total", 0)
    return transfers


def summarize_transfers_by_party(db, session):
    """Compute a by-party breakdown of each committee's transfers to other
    committees, mirroring the party_summary computed for individuals/companies.

    Uses the same per-recipient hybrid the frontend transfer graph uses: for a
    recipient we track, prefer its own (Schedule A) report of the transfer,
    falling back to the sender's (Schedule B) disbursement; for an untracked
    recipient, use the sender's disbursement (the only source). Resolves each
    recipient's party from allRecipients, fetching data for any we don't yet
    have. Writes `transfers_by_party` onto each committee that has transfers."""
    all_recipients = get_all_recipients(db)
    schedule_a = _build_schedule_a_transfers(db)

    # Read every committee we track so we can (a) build each sender's hybrid
    # recipient->amount map and (b) decide which recipients are tracked.
    committee_docs = {}
    for committee_snapshot in db.client.collection("committees").stream():
        committee = committee_snapshot.to_dict()
        committee_docs[committee["id"]] = committee
    tracked_ids = set(committee_docs)

    # Gather transfer recipients across all committees, flagging any we don't
    # yet have recipient data for so we can fetch it.
    committees_with_transfers = {}
    has_new_recipients = False
    for committee_id, committee in committee_docs.items():
        disbursements = committee.get("disbursements_by_committee") or {}
        # Union of sender-reported recipients (Schedule B) and recipients that
        # reported a transfer from this committee (Schedule A) — a sender may not
        # have filed a disbursement the recipient has already logged.
        recipient_ids = set(disbursements)
        recipient_ids.update(
            to_id for (from_id, to_id) in schedule_a if from_id == committee_id
        )
        if not recipient_ids:
            continue

        hybrid = {}
        for recipient_id in recipient_ids:
            schedule_b_total = disbursements.get(recipient_id, {}).get("total", 0)
            schedule_a_total = schedule_a.get((committee_id, recipient_id), 0)
            if recipient_id in tracked_ids and schedule_a_total > 0:
                hybrid[recipient_id] = schedule_a_total
            else:
                hybrid[recipient_id] = schedule_b_total
        committees_with_transfers[committee_id] = hybrid

        for recipient_id in recipient_ids:
            if recipient_id not in all_recipients:
                has_new_recipients = True
                all_recipients[recipient_id] = {
                    "committee_id": recipient_id,
                    "candidate_details": {},
                    "needs_data": True,
                }

    # Fetch data for any transfer recipients we haven't seen before, and persist
    # them so the frontend's recipient lookups cover transfer targets too.
    if has_new_recipients:
        all_recipients = get_missing_recipient_data(all_recipients, db, session)
        set_all_recipients(db, all_recipients)

    # Compute and persist the by-party breakdown per committee.
    for committee_id, hybrid in committees_with_transfers.items():
        party_summary = {}
        for recipient_id, amount in hybrid.items():
            if amount <= 0:
                continue
            party = resolve_recipient_party(all_recipients.get(recipient_id, {}))
            party_summary[party] = party_summary.get(party, 0) + amount
        party_summary = {k: round(v, 2) for k, v in party_summary.items()}
        db.client.collection("committees").document(committee_id).set(
            {"transfers_by_party": party_summary}, merge=True
        )
