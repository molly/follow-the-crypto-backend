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
