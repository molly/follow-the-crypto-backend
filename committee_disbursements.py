from utils import FEC_fetch, pick

DISBURSEMENT_FIELDS = [
    "disbursement_amount",
    "disbursement_date",
    "pdf_url",
    "recipient_committee_id",
    "transaction_id",
]


def update_committee_disbursements(db):
    committees = db.client.collection("committees").stream()
    new_disbursements = {}
    total_receipts = 0
    for committee_snapshot in committees:
        committee = committee_snapshot.to_dict()
        committee_id = committee["id"]
        if committee["committee_type"] in ["N", "O", "Q", "V", "W"]:
            disbursements = {}
            last_disbursement_date = None
            last_index = None
            disbursements_count = 0
            while True:
                data = FEC_fetch(
                    "committee disbursements",
                    "https://api.open.fec.gov/v1/schedules/schedule_b",
                    params={
                        "committee_id": committee_id,
                        "two_year_transaction_period": 2024,
                        "line_number": "F3X-22",
                        "last_index": last_index,
                        "last_disbursement_date": last_disbursement_date,
                        "per_page": 100,
                    },
                )
                if not data:
                    continue
                disbursements_count += data["pagination"]["per_page"]
                for disbursement in data["results"]:
                    if disbursement["recipient_committee_id"] not in disbursements:
                        disbursements[disbursement["recipient_committee_id"]] = {
                            "total": disbursement["disbursement_amount"],
                            "recipient_name": disbursement["recipient_name"],
                            "disbursements": [pick(disbursement, DISBURSEMENT_FIELDS)],
                        }
                    else:
                        disbursements[disbursement["recipient_committee_id"]][
                            "total"
                        ] += disbursement["disbursement_amount"]
                        disbursements[disbursement["recipient_committee_id"]][
                            "disbursements"
                        ].append(pick(disbursement, DISBURSEMENT_FIELDS))

                if disbursements_count >= data["pagination"]["count"]:
                    break
                else:
                    last_index = data["pagination"]["last_indexes"]["last_index"]
                    last_disbursement_date = data["pagination"]["last_indexes"][
                        "last_disbursement_date"
                    ]

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

            disbursements_total = sum(
                [
                    recipient["total"]
                    for recipient in disbursements.values()
                    if recipient["total"] > 0
                ]
            )
            total_receipts += committee.get("receipts", 0) - disbursements_total
    db.client.collection("totals").document("committees").set(
        {"net_receipts": total_receipts}, merge=True
    )
    return new_disbursements
