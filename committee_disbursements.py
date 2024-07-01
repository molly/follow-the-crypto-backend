from utils import FEC_fetch, pick

DISBURSEMENT_FIELDS = ["disbursement_amount", "disbursement_date", "pdf_url"]


def update_committee_disbursements(db):
    committees = db.client.collection("committees").stream()
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
                db.client.collection("committees").document(committee_id).set(
                    {"disbursements_by_committee": disbursements}, merge=True
                )
