from utils import FEC_fetch, pick, get_expenditure_race_type

EXPENDITURE_FIELDS = [
    "expenditure_amount",
    "candidate_office_state",
    "expenditure_date",
    "expenditure_description",
    "candidate_id",
    "candidate_first_name",
    "candidate_last_name",
    "candidate_middle_name",
    "candidate_suffix",
    "candidate_name",
    "candidate_office",
    "candidate_office_state",
    "candidate_office_district",
    "candidate_party",
    "category_code",
    "category_code_full",
    "dissemination_date",
    "election_type",
    "payee_name",
    "support_oppose_indicator",
    "transaction_id",
    # Custom added fields
    "subrace",
    "committee_id",
    "uid",
]


def get_race_name(expenditure):
    race = "{candidate_office_state}-{candidate_office}".format(**expenditure)
    if (
        expenditure["candidate_office_district"]
        and int(expenditure["candidate_office_district"]) != 0
    ):
        race += "-" + expenditure["candidate_office_district"]
    return race


def update_committee_expenditures(db, session):
    """
    Fetch processed transactions, and any transactions that have been efiled but not yet processed.
    These are stored raw in expenditures.all, and processed later in process_committee_expenditures.py.
    """
    committee_ids = [committee["id"] for committee in db.committees.values()]
    transactions = {}
    last_index = None
    last_expenditure_date = None
    exp_count = 0
    for committee_id in committee_ids:
        # First fetch processed expenditures
        while True:
            data = FEC_fetch(
                session,
                "committee expenditures",
                "https://api.open.fec.gov/v1/schedules/schedule_e",
                params={
                    "committee_id": committee_id,
                    "per_page": 100,
                    "is_notice": True,
                    "most_recent": True,
                    "cycle": 2026,
                    "last_index": last_index,
                    "last_expenditure_date": last_expenditure_date,
                },
            )

            if not data:
                continue

            exp_count += data["pagination"]["per_page"]

            for exp in data["results"]:
                if exp["memoed_subtotal"]:
                    continue
                exp["subrace"] = get_expenditure_race_type(exp)
                exp["committee_id"] = committee_id
                uid = "{}-{}".format(exp["committee_id"], exp["transaction_id"])
                exp["uid"] = uid
                if exp["amendment_indicator"] == "A":
                    if uid in transactions and (
                        transactions[uid]["amendment_indicator"] == "N"
                        or transactions[uid]["amendment_number"]
                        < exp["amendment_number"]
                    ):
                        transactions[uid] = pick(exp, EXPENDITURE_FIELDS)
                elif uid not in transactions:
                    transactions[uid] = pick(exp, EXPENDITURE_FIELDS)

            if exp_count >= data["pagination"]["count"]:
                break
            else:
                last_index = data["pagination"]["last_indexes"]["last_index"]
                last_expenditure_date = data["pagination"]["last_indexes"][
                    "last_expenditure_date"
                ]

        # Now fetch efiled expenditures that may have not yet been processed
        page = 1
        while True:
            data = FEC_fetch(
                session,
                "unprocessed committee expenditures",
                "https://api.open.fec.gov/v1/schedules/schedule_e/efile",
                params={
                    "committee_id": committee_id,
                    "per_page": 100,
                    "min_date": "2025-01-01",
                    "sort": "-expenditure_date",
                    "is_notice": True,
                    "most_recent": True,
                    "page": page,
                },
            )

            if not data:
                continue

            results = data["results"]
            for exp in results:
                # Efiled expenditures store the candidate last name in the candidate name field, causing problems
                # down the line. Copy it over to keep consistent.
                exp["candidate_last_name"] = exp["candidate_name"]
                exp["subrace"] = get_expenditure_race_type(exp)

                uid = "{}-{}".format(exp["committee_id"], exp["transaction_id"])
                exp["uid"] = uid
                if exp["amendment_indicator"] == "A":
                    if uid in transactions and (
                        (
                            not transactions[uid].get("amendment_indicator", None)
                            or transactions[uid].get("amendment_indicator", None) == "N"
                        )
                        or (
                            not transactions[uid].get("amendment_number", None)
                            or transactions[uid].get("amendment_number", None)
                            < exp["amendment_number"]
                        )
                    ):
                        transactions[uid] = pick(exp, EXPENDITURE_FIELDS)
                elif uid not in transactions:
                    transactions[uid] = pick(exp, EXPENDITURE_FIELDS)

            if page >= data["pagination"]["pages"]:
                break
            else:
                page += 1

    # Diff with previously stored expenditures
    # new_transactions = {}
    # old_transactions = (
    #     db.client.collection("expenditures").document("all").get().to_dict()
    # )
    # if old_transactions:
    #     old_transaction_ids = set(old_transactions.keys())
    #     diff_ids = set(transactions.keys()).difference(old_transaction_ids)
    #     if diff_ids:
    #         new_transactions = {x: transactions[x] for x in diff_ids}

    db.client.collection("expenditures").document("all").set(transactions)
    # return new_transactions
