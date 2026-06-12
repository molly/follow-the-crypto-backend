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
                # Normalize a stale candidate_id (e.g. an old House id after a switch
                # to Senate) to its canonical id so the expenditure substring-matches
                # the race roster on the frontend instead of being hidden.
                if exp.get("candidate_id") in db.candidate_aliases:
                    exp["candidate_id"] = db.candidate_aliases[exp["candidate_id"]]
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
                # Normalize a stale candidate_id (e.g. an old House id after a switch
                # to Senate) to its canonical id so the expenditure substring-matches
                # the race roster on the frontend instead of being hidden.
                if exp.get("candidate_id") in db.candidate_aliases:
                    exp["candidate_id"] = db.candidate_aliases[exp["candidate_id"]]
                if exp["amendment_indicator"] == "A":
                    if uid not in transactions:
                        # Original was never in the processed endpoint (e.g. it was
                        # filed and immediately corrected); treat the amendment as
                        # the authoritative record.
                        transactions[uid] = pick(exp, EXPENDITURE_FIELDS)
                    elif (
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

    # Backfill missing candidate_ids from a sibling expenditure for the same seat
    # and candidate. FEC efile records occasionally omit candidate_id even when the
    # candidate's name/office/state/district are present; without it the IE can't
    # substring-match a race roster on the frontend and is hidden. Only fill when a
    # single candidate_id exists for that seat+name, so the match is unambiguous.
    def _seat_name_key(t):
        return (
            t.get("candidate_office_state"),
            t.get("candidate_office"),
            str(t.get("candidate_office_district")),
            (t.get("candidate_last_name") or "").upper(),
        )

    ids_by_seat_name = {}
    for t in transactions.values():
        if t.get("candidate_id"):
            ids_by_seat_name.setdefault(_seat_name_key(t), set()).add(
                t["candidate_id"]
            )
    for t in transactions.values():
        if not t.get("candidate_id"):
            siblings = ids_by_seat_name.get(_seat_name_key(t))
            if siblings and len(siblings) == 1:
                t["candidate_id"] = next(iter(siblings))

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
