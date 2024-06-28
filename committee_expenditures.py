from utils import FEC_fetch, pick

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
]


def get_race_name(expenditure):
    race = "{candidate_office_state}-{candidate_office}".format(**expenditure)
    if (
        expenditure["candidate_office_district"]
        and int(expenditure["candidate_office_district"]) != 0
    ):
        race += "-" + expenditure["candidate_office_district"]
    return race


def handle_amendment(exp, amendments, state_data, committee_id, race):
    if exp["transaction_id"] not in amendments:
        amendments[exp["transaction_id"]] = exp.get("amendment_number", 0)
    elif amendments[exp["transaction_id"]] > exp.get("amendment_number", 0):
        amendments[exp["transaction_id"]] = exp["amendment_number"]
    else:
        # Skip this amendment, as we've already processed a newer one
        return False

    # Remove amended expenditure from committee expenditures
    if committee_id in state_data["by_committee"]:
        old_ind = next(
            (
                i
                for i, transaction in enumerate(
                    state_data["by_committee"][committee_id]["expenditures"]
                )
                if transaction["transaction_id"] == exp["transaction_id"]
            ),
            -1,
        )
        if old_ind != -1:
            state_data["by_committee"][committee_id]["total"] = round(
                state_data["by_committee"][committee_id]["total"]
                - state_data["by_committee"][committee_id]["expenditures"][old_ind][
                    "expenditure_amount"
                ],
                2,
            )
            state_data["by_committee"][committee_id]["expenditures"].pop(old_ind)

    # Remove amended expenditure from race expenditures
    if race in state_data["by_race"]:
        old_ind = next(
            (
                i
                for i, transaction in enumerate(
                    state_data["by_race"][race]["expenditures"]
                )
                if transaction["transaction_id"] == exp["transaction_id"]
            ),
            -1,
        )
        if old_ind != -1:
            state_data["by_race"][race]["total"] = round(
                state_data["by_race"][race]["total"]
                - state_data["by_race"][race]["expenditures"][old_ind][
                    "expenditure_amount"
                ],
                2,
            )
            state_data["by_race"][race]["expenditures"].pop(old_ind)
    return state_data


def update_committee_expenditures(db):
    """
    Fetch expenditures that have been processed by the FEC. Recent expenditures may not be included in this data, and
    are fetched separately in update_recent_committee_expenditures.
    """
    committee_ids = [committee["id"] for committee in db.committees.values()]
    states = {}

    last_index = None
    last_expenditure_date = None
    exp_count = 0
    for committee_id in committee_ids:
        amendments = {}
        transaction_ids = set()

        while True:
            data = FEC_fetch(
                "committee expenditures",
                "https://api.open.fec.gov/v1/schedules/schedule_e",
                params={
                    "committee_id": committee_id,
                    "per_page": 100,
                    "last_index": last_index,
                    "last_expenditure_date": last_expenditure_date,
                    "cycle": 2024,
                },
            )

            if not data:
                continue

            exp_count += data["pagination"]["per_page"]

            for exp in data["results"]:
                amount = round(exp["expenditure_amount"], 2)
                race = get_race_name(exp)

                if exp["amendment_indicator"] == "A":
                    state_data = states[exp["candidate_office_state"]]
                    new_state_data = handle_amendment(
                        exp, amendments, state_data, committee_id, race
                    )
                    if new_state_data:
                        states[exp["candidate_office_state"]] = new_state_data
                    else:
                        # Skip this amendment, as we've already processed a newer one
                        continue
                else:
                    if exp["transaction_id"] in amendments:
                        # Skip this expenditure, as it's been amended
                        continue
                    if exp["transaction_id"] in transaction_ids:
                        # Skip this expenditure, as it's a duplicate
                        continue

                transaction_ids.add(exp["transaction_id"])

                if exp["candidate_office_state"] not in states:
                    states[exp["candidate_office_state"]] = {
                        "total": amount,
                        "by_committee": {},
                        "by_race": {},
                    }
                else:
                    states[exp["candidate_office_state"]]["total"] = round(
                        states[exp["candidate_office_state"]]["total"] + amount, 2
                    )

                if (
                    committee_id
                    not in states[exp["candidate_office_state"]]["by_committee"]
                ):
                    states[exp["candidate_office_state"]]["by_committee"][
                        committee_id
                    ] = {
                        "total": amount,
                        "expenditures": [
                            {
                                **pick(exp, EXPENDITURE_FIELDS),
                                "committee_id": committee_id,
                            }
                        ],
                    }
                else:
                    states[exp["candidate_office_state"]]["by_committee"][committee_id][
                        "total"
                    ] = round(
                        states[exp["candidate_office_state"]]["by_committee"][
                            committee_id
                        ]["total"]
                        + amount,
                        2,
                    )
                    states[exp["candidate_office_state"]]["by_committee"][committee_id][
                        "expenditures"
                    ].append(
                        {
                            **pick(exp, EXPENDITURE_FIELDS),
                            "committee_id": committee_id,
                        }
                    )

                if race not in states[exp["candidate_office_state"]]["by_race"]:
                    states[exp["candidate_office_state"]]["by_race"][race] = {
                        "total": amount,
                        "details": {
                            "candidate_office": exp["candidate_office"],
                            "candidate_office_district": exp[
                                "candidate_office_district"
                            ],
                        },
                        "expenditures": [
                            {
                                **pick(exp, EXPENDITURE_FIELDS),
                                "committee_id": committee_id,
                            }
                        ],
                    }
                else:
                    states[exp["candidate_office_state"]]["by_race"][race][
                        "total"
                    ] = round(
                        states[exp["candidate_office_state"]]["by_race"][race]["total"]
                        + amount,
                        2,
                    )
                    states[exp["candidate_office_state"]]["by_race"][race][
                        "expenditures"
                    ].append(
                        {
                            **pick(exp, EXPENDITURE_FIELDS),
                            "committee_id": committee_id,
                        }
                    )

            if exp_count >= data["pagination"]["count"]:
                break
            else:
                last_index = data["pagination"]["last_indexes"]["last_index"]
                last_expenditure_date = data["pagination"]["last_indexes"][
                    "last_expenditure_date"
                ]

    races = {}
    for state, state_expenditures in states.items():
        for race, race_data in state_expenditures["by_race"].items():
            races[race] = race_data
        db.client.collection("expendituresByState").document(state).set(
            state_expenditures
        )


def update_recent_committee_expenditures(db):
    """
    There may be more recent expenditures that have not yet been processed. Fetch these. This function is
    safe to re-run as often as needed, as it checks for duplicates before adding new contributions.
    """
    committee_ids = [committee["id"] for committee in db.committees.values()]

    # Save new contributions to pass to bot code
    new_expenditures = []
    for committee_id in committee_ids:
        page = 1
        while True:
            data = FEC_fetch(
                "unprocessed committee expenditures",
                "https://api.open.fec.gov/v1/schedules/schedule_e/efile",
                params={
                    "committee_id": committee_id,
                    "per_page": 100,
                    "min_date": "2023-01-01",
                    "sort": "-expenditure_date",
                    "page": page,
                },
            )

            if not data:
                continue

            results = data["results"]
            amendments = {}

            for exp in results:
                state = exp["candidate_office_state"]
                race = get_race_name(exp)
                # Efiled expenditures store the candidate last name in the candidate name field, causing problems
                # down the line. Copy it over to keep consistent.
                exp["candidate_last_name"] = exp["candidate_name"]

                # Get existing contributions for this state, if there are any
                state_contributions_snapshot = (
                    db.client.collection("expendituresByState").document(state).get()
                )
                if not state_contributions_snapshot.exists:
                    state_contributions = {
                        "total": round(exp["expenditure_amount"], 20),
                        "by_committee": {},
                        "by_race": {},
                    }
                else:
                    state_contributions = state_contributions_snapshot.to_dict()

                if exp["amendment_indicator"] == "A":
                    result = handle_amendment(
                        exp, amendments, state_contributions, committee_id, race
                    )
                    if not result:
                        # Skip this amendment, as we've already processed a newer one
                        continue
                else:
                    if exp["transaction_id"] in amendments:
                        # Skip this expenditure, as it's been amended
                        continue

                # Check that we haven't already recorded this transaction from the processed data
                if not any(
                    recorded["transaction_id"] == exp["transaction_id"]
                    for recorded in state_contributions["by_race"]
                    .get(race, {})
                    .get("expenditures", {})
                ):
                    # Add to by_committee
                    if committee_id not in state_contributions["by_committee"]:
                        state_contributions["by_committee"][committee_id] = {
                            "total": round(exp["expenditure_amount"], 2),
                            "expenditures": [
                                {
                                    **pick(exp, EXPENDITURE_FIELDS),
                                    "committee_id": committee_id,
                                }
                            ],
                        }
                    else:
                        state_contributions["by_committee"][committee_id][
                            "total"
                        ] = round(
                            state_contributions["by_committee"][committee_id]["total"]
                            + exp["expenditure_amount"],
                            2,
                        )
                        state_contributions["by_committee"][committee_id][
                            "expenditures"
                        ].append(
                            {
                                **pick(exp, EXPENDITURE_FIELDS),
                                "committee_id": committee_id,
                            }
                        )

                    # Add to by_race
                    if race not in state_contributions["by_race"]:
                        state_contributions["by_race"][race] = {
                            "total": round(exp["expenditure_amount"], 2),
                            "details": {
                                "candidate_office": exp["candidate_office"],
                                "candidate_office_district": exp[
                                    "candidate_office_district"
                                ],
                            },
                            "expenditures": [
                                {
                                    **pick(exp, EXPENDITURE_FIELDS),
                                    "committee_id": committee_id,
                                }
                            ],
                        }
                    else:
                        state_contributions["by_race"][race]["total"] = round(
                            state_contributions["by_race"][race]["total"]
                            + exp["expenditure_amount"],
                            2,
                        )
                        state_contributions["by_race"][race]["expenditures"].append(
                            {
                                **pick(exp, EXPENDITURE_FIELDS),
                                "committee_id": committee_id,
                            }
                        )

                    new_expenditures.append(exp)
                    db.client.collection("expendituresByState").document(state).set(
                        state_contributions
                    )

            if page >= data["pagination"]["pages"]:
                break
            else:
                page += 1
        return new_expenditures
