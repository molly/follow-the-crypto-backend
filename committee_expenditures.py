import logging
import requests
from secrets import FEC_API_KEY
from utils import FEC_fetch, pick

EXPENDITURE_FIELDS = [
    "expenditure_amount",
    "candidate_office_state",
    "expenditure_date",
    "expenditure_description",
    "candidate_first_name",
    "candidate_last_name",
    "candidate_middle_name",
    "candidate_suffix",
    "candidate_name",
    "candidate_office",
    "candidate_office_district",
    "candidate_party",
    "category_code",
    "category_code_full",
    "payee_name",
    "support_oppose_indicator",
]


def update_committee_expenditures(db):
    committee_ids = [committee["id"] for committee in db.committees.values()]
    states = {}

    last_index = None
    last_expenditure_date = None
    exp_count = 0
    for committee_id in committee_ids:
        while True:
            data = FEC_fetch(
                "committee expenditures",
                "https://api.open.fec.gov/v1/schedules/schedule_e",
                params={
                    "committee_id": committee_id,
                    "per_page": 100,
                    "last_index": last_index,
                    "last_expenditure_date": last_expenditure_date,
                },
            )

            if not data:
                continue

            exp_count += data["pagination"]["per_page"]

            for exp in data["results"]:
                amount = round(exp["expenditure_amount"], 2)
                race = "{candidate_office_state}-{candidate_office}".format(**exp)
                if (
                    exp["candidate_office_district"]
                    and int(exp["candidate_office_district"]) != 0
                ):
                    race += "-" + exp["candidate_office_district"]

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
                        "expenditures": [pick(exp, EXPENDITURE_FIELDS)],
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
                    ].append(pick(exp, EXPENDITURE_FIELDS))

                if race not in states[exp["candidate_office_state"]]["by_race"]:
                    states[exp["candidate_office_state"]]["by_race"][race] = {
                        "total": amount,
                        "details": {
                            "candidate_office": exp["candidate_office"],
                            "candidate_office_district": exp[
                                "candidate_office_district"
                            ],
                        },
                        "expenditures": [pick(exp, EXPENDITURE_FIELDS)],
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
                    ].append(pick(exp, EXPENDITURE_FIELDS))

            if exp_count >= data["pagination"]["count"]:
                break
            else:
                last_index = data["pagination"]["last_indexes"]["last_index"]
                last_expenditure_date = data["pagination"]["last_indexes"][
                    "last_expenditure_date"
                ]

    for state, state_expenditures in states.items():
        db.client.collection("expendituresByState").document(state).set(
            state_expenditures
        )
