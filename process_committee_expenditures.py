import datetime


def sort_and_slice(lst, length=10):
    def get_date(x):
        date = x.get("expenditure_date")
        if not date:
            date = x.get("dissemination_date")
        return date if date else "0"

    return sorted(
        lst,
        key=get_date,
        reverse=True,
    )[:length]


def get_race_name(expenditure):
    race = "{candidate_office_state}-{candidate_office}".format(**expenditure)
    if (
        expenditure["candidate_office_district"]
        and int(expenditure["candidate_office_district"]) != 0
    ):
        race += "-" + expenditure["candidate_office_district"]
    return race


def process_expenditures(db):
    all_expenditures = (
        db.client.collection("expenditures").document("all").get().to_dict()
    )
    states = {}
    for uid, expenditure in all_expenditures.items():
        race = get_race_name(expenditure)
        committee_id = expenditure["committee_id"]
        state = expenditure["candidate_office_state"]

        # Initialize state and set total
        if state not in states:
            states[state] = {
                "total": expenditure["expenditure_amount"],
                "by_committee": {},
                "by_race": {},
            }
        else:
            states[state]["total"] = round(
                states[state]["total"] + expenditure["expenditure_amount"], 2
            )

        # Initialize committee and record
        if committee_id not in states[state]["by_committee"]:
            states[state]["by_committee"][committee_id] = {
                "total": expenditure["expenditure_amount"],
                "expenditures": [uid],
            }
        else:
            states[state]["by_committee"][committee_id]["total"] = round(
                states[state]["by_committee"][committee_id]["total"]
                + expenditure["expenditure_amount"],
                2,
            )
            states[state]["by_committee"][committee_id]["expenditures"].append(uid)

        # Initialize race and record
        if race not in states[state]["by_race"]:
            states[state]["by_race"][race] = {
                "total": expenditure["expenditure_amount"],
                "details": {
                    "candidate_office": expenditure["candidate_office"],
                    "candidate_office_district": expenditure[
                        "candidate_office_district"
                    ],
                },
                "expenditures": [uid],
            }
        else:
            states[state]["by_race"][race]["total"] = round(
                states[state]["by_race"][race]["total"]
                + expenditure["expenditure_amount"],
                2,
            )
            states[state]["by_race"][race]["expenditures"].append(uid)
    db.client.collection("expenditures").document("states").set(states)

    # Get most recent for committee, all
    most_recent_all = [x["uid"] for x in sort_and_slice(all_expenditures.values())]
    most_recent_by_committee = {}

    committee_ids = [committee["id"] for committee in db.committees.values()]
    for committee_id in committee_ids:
        most_recent_by_committee[committee_id] = [
            x["uid"]
            for x in sort_and_slice(
                filter(
                    lambda x: x["committee_id"] == committee_id,
                    all_expenditures.values(),
                )
            )
        ]
    db.client.collection("expenditures").document("recent").set(
        {
            "all": most_recent_all,
            "by_committee": most_recent_by_committee,
        }
    )
