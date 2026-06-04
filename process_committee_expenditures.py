from states import SPECIAL_ELECTIONS, CURRENT_CYCLE
from utils import get_sector_keys


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
    entry = SPECIAL_ELECTIONS.get(race)
    if entry:
        election_type = expenditure.get("election_type") or ""
        # A current-cycle special-only seat has no regular contest, so every
        # expenditure there is the special regardless of how the filer coded
        # election_type. For seats that also hold a regular election (or whose
        # special is historical), trust the FEC election_type: "S..." means
        # "special". Gating on SPECIAL_ELECTIONS membership avoids creating
        # spurious special entries from misfiled election_types elsewhere.
        if entry["year"] == CURRENT_CYCLE and not entry["has_regular"]:
            race += "-special"
        elif election_type.startswith("S"):
            race += "-special"
    return race


def process_expenditures(db):
    all_expenditures = (
        db.client.collection("expenditures").document("all").get().to_dict()
    )
    states = {}
    new_opposition_spending = set()

    def empty_parties():
        return {
            "dem_oppose": 0,
            "dem_support": 0,
            "rep_oppose": 0,
            "rep_support": 0,
            "oppose_benefit_dem": 0,
            "oppose_benefit_rep": 0,
            "oppose_benefit_mix": 0,
            "oppose_benefit_unk": 0,
        }

    all_parties = {
        "all": empty_parties(),
        "crypto": empty_parties(),
        "ai": empty_parties(),
    }
    committees = {}

    def empty_expenditure_totals():
        return {"total": 0, "by_committee": {}}

    totals = {
        "all": empty_expenditure_totals(),
        "crypto": empty_expenditure_totals(),
        "ai": empty_expenditure_totals(),
    }
    for uid, expenditure in all_expenditures.items():
        if not expenditure["expenditure_amount"]:
            continue
        race = get_race_name(expenditure)
        committee_id = expenditure["committee_id"]
        state = expenditure["candidate_office_state"]
        if state is None:
            state = "US"

        committee_sector = db.committees.get(committee_id, {}).get("sector")
        sector_keys = get_sector_keys(committee_sector)

        for key in sector_keys:
            totals[key]["total"] += expenditure["expenditure_amount"]
            if committee_id not in totals[key]["by_committee"]:
                totals[key]["by_committee"][committee_id] = expenditure["expenditure_amount"]
            else:
                totals[key]["by_committee"][committee_id] += expenditure["expenditure_amount"]

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

        # Record party support/oppose for all committees, and per-committee
        if committee_id not in committees:
            committees[committee_id] = {
                "dem_support": 0,
                "dem_oppose": 0,
                "rep_support": 0,
                "rep_oppose": 0,
                "oppose_benefit_dem": 0,
                "oppose_benefit_rep": 0,
                "oppose_benefit_mix": 0,
                "oppose_benefit_unk": 0,
            }
        if expenditure["support_oppose_indicator"] == "S":
            if expenditure["candidate_party"] == "DEM":
                committees[committee_id]["dem_support"] += expenditure[
                    "expenditure_amount"
                ]
                for key in sector_keys:
                    all_parties[key]["dem_support"] += expenditure["expenditure_amount"]
            elif expenditure["candidate_party"] == "REP":
                committees[committee_id]["rep_support"] += expenditure[
                    "expenditure_amount"
                ]
                for key in sector_keys:
                    all_parties[key]["rep_support"] += expenditure["expenditure_amount"]
        elif expenditure["support_oppose_indicator"] == "O":
            if expenditure["candidate_party"] == "DEM":
                committees[committee_id]["dem_oppose"] += expenditure[
                    "expenditure_amount"
                ]
                for key in sector_keys:
                    all_parties[key]["dem_oppose"] += expenditure["expenditure_amount"]
            elif expenditure["candidate_party"] == "REP":
                committees[committee_id]["rep_oppose"] += expenditure[
                    "expenditure_amount"
                ]
                for key in sector_keys:
                    all_parties[key]["rep_oppose"] += expenditure["expenditure_amount"]
            if expenditure["candidate_id"] in db.opposition_spending:
                party = db.opposition_spending[expenditure["candidate_id"]][
                    "benefitsParty"
                ]
                if party == "DEM":
                    committees[committee_id]["oppose_benefit_dem"] += expenditure[
                        "expenditure_amount"
                    ]
                    for key in sector_keys:
                        all_parties[key]["oppose_benefit_dem"] += expenditure[
                            "expenditure_amount"
                        ]
                elif party == "REP":
                    committees[committee_id]["oppose_benefit_rep"] += expenditure[
                        "expenditure_amount"
                    ]
                    for key in sector_keys:
                        all_parties[key]["oppose_benefit_rep"] += expenditure[
                            "expenditure_amount"
                        ]
                elif party == "MIX":
                    committees[committee_id]["oppose_benefit_mix"] += expenditure[
                        "expenditure_amount"
                    ]
                    for key in sector_keys:
                        all_parties[key]["oppose_benefit_mix"] += expenditure[
                            "expenditure_amount"
                        ]
            else:
                committees[committee_id]["oppose_benefit_unk"] += expenditure[
                    "expenditure_amount"
                ]
                for key in sector_keys:
                    all_parties[key]["oppose_benefit_unk"] += expenditure[
                        "expenditure_amount"
                    ]
                new_opposition_spending.add(expenditure["candidate_id"])

    db.client.collection("expenditures").document("states").set(states)
    for committee_id, committee_data in committees.items():
        db.client.collection("committees").document(committee_id).set(
            {"by_party": committee_data}, merge=True
        )
    db.client.collection("expenditures").document("total").set(totals)

    # Get most recent for committee, all, and by sector
    most_recent_all = [x["uid"] for x in sort_and_slice(all_expenditures.values(), 50)]
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

    crypto_committee_ids_set = {
        cid for cid, c in db.committees.items()
        if c.get("sector") in ("crypto", "tech")
    }
    ai_committee_ids_set = {
        cid for cid, c in db.committees.items()
        if c.get("sector") in ("ai", "tech")
    }
    most_recent_crypto = [
        x["uid"]
        for x in sort_and_slice(
            [
                e
                for e in all_expenditures.values()
                if e["committee_id"] in crypto_committee_ids_set
            ],
            50,
        )
    ]
    most_recent_ai = [
        x["uid"]
        for x in sort_and_slice(
            [
                e
                for e in all_expenditures.values()
                if e["committee_id"] in ai_committee_ids_set
            ],
            50,
        )
    ]

    db.client.collection("expenditures").document("recent").set(
        {
            "all": {
                "all": most_recent_all,
                "by_committee": most_recent_by_committee,
            },
            "crypto": {
                "all": most_recent_crypto,
                "by_committee": {
                    k: v
                    for k, v in most_recent_by_committee.items()
                    if k in crypto_committee_ids_set
                },
            },
            "ai": {
                "all": most_recent_ai,
                "by_committee": {
                    k: v
                    for k, v in most_recent_by_committee.items()
                    if k in ai_committee_ids_set
                },
            },
        }
    )
    db.client.collection("expenditures").document("by_party").set(all_parties)
    return new_opposition_spending
