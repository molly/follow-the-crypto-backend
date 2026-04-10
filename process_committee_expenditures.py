from states import SPECIAL_ELECTIONS


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


def get_race_name(expenditure, race_exists=None, candidate_to_race=None):
    """Returns (race_name, resolved_as_special) where resolved_as_special is True
    when a missing election_type was inferred to be a special election via raceDetails."""
    race = "{candidate_office_state}-{candidate_office}".format(**expenditure)
    if (
        expenditure["candidate_office_district"]
        and int(expenditure["candidate_office_district"]) != 0
    ):
        race += "-" + expenditure["candidate_office_district"]
    election_type = expenditure.get("election_type") or ""
    # Only append "-special" when the race is a known special election.
    # election_type "S..." means "special" in FEC data, but filers sometimes use
    # it for regular-cycle candidates, which would create spurious special-election
    # entries.  Gating on SPECIAL_ELECTIONS prevents that.
    if election_type.startswith("S") and race in SPECIAL_ELECTIONS:
        race += "-special"
        return race, False
    elif not election_type and race in SPECIAL_ELECTIONS and race_exists is not None:
        # For efiled expenditures with no election_type, resolve using raceDetails:
        # if only one of the regular/special race exists, use that; if both exist,
        # check which race the candidate belongs to.
        special_race = race + "-special"
        base_exists = race in race_exists
        special_exists = special_race in race_exists
        if special_exists and not base_exists:
            return special_race, True
        elif base_exists and special_exists and candidate_to_race is not None:
            candidate_id = expenditure.get("candidate_id")
            candidate_races = candidate_to_race.get(candidate_id, [])
            if special_race in candidate_races and race not in candidate_races:
                return special_race, True
    return race, False


def process_expenditures(db):
    all_expenditures = (
        db.client.collection("expenditures").document("all").get().to_dict()
    )

    # Build lookups for resolving efiled expenditures with no election_type.
    # race_exists: set of full race IDs (e.g. "GA-H-14-special") present in raceDetails.
    # candidate_to_race: candidate_id → list of full race IDs they appear in.
    race_exists = set()
    candidate_to_race = {}
    for state_doc in db.client.collection("raceDetails").stream():
        state = state_doc.id
        for race_id, race_data in state_doc.to_dict().items():
            full_race_id = f"{state}-{race_id}"
            race_exists.add(full_race_id)
            for candidate in race_data.get("candidates", {}).values():
                cid = candidate.get("candidate_id")
                if cid:
                    if cid not in candidate_to_race:
                        candidate_to_race[cid] = []
                    candidate_to_race[cid].append(full_race_id)

    states = {}
    resolved_as_special = set()
    new_opposition_spending = set()
    all_parties = {
        "dem_oppose": 0,
        "dem_support": 0,
        "rep_oppose": 0,
        "rep_support": 0,
        "oppose_benefit_dem": 0,
        "oppose_benefit_rep": 0,
        "oppose_benefit_mix": 0,  # Both parties benefit from opposing
        "oppose_benefit_unk": 0,  # Unknown who benefits from opposing
    }
    committees = {}
    totals = {
        "all": 0,
        "by_committee": {},
    }
    for uid, expenditure in all_expenditures.items():
        race, is_resolved_special = get_race_name(expenditure, race_exists, candidate_to_race)
        if is_resolved_special:
            resolved_as_special.add(uid)
        committee_id = expenditure["committee_id"]
        state = expenditure["candidate_office_state"]
        if state is None:
            state = "US"

        totals["all"] += expenditure["expenditure_amount"]
        if committee_id not in totals["by_committee"]:
            totals["by_committee"][committee_id] = expenditure["expenditure_amount"]
        else:
            totals["by_committee"][committee_id] += expenditure["expenditure_amount"]

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
                all_parties["dem_support"] += expenditure["expenditure_amount"]
            elif expenditure["candidate_party"] == "REP":
                committees[committee_id]["rep_support"] += expenditure[
                    "expenditure_amount"
                ]
                all_parties["rep_support"] += expenditure["expenditure_amount"]
        elif expenditure["support_oppose_indicator"] == "O":
            if expenditure["candidate_party"] == "DEM":
                committees[committee_id]["dem_oppose"] += expenditure[
                    "expenditure_amount"
                ]
                all_parties["dem_oppose"] += expenditure["expenditure_amount"]
            elif expenditure["candidate_party"] == "REP":
                committees[committee_id]["rep_oppose"] += expenditure[
                    "expenditure_amount"
                ]
                all_parties["rep_oppose"] += expenditure["expenditure_amount"]
            if expenditure["candidate_id"] in db.opposition_spending:
                party = db.opposition_spending[expenditure["candidate_id"]][
                    "benefitsParty"
                ]
                if party == "DEM":
                    committees[committee_id]["oppose_benefit_dem"] += expenditure[
                        "expenditure_amount"
                    ]
                    all_parties["oppose_benefit_dem"] += expenditure[
                        "expenditure_amount"
                    ]
                elif party == "REP":
                    committees[committee_id]["oppose_benefit_rep"] += expenditure[
                        "expenditure_amount"
                    ]
                    all_parties["oppose_benefit_rep"] += expenditure[
                        "expenditure_amount"
                    ]
                elif party == "MIX":
                    committees[committee_id]["oppose_benefit_mix"] += expenditure[
                        "expenditure_amount"
                    ]
                    all_parties["oppose_benefit_mix"] += expenditure[
                        "expenditure_amount"
                    ]
            else:
                committees[committee_id]["oppose_benefit_unk"] += expenditure[
                    "expenditure_amount"
                ]
                all_parties["oppose_benefit_unk"] += expenditure["expenditure_amount"]
                new_opposition_spending.add(expenditure["candidate_id"])

    db.client.collection("expenditures").document("states").set(states)
    for committee_id, committee_data in committees.items():
        db.client.collection("committees").document(committee_id).set(
            {"by_party": committee_data}, merge=True
        )
    db.client.collection("expenditures").document("total").set(totals)

    # Get most recent for committee, all
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
    db.client.collection("expenditures").document("recent").set(
        {
            "all": most_recent_all,
            "by_committee": most_recent_by_committee,
        }
    )
    db.client.collection("expenditures").document("by_party").set(all_parties)

    # Mark expenditures we inferred as special-election so the frontend can
    # construct the correct race ID (e.g. GA-H-14-special) even when subrace
    # is a sub-type like "general_runoff".
    if resolved_as_special:
        for uid in resolved_as_special:
            db.client.collection("expenditures").document("all").update(
                {db.client.field_path(uid, "is_special"): True}
            )

    return new_opposition_spending
