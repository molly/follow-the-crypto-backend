import logging
import re
from utils import FEC_fetch
from states import SINGLE_MEMBER_STATES

RACE_PRIORITY = {
    "general": 0,
    "primary_runoff": 1,
    "primary": 2,
    "convention": 3,
    None: 4,
}


def trim_name(name):
    m = re.match(r"^(.+)(\s(?:[SJ]r\.?|IX|IV|V?I{0,3}))$", name)
    if m:
        return m.group(1).split(" ")[-1]
    return name.split(" ")[-1]


def sort_candidates(candidates):
    # 1. If defeated: false, sort by party, putting "DEM" or "REP" ahead of any third parties or undefined.
    # 2. If defeated: true, sort by defeated_race by RACE_PRIORITY
    # 3. If two defeated candidates have the same defeated_race, sort by whichever candidate has the higher
    #    support_total + oppose_total
    def sort_key(candidate):
        name, data = candidate
        defeated = data.get("defeated", False)
        party = data.get("party", [])
        defeated_race = data.get("defeated_race")
        total_support = data.get("support_total", 0) + data.get("oppose_total", 0)

        if not defeated:
            party_order = {"D": 0, "R": 1}.get(party[0] if party else "", 2)
            return 0, party_order, 0
        else:
            race_order = RACE_PRIORITY.get(defeated_race, 4)
            return 1, race_order, -total_support

    sorted_candidates = sorted(candidates.items(), key=sort_key)
    return [name for name, _ in sorted_candidates]


def get_last_index_with_donation(sorted_candidates, candidates_data):
    for i in range(len(sorted_candidates) - 1, -1, -1):
        candidate_name = sorted_candidates[i]
        candidate = candidates_data[candidate_name]
        if (
            candidate.get("support_total", 0) != 0
            or candidate.get("oppose_total", 0) != 0
            or candidate.get("defeated", False) is False
        ):
            return i
    return -1


def get_expenditure_race_type(expenditure):
    election_type = expenditure.get("election_type", "")[0]
    if election_type == "G":
        return "general"
    if election_type == "P":
        return "primary"
    if election_type == "R":
        return "primary_runoff"
    if election_type == "C":
        return "convention"
    if election_type == "S":
        return "special"
    return None


def summarize_races(db):
    race_docs = db.client.collection("raceDetails").stream()
    for doc in race_docs:
        state, state_data = doc.id, doc.to_dict()
        all_expenditures = (
            db.client.collection("expendituresByState").document(state).get().to_dict()
        )
        for race_id, race_data in state_data.items():
            race_id_split = race_id.split("-")

            # Get set of candidate names
            candidates = {
                candidate["name"]
                for race in race_data["races"]
                for candidate in race["candidates"]
            }
            candidates_data = {
                candidate: {
                    "common_name": candidate,
                    "support_total": 0,
                    "oppose_total": 0,
                    "races": [],
                    "defeated_race": None,
                }
                for candidate in candidates
            }
            if "withdrew" in race_data:
                withdrawn_candidates = {
                    candidate for candidate in race_data["withdrew"].keys()
                }
                candidates = candidates.union(withdrawn_candidates)
                for candidate in withdrawn_candidates:
                    candidates_data[candidate] = {
                        "common_name": candidate,
                        "withdrew": True,
                        "support_total": 0,
                        "oppose_total": 0,
                        "races": [],
                    }
                    if "party" in race_data["withdrew"][candidate]:
                        candidates_data[candidate]["party"] = race_data["withdrew"][
                            candidate
                        ]["party"]

            # Get candidate data from FEC
            params = {
                "office": race_id_split[0],
                "state": state,
                "election_year": 2024,
                "q": map(trim_name, candidates),
            }
            if (
                race_id_split[0] == "H"
                and len(race_id_split) > 1
                and state not in SINGLE_MEMBER_STATES
            ):
                params["district"] = race_id_split[1]

            FEC_candidates_data = FEC_fetch(
                f"candidates data for {state}",
                "https://api.open.fec.gov/v1/candidates/search",
                params,
            )

            names = {}
            for FEC_candidate_data in FEC_candidates_data["results"]:
                split_name = FEC_candidate_data["name"].split(", ")
                last_name = split_name[0]
                first_name = split_name[1].split(" ")[0]
                candidate = {name for name in candidates if last_name in name.upper()}
                if len(candidate) == 1:
                    candidate_race_name = candidate.pop()
                else:
                    candidate = {
                        name for name in candidate if first_name in name.upper()
                    }
                    if len(candidate) == 1:
                        candidate_race_name = candidate.pop()
                    else:
                        logging.error(
                            f"Having trouble locating candidate: {first_name} {last_name} in {state} {race_id}"
                        )
                        continue
                names[FEC_candidate_data["name"]] = candidate_race_name
                candidates_data[candidate_race_name]["candidate_id"] = (
                    FEC_candidate_data["candidate_id"],
                )
                candidates_data[candidate_race_name]["party"] = (
                    FEC_candidate_data["party"][0],
                )
                candidates_data[candidate_race_name][
                    "incumbent_challenge"
                ] = FEC_candidate_data["incumbent_challenge"]
                candidates_data[candidate_race_name]["FEC_name"] = FEC_candidate_data[
                    "name"
                ]

            for ind, race in enumerate(race_data["races"]):
                for candidate in race["candidates"]:
                    candidates_data[candidate["name"]]["races"].append(race["type"])
                    if ind == 0 or (
                        "won" in candidate
                        and candidate["won"] is True
                        and "defeated" not in candidates_data[candidate["name"]]
                    ):
                        candidates_data[candidate["name"]]["defeated"] = False
                    elif (
                        "won" in candidate
                        and candidate["won"] is False
                        and "defeated" not in candidates_data[candidate["name"]]
                    ):
                        candidates_data[candidate["name"]]["defeated"] = True
                        if candidates_data[candidate["name"]]["defeated_race"] is None:
                            candidates_data[candidate["name"]]["defeated_race"] = race[
                                "type"
                            ]
                    if "withdrew" in candidate and candidate["withdrew"]:
                        candidates_data[candidate["name"]]["withdrew"] = True

            expenditures = all_expenditures["by_race"][f"{state}-{race_id}"][
                "expenditures"
            ]
            for expenditure in expenditures:
                try:
                    candidate_key = names[expenditure["candidate_name"]]
                except KeyError:
                    k = next(
                        (
                            key
                            for key in names
                            if expenditure["candidate_last_name"] in key
                        ),
                        None,
                    )
                    if k is None:
                        logging.error(
                            f"Having trouble locating candidate: {expenditure['candidate_name']} in {state} {race_id}"
                        )
                        continue
                    else:
                        candidate_key = names[k]

                if "expenditure_races" not in candidates_data[candidate_key]:
                    candidates_data[candidate_key]["expenditure_races"] = set()
                if "expenditure_committees" not in candidates_data[candidate_key]:
                    candidates_data[candidate_key]["expenditure_committees"] = set()
                candidates_data[candidate_key]["expenditure_races"].add(
                    get_expenditure_race_type(expenditure)
                )
                candidates_data[candidate_key]["expenditure_committees"].add(
                    expenditure["committee_id"]
                )
                if expenditure["support_oppose_indicator"] == "S":
                    candidates_data[candidate_key]["support_total"] = round(
                        candidates_data[candidate_key]["support_total"]
                        + expenditure["expenditure_amount"],
                        2,
                    )
                elif expenditure["support_oppose_indicator"] == "O":
                    candidates_data[candidate_key]["oppose_total"] = round(
                        candidates_data[candidate_key]["oppose_total"]
                        + expenditure["expenditure_amount"],
                        2,
                    )

            # There can be a lot of withdrawn candidates, so remove those who weren't involved in any expenditures
            withdrawn_candidates = (
                list(race_data["withdrew"].keys()) if "withdrew" in race_data else []
            )
            for candidate in withdrawn_candidates:
                if (
                    candidates_data[candidate]["support_total"] == 0
                    and candidates_data[candidate]["oppose_total"] == 0
                ):
                    del candidates_data[candidate]
                    del race_data["withdrew"][candidate]

            sorted_candidates = sort_candidates(candidates_data)
            last_index_with_donation = get_last_index_with_donation(
                sorted_candidates, candidates_data
            )
            if last_index_with_donation > -1:
                sorted_candidates = sorted_candidates[: last_index_with_donation + 1]

            updated_data = {
                db.client.field_path(race_id, "candidates"): candidates_data,
                db.client.field_path(race_id, "candidatesOrder"): sorted_candidates,
            }
            if "withdrew" in race_data:
                updated_data[db.client.field_path(race_id, "withdrew")] = race_data[
                    "withdrew"
                ]
            db.client.collection("raceDetails").document(state).update(updated_data)
