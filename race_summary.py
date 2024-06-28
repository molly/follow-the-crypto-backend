from datetime import date, timedelta
import logging
import re
from utils import FEC_fetch
from states import SINGLE_MEMBER_STATES
from unidecode import unidecode

RACE_PRIORITY = {
    "general": 0,
    "primary_runoff": 1,
    "primary": 2,
    "convention": 3,
    None: 4,
}


def compare_names(name_portion, name):
    upper_name = unidecode(name).upper()
    if name_portion.upper() in upper_name:
        return True
    return False


def trim_name(name):
    m = re.match(r"^(.+)(\s(?:[SJ]r\.?|IX|IV|V?I{0,3}))$", name)
    if m:
        last_name = m.group(1).split(" ")[-1]
    else:
        last_name = unidecode(name.split(" ")[-1])
    if len(last_name) < 3:
        # FEC API won't accept queries of < 3 characters, so short names like "Xu" throw errors
        return unidecode(name)
    return unidecode(last_name)


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
        if candidate_name in candidates_data:
            candidate = candidates_data[candidate_name]
            if (
                candidate.get("support_total", 0) != 0
                or candidate.get("oppose_total", 0) != 0
                or candidate.get("defeated", False) is False
            ):
                return i
    return -1


def get_expenditure_race_type(expenditure):
    election_type = expenditure.get("election_type", None)
    if election_type is None:
        return None
    else:
        election_type = election_type[0]
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

        # Iterate through each race in each state
        for race_id, race_data in state_data.items():
            race_id_split = race_id.split("-")

            # Create set for each unique candidate in any sub-race in this race. This will always be equivalent to
            # Object.keys(candidates_data) and is just maintained for convenience.
            candidates = {
                candidate["name"]
                for race in race_data["races"]
                for candidate in race["candidates"]
            }

            # Create dict with an entry for each candidate. This dict will eventually be saved to the "candidates" field
            # in the race entry.
            candidates_data = {
                candidate: {
                    "common_name": candidate,
                    "support_total": 0,
                    "oppose_total": 0,
                    "races": [],  # Sub-races in which this person was a candidate
                    "defeated_race": None,  # Race in which this candidate was defeated
                }
                for candidate in candidates
            }

            # Add withdrawn candidates to this set and dict
            if "withdrew" in race_data:
                withdrawn_candidates = {
                    candidate for candidate in race_data["withdrew"].keys()
                }
                candidates = candidates.union(withdrawn_candidates)
                for candidate in withdrawn_candidates:
                    candidates_data[candidate] = {
                        "common_name": candidate,
                        "support_total": 0,
                        "oppose_total": 0,
                        "races": [],
                        "withdrew": True,
                        "withdrew_race": None,  # Race from which candidate withdrew
                    }

            # Try to get candidate data from FEC
            params = {
                "office": race_id_split[0],
                "state": state,
                "election_year": 2024,
                "q": map(trim_name, candidates),
                "per_page": 50,
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

            # Map FEC candidate names to formatted candidate names (which are being used as keys)
            names = {}
            # Add relevant FEC data to candidate data
            for FEC_candidate_data in FEC_candidates_data["results"]:
                # Try to match FEC candidate result to candidate in our data
                split_name = FEC_candidate_data["name"].split(", ")
                last_name = split_name[0]
                first_name = split_name[1].split(" ")[0]

                # Get the common name for this candidate
                candidate_race_name = None
                candidate = {
                    name for name in candidates if compare_names(last_name, name)
                }
                if len(candidate) == 1:
                    candidate_race_name = candidate.pop()
                elif len(candidate) > 1:
                    # Sometimes there are multiple candidates with the same surname, in which case we compare first
                    # names.
                    candidate = {
                        name for name in candidate if compare_names(first_name, name)
                    }
                    if len(candidate) == 1:
                        candidate_race_name = candidate.pop()
                if candidate_race_name is None:
                    # There weren't any matching candidates, or there were still multiple candidates in the results
                    # This is not ALWAYS an error — there can be results from the FEC API for candidates we're not
                    # interested in (and who therefore aren't represented in the candidates set).
                    logging.debug(
                        f"Having trouble locating FEC candidate in candidates data: {first_name} {last_name} in {state} {race_id}"
                    )
                    continue

                # Map FEC name to common name
                names[FEC_candidate_data["name"]] = candidate_race_name

                # Add FEC data to candidate data map
                candidates_data[candidate_race_name][
                    "candidate_id"
                ] = FEC_candidate_data["candidate_id"]
                candidates_data[candidate_race_name]["party"] = FEC_candidate_data[
                    "party"
                ][0]
                candidates_data[candidate_race_name][
                    "incumbent_challenge"
                ] = FEC_candidate_data["incumbent_challenge"]
                candidates_data[candidate_race_name]["FEC_name"] = FEC_candidate_data[
                    "name"
                ]

            for entry in candidates_data.values():
                if "candidate_id" not in entry:
                    if entry["common_name"] in db.candidates:
                        # A few weird edge cases are in the candidates constant, get that data here
                        c_id = db.candidates[entry["common_name"]]
                        FEC_candidates_data = FEC_fetch(
                            f"candidates data for {state}",
                            "https://api.open.fec.gov/v1/candidates/search",
                            {"candidate_id": [c_id]},
                        )
                        FEC_candidate_data = FEC_candidates_data["results"][0]
                        names[FEC_candidate_data["name"]] = entry["common_name"]
                        candidates_data[entry["common_name"]][
                            "candidate_id"
                        ] = FEC_candidate_data["candidate_id"]

                        candidates_data[entry["common_name"]]["party"] = (
                            FEC_candidate_data["party"][0],
                        )

                        candidates_data[entry["common_name"]][
                            "incumbent_challenge"
                        ] = FEC_candidate_data["incumbent_challenge"]
                        candidates_data[entry["common_name"]][
                            "FEC_name"
                        ] = FEC_candidate_data["name"]
                    else:
                        print(
                            f"Having trouble locating FEC candidate: {entry['common_name']} in {state}-{race_id}"
                        )
                        logging.error(
                            f"Having trouble locating FEC candidate: {entry['common_name']} in {state}-{race_id}"
                        )

            # Iterate through each subrace
            for ind, race in enumerate(race_data["races"]):
                is_upcoming = None
                has_winner = any("won" in candidate for candidate in race["candidates"])
                if "date" in race and race["date"]:
                    race_date = date.fromisoformat(race["date"])
                    if race_date > date.today():
                        # Race is happening sometime in the future.
                        is_upcoming = True
                    elif (
                        race_date > (date.today() - timedelta(days=7))
                        and not has_winner
                    ):
                        # Race happened within the last week, but outcome has not been announced.
                        is_upcoming = True
                    else:
                        is_upcoming = False

                # Iterate through each candidate in the subrace. These should generally be in reverse chrono order.
                for candidate in race["candidates"]:
                    # Add this subrace to their list of involved races
                    candidates_data[candidate["name"]]["races"].append(race["type"])
                    if is_upcoming is True or (
                        "won" in candidate
                        and candidate["won"] is True
                        and "defeated" not in candidates_data[candidate["name"]]
                    ):
                        # If the candidate is involved in a race that's still upcoming
                        #   OR this is the most recent race, and they won it, mark them as not defeated
                        candidates_data[candidate["name"]]["defeated"] = False
                    elif (
                        "won" in candidate
                        and candidate["won"] is False
                        and "defeated" not in candidates_data[candidate["name"]]
                    ):
                        # If this race already happened
                        #   AND the candidate is not listed in a more recent or upcoming race*
                        #   AND candidate has the "won" field set to False for this race, mark them as defeated
                        #
                        # * It is possible for a candidate to lose a race and still be listed in a more recent or
                        #   upcoming race, for example as a write-in or because they secured enough signatures
                        #   despite not progressing through the convention vote.
                        candidates_data[candidate["name"]]["defeated"] = True

                        # If they lost, and they do not already have a race listed in their defeated races list,
                        # mark this as the defeated race because it was the most advanced race in which they
                        # participated.
                        if "defeated_race" not in candidates_data[
                            candidate["name"]
                        ] or (
                            candidates_data[candidate["name"]]["defeated_race"] is None
                        ):
                            candidates_data[candidate["name"]]["defeated_race"] = race[
                                "type"
                            ]

            expenditures = all_expenditures["by_race"][f"{state}-{race_id}"][
                "expenditures"
            ]
            # Iterate through each expenditure in this race
            for expenditure in expenditures:
                # Try to find the candidate this expenditure is associated with
                try:
                    # Ideally this will match their FEC_name
                    candidate_key = names[expenditure["candidate_name"]]
                except KeyError:
                    # If it doesn't, try to find the candidate with a matching last name
                    k = None
                    ks = {
                        key
                        for key in names
                        if expenditure["candidate_last_name"] in key
                    }
                    if len(ks) == 1:
                        k = ks.pop()
                    if k is None:
                        # TODO: We're going to have to figure out something else if we end up here.
                        print(
                            f"Having trouble locating candidate named in expenditure: {expenditure['candidate_name']} in {state} {race_id}"
                        )
                        logging.error(
                            f"Having trouble locating candidate named in expenditure: {expenditure['candidate_name']} in {state} {race_id}"
                        )
                        continue
                    else:
                        candidate_key = names[k]

                # Initialize fields if necessary
                if "expenditure_races" not in candidates_data[candidate_key]:
                    candidates_data[candidate_key]["expenditure_races"] = set()
                if "expenditure_committees" not in candidates_data[candidate_key]:
                    candidates_data[candidate_key]["expenditure_committees"] = set()

                # Add the expenditure's sub-race to the candidate's list of expenditure_races
                candidates_data[candidate_key]["expenditure_races"].add(
                    get_expenditure_race_type(expenditure)
                )
                # Add this expenditure's committee to the candidate's list of expenditure_committees
                candidates_data[candidate_key]["expenditure_committees"].add(
                    expenditure["committee_id"]
                )

                # Add expenditure to total support/oppose amount
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

            withdrawn_candidates = (
                list(race_data["withdrew"].keys()) if "withdrew" in race_data else []
            )
            for candidate in withdrawn_candidates:
                if (
                    candidates_data[candidate]["support_total"] == 0
                    and candidates_data[candidate]["oppose_total"] == 0
                ):
                    # There can be a lot of withdrawn candidates, so only keep those involved in some expenditure
                    del candidates_data[candidate]
                    del race_data["withdrew"][candidate]
                else:
                    # However, if they were involved, we need to add them to the list of candidates in the race from
                    # which they withdrew.
                    if "withdrew_race" in race_data["withdrew"][candidate]:
                        candidate_details = race_data["withdrew"][candidate]
                        matching = next(
                            (
                                i
                                for i, race in enumerate(race_data["races"])
                                if (
                                    (
                                        race["type"]
                                        == candidate_details["withdrew_race"]["type"]
                                    )
                                    and (
                                        race["party"]
                                        == candidate_details["withdrew_race"]["party"]
                                    )
                                )
                            ),
                            None,
                        )
                        if matching is not None:
                            race_data["races"][matching]["candidates"].append(
                                candidate_details
                            )

            # Get total raised for each candidate
            candidate_ids = [
                c["candidate_id"]
                for c in candidates_data.values()
                if "candidate_id" in c
            ]
            FEC_totals_data = FEC_fetch(
                "candidate totals",
                "https://api.open.fec.gov/v1/candidates/totals",
                {
                    "cycle": 2024,
                    "per_page": 50,
                    "candidate_id": candidate_ids,
                },
            )
            for total_result in FEC_totals_data["results"]:
                FEC_name = total_result["name"]
                if FEC_name not in names:
                    # This candidate was not found in the list of candidates for this
                    continue
                candidate_key = names[FEC_name]
                candidates_data[candidate_key]["raised_total"] = total_result[
                    "receipts"
                ]
                candidates_data[candidate_key]["spent_total"] = total_result[
                    "disbursements"
                ]

            updated_data = {
                db.client.field_path(race_id, "candidates"): candidates_data,
            }
            if "withdrew" in race_data:
                updated_data[db.client.field_path(race_id, "withdrew")] = race_data[
                    "withdrew"
                ]
                updated_data[db.client.field_path(race_id, "races")] = race_data[
                    "races"
                ]
            db.client.collection("raceDetails").document(state).update(updated_data)
