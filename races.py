import logging
from utils import FEC_fetch


def update_race_details(db):
    docs = db.client.collection("expendituresByState").stream()
    race_data = {}
    races = set()
    for doc in docs:
        state_data = doc.to_dict()
        for race_id in state_data["by_race"].keys():
            races.add(race_id)

    for race in races:
        race_parts = race.split("-")
        election_name = "-".join(race_parts[1:])

        if race_parts[0] not in race_data:
            race_data[race_parts[0]] = {election_name: {"candidates": [], "dates": []}}
        if election_name not in race_data[race_parts[0]]:
            race_data[race_parts[0]][election_name] = {"candidates": [], "dates": []}

        candidate_params = {
            "cycle": 2024,
            "state": race_parts[0],
            "sort": "-total_receipts",
            "per_page": 10,
        }
        if race_parts[1] == "S":
            candidate_params["office"] = "senate"
        if race_parts[1] == "H":
            candidate_params["office"] = "house"
            candidate_params["district"] = race_parts[2]

        candidate_data = FEC_fetch(
            "candidate details",
            "https://api.open.fec.gov/v1/elections",
            params=candidate_params,
        )
        if not candidate_data:
            continue

        data_to_include = candidate_data["results"][:2]
        for candidate in candidate_data["results"][2:]:
            if candidate["total_receipts"] > 1000000:
                data_to_include.append(candidate)
        race_data[race_parts[0]][election_name]["candidates"] = data_to_include

        dates_params = {
            "election_state": race_parts[0],
            "election_year": 2024,
            "office_sought": race_parts[1],
            "sort": "-election_date",
        }
        if race_parts[1] == "H":
            dates_params["district"] = race_parts[2]

        dates_data = FEC_fetch(
            "election date details",
            "https://api.open.fec.gov/v1/election-dates",
            params=dates_params,
        )
        if dates_data:
            race_data[race_parts[0]][election_name]["dates"] = dates_data["results"]

    for state, state_data in race_data.items():
        db.client.collection("electionsByState").document(state).set(state_data)
