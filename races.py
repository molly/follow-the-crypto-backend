import logging
import requests

from config import fec_data_config
from states import STATES_BY_ABBR


def update_race_details(db):
    doc_ref = db.client.collection("expenditures").document("states")
    doc = doc_ref.get()
    data = None
    if doc.exists:
        data = doc.to_dict()
    else:
        logging.error("No expenditures found when updating race details.")

    race_data = {}
    races = set()
    for state, state_data in data.items():
        for race_id in state_data["by_race"].keys():
            races.add(race_id)

    for race in races:
        race_parts = race.split("-")
        election_name = "-".join(race_parts[1:])

        if race_parts[0] not in race_data:
            race_data[race_parts[0]] = {
                election_name: {"candidates": [], "dates": []}
            }
        if election_name not in race_data[race_parts[0]]:
            race_data[race_parts[0]][election_name] = {"candidates": [], "dates": []}

        candidate_params = {
            "api_key": fec_data_config.api_key,
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

        try:
            candidate_r = requests.get(
                "https://api.open.fec.gov/v1/elections",
                params=candidate_params,
                timeout=30,
            )
            candidate_r.raise_for_status()
        except (
            requests.exceptions.RequestException,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            requests.exceptions.Timeout,
        ) as e:
            logging.error(f"Failed to fetch election candidate details: {e}")
        else:
            candidate_data = candidate_r.json()
            data_to_include = candidate_data["results"][:2]
            for candidate in candidate_data["results"][2:]:
                if candidate["total_receipts"] > 1000000:
                    data_to_include.append(candidate)
            race_data[race_parts[0]][election_name]["candidates"] = data_to_include

        dates_params = {
            "api_key": fec_data_config.api_key,
            "election_state": race_parts[0],
            "election_year": 2024,
            "office_sought": race_parts[1],
            "sort": "-election_date",
        }
        if race_parts[1] == "H":
            dates_params["district"] = race_parts[2]
        try:
            dates_r = requests.get(
                "https://api.open.fec.gov/v1/election-dates",
                params=dates_params,
                timeout=30,
            )
            dates_r.raise_for_status()
        except (
            requests.exceptions.RequestException,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            requests.exceptions.Timeout,
        ) as e:
            logging.error(f"Failed to fetch election date details: {e}")
            return
        else:
            dates_data = dates_r.json()
            race_data[race_parts[0]][election_name]["dates"] = dates_data["results"]

    db.client.collection("elections").document("state").set(race_data)
