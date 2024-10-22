import backoff
from Levenshtein import ratio
import logging
import os
import re
import requests
from unidecode import unidecode

logging.getLogger("backoff").addHandler(logging.StreamHandler())


def pick(d, keys):
    return {k: d[k] for k in keys if k in d}


def fatal_code(e):
    try:
        return e.response.status_code == 422 or e.response.status_code >= 500
    except AttributeError:
        return False


def chunk(lst, chunk_size=10):
    for i in range(0, len(lst), chunk_size):
        yield lst[i : i + chunk_size]


@backoff.on_exception(
    backoff.constant,
    (
        requests.exceptions.RequestException,
        requests.exceptions.ConnectionError,
        requests.exceptions.HTTPError,
        requests.exceptions.Timeout,
    ),
    interval=20,
    max_tries=5,
    giveup=fatal_code,
)
def FEC_fetch(description, url, params={}):
    r = requests.get(
        url,
        params={
            **params,
            "api_key": os.environ["FEC_API_KEY"],
        },
        timeout=30,
    )
    r.raise_for_status()
    if r.status_code == 200:
        return r.json()


def openSecrets_fetch(description, url, params={}):
    r = requests.get(
        url,
        params={
            **params,
            "output": "json",
            "apikey": os.environ["OS_API_KEY"],
        },
        timeout=30,
    )
    r.raise_for_status()
    if r.status_code == 200:
        return r.json()


def get_first_last_name(common_name):
    name_parts = common_name.split(" ")
    first_name = name_parts[0]
    last_name = name_parts[-1]
    if re.match("^([IVX]+|[SJ]r.?)$", last_name):
        last_name = name_parts[-2]
    return first_name, last_name


def compare_names(name_portion, name, allow_levenstein=False):
    upper_name = unidecode(name).upper()
    upper_name_portion = unidecode(name_portion).upper()
    if upper_name_portion in upper_name:
        return True
    elif allow_levenstein:
        upper_last_name = upper_name.split(" ")[-1]
        if ratio(upper_name_portion, upper_last_name, score_cutoff=0.8) > 0.8:
            # Account for occasional typos in names
            return True
    return False


def get_expenditure_race_type(expenditure, races=None):
    subrace = expenditure.get("subrace", None)
    if subrace is not None:
        return subrace

    if (
        expenditure.get("candidate_name") == "ONDER"
        and expenditure.get("dissemination_date") == "2025-07-25"
    ):
        # Screwy date on an expenditure
        expenditure["dissemination_date"] = "2024-07-25"
    election_type = expenditure.get("election_type", None)
    if election_type is None:
        if races is None:
            # If the expenditure doesn't have an election type (as with efiled expenditures), we have to try to figure it
            # out later by comparing dates.
            return None
        else:
            expenditure_date = expenditure.get("dissemination_date")
            if expenditure_date is None:
                expenditure_date = expenditure.get("expenditure_date", None)
                if expenditure_date is None:
                    return None
            for race in reversed(races):
                for candidate in race["candidates"]:
                    if compare_names(
                        expenditure.get(
                            "candidate_last_name", expenditure.get("candidate_name")
                        ),
                        candidate["name"],
                    ):
                        race_date = race.get("date", None)
                        if race_date is None or race_date >= expenditure_date:
                            expenditure["subrace"] = race["type"]
                            return race["type"]

            # If we couldn't find the race type in the first loop, try again and look for typos
            for race in reversed(races):
                for candidate in race["candidates"]:
                    if compare_names(
                        expenditure.get(
                            "candidate_last_name", expenditure.get("candidate_name")
                        ),
                        candidate["name"],
                        True,
                    ):
                        race_date = race.get("date", None)
                        if race_date and race_date >= expenditure_date:
                            expenditure["subrace"] = race["type"]
                            return race["type"]
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
    if election_type == "O":
        return "other"
    else:
        print("Unknown election type: " + election_type)
        return election_type


def get_beneficiaries(contributionGroup, recipientCommittee):
    if (
        recipientCommittee
        and "candidate_ids" in recipientCommittee
        and len(recipientCommittee["candidate_ids"]) > 0
    ):
        return recipientCommittee["candidate_ids"]
    elif (
        "candidate_ids" in contributionGroup
        and len(contributionGroup["candidate_ids"]) > 0
    ):
        return contributionGroup["candidate_ids"]
    else:
        return [contributionGroup["committee_id"]]
