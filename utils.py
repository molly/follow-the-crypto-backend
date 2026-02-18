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
def FEC_fetch(session, description, url, params={}):
    r = session.get(
        url,
        params={
            **params,
            "api_key": os.environ["FEC_API_KEY"],
        },
        timeout=30,
    )
    if r.status_code == 404:
        return None
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


def compare_names_lastfirst(name, last_first):
    """Attempt to match last_first (eg Doe, John) to name (eg John Doe), accounting for typos and common variations."""
    normalized_name = unidecode(name).upper().split(" ")
    a_first = normalized_name[0]
    a_last = normalized_name[-1]

    normalized_last_first = unidecode(last_first).upper().split(", ")
    b_first = normalized_last_first[1] if len(normalized_last_first) > 1 else ""
    b_first = b_first.split(" ")[0]  # In case there are middle names or suffixes
    b_last = normalized_last_first[0]

    if a_last == b_last and a_first == b_first:
        return True

    first_similar = False
    last_similar = False
    if a_last == b_last:
        last_similar = True
    elif ratio(a_last, b_last, score_cutoff=0.8) > 0.8:
        last_similar = True

    if a_first == b_first:
        first_similar = True
    elif a_first.startswith(b_first) or b_first.startswith(a_first):
        # Account for Bens, Chrises, etc.
        first_similar = True
    elif ratio(a_first, b_first, score_cutoff=0.8) > 0.8:
        first_similar = True
    elif re.match(r"(MRS?|MS)\.?", b_first):
        first_similar = True

    return first_similar and last_similar


def get_expenditure_race_type(expenditure, races=None):
    subrace = expenditure.get("subrace", None)
    if subrace is not None:
        return subrace

    election_type = expenditure.get("election_type", None)
    election_type_full = expenditure.get("election_type_full", None)
    if election_type_full is not None:
        election_type_full = election_type_full.lower()
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
        split_type = re.split("[- ]", election_type_full)
        if len(split_type) > 1:
            if split_type[1] == "primary":
                return "primary"
            elif split_type[1] == "runoff":
                return "primary_runoff"
            elif split_type[1] == "general":
                return "general"
        return "special"
    if election_type == "O":
        return "other"
    else:
        print("Unknown election type: " + election_type)
        return election_type


def get_beneficiaries(contributionGroup, recipientCommittee, nonCandidateCommittees):
    committee_id = (
        recipientCommittee["committee_id"] or contributionGroup["committee_id"]
    )
    if committee_id in nonCandidateCommittees:
        return committee_id
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
