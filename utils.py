import backoff
import logging
import os
import re
import requests

logging.getLogger("backoff").addHandler(logging.StreamHandler())


def pick(d, keys):
    return {k: d[k] for k in keys if k in d}


def fatal_code(e):
    try:
        return e.response.status_code == 422 or e.response.status_code >= 500
    except AttributeError:
        return False


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


def get_first_last_name(common_name):
    name_parts = common_name.split(" ")
    first_name = name_parts[0]
    last_name = name_parts[-1]
    if re.match("^([IVX]+|[SJ]r.?)$", last_name):
        last_name = name_parts[-2]
    return first_name, last_name


def get_expenditure_race_type(expenditure):
    election_type = expenditure.get("election_type", None)
    if election_type is None:
        # If the expenditure doesn't have an election type (as with efiled expenditures), we have to try to figure it
        # out later by comparing dates.
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
