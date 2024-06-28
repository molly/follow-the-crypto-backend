import backoff
import logging
import re
import requests
from msecrets import FEC_API_KEY

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
            "api_key": FEC_API_KEY,
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
