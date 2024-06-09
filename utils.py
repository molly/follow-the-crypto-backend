import logging
import re
import requests
from msecrets import FEC_API_KEY


def pick(d, keys):
    return {k: d[k] for k in keys if k in d}


def FEC_fetch(description, url, params={}):
    try:
        r = requests.get(
            url,
            params={
                **params,
                "api_key": FEC_API_KEY,
            },
            timeout=30,
        )
        r.raise_for_status()
    except (
        requests.exceptions.RequestException,
        requests.exceptions.ConnectionError,
        requests.exceptions.HTTPError,
        requests.exceptions.Timeout,
    ) as e:
        print(f"Failed to fetch {description}: {e}")
        logging.error(f"Failed to fetch {description}: {e}")
        print(e)
        return
    return r.json()


def get_first_last_name(common_name):
    name_parts = common_name.split(" ")
    first_name = name_parts[0]
    last_name = name_parts[-1]
    if re.match("^([IVX]+|[SJ]r.?)$", last_name):
        last_name = name_parts[-2]
    return first_name, last_name
