import logging
import requests
from secrets import FEC_API_KEY


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
        logging.error(f"Failed to fetch {description}: {e}")
        return
    return r.json()
