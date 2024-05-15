import logging
import requests
from secrets import FEC_API_KEY
from utils import pick

CONTRIBUTION_FIELDS = [
    "contributor_first_name",
    "contributor_middle_name",
    "contributor_last_name",
    "contributor_suffix",
    "contributor_name",
    "contributor_occupation",
    "contributor_employer",
    "contribution_receipt_amount",
    "contribution_receipt_date",
    "entity_type",
]


def pick_contribution(d, keys, redacted):
    res = pick(d, keys)
    if redacted:
        for k in CONTRIBUTION_FIELDS[0:5]:
            res[k] = "REDACTED"
    res["redacted"] = redacted
    return res


def is_redacted(contrib, allowlists):
    """Redact any names for occupations not captured within the occupationAllowlist."""
    if not contrib["contributor_first_name"] and not contrib["contributor_last_name"]:
        # No redactions needed if this isn't an individual
        return False
    if not contrib["contributor_occupation"]:
        # Redact if the contributor is missing, just in case
        return True
    return not contrib["contributor_occupation"] in allowlists["equals"] and not any(
        substring in contrib["contributor_occupation"]
        for substring in allowlists["contains"]
    )


def is_identical(contrib1, contrib2):
    return (
        contrib1["contributor_first_name"] == contrib2["contributor_first_name"]
        and contrib1["contributor_middle_name"] == contrib2["contributor_middle_name"]
        and contrib1["contributor_last_name"] == contrib2["contributor_last_name"]
        and contrib1["contributor_suffix"] == contrib2["contributor_suffix"]
        and contrib1["contributor_name"] == contrib2["contributor_name"]
        and contrib1["contributor_occupation"] == contrib2["contributor_occupation"]
        and contrib1["contributor_employer"] == contrib2["contributor_employer"]
        and round(contrib1["contribution_receipt_amount"], 2)
        == contrib2["contribution_receipt_amount"]
        and contrib1["contribution_receipt_date"]
        == contrib2["contribution_receipt_date"]
    )


def update_committee_contributions(db):
    committee_ids = [committee["id"] for committee in db.committees.values()]
    for committee_id in committee_ids:
        donorMap = {"contributions_count": 0, "groups": {}}

        last_index = None
        last_contribution_receipt_amount = None
        contribs_count = 0
        redacted_count = 0
        while True:
            try:
                r = requests.get(
                    "https://api.open.fec.gov/v1/schedules/schedule_a",
                    params={
                        "api_key": FEC_API_KEY,
                        "committee_id": committee_id,
                        "per_page": 100,
                        "sort": "-contribution_receipt_amount",
                        "last_index": last_index,
                        "last_contribution_receipt_amount": last_contribution_receipt_amount,
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
                logging.error(f"Failed to fetch committee contributions: {e}")
                return

            data = r.json()
            contribs_count += data["pagination"]["per_page"]

            for contrib in data["results"]:
                redacted = is_redacted(contrib, db.occupation_allowlist)
                group = contrib["contributor_employer"] or contrib["contributor_name"]
                if group and group in db.individual_employers:
                    if redacted:
                        group = "REDACTED" + str(redacted_count)
                        redacted = True
                        redacted_count += 1
                    else:
                        group = contrib["contributor_name"]
                elif not group:
                    group = "UNKNOWN"
                elif group in db.company_aliases:
                    group = db.company_aliases[group]

                if group not in donorMap["groups"]:
                    donorMap["groups"][group] = {
                        "contributions": [
                            pick_contribution(contrib, CONTRIBUTION_FIELDS, redacted)
                        ],
                        "total": round(contrib["contribution_receipt_amount"], 2),
                    }
                else:
                    if any(
                        is_identical(contrib, c)
                        for c in donorMap["groups"][group]["contributions"]
                    ):
                        # Omit duplicate contributions
                        continue
                    donorMap["groups"][group]["contributions"].append(
                        pick_contribution(contrib, CONTRIBUTION_FIELDS, redacted)
                    )
                    donorMap["groups"][group]["total"] = round(
                        donorMap["groups"][group]["total"]
                        + contrib["contribution_receipt_amount"],
                        2,
                    )
                donorMap["contributions_count"] += 1

            if contribs_count >= data["pagination"]["count"]:
                break
            else:
                last_index = data["pagination"]["last_indexes"]["last_index"]
                last_contribution_receipt_amount = data["pagination"]["last_indexes"][
                    "last_contribution_receipt_amount"
                ]

        # Turn the map of groups into a list, sorted descending by total contributions
        donor_list = [
            {"company": company, **data} for company, data in donorMap["groups"].items()
        ]
        donorMap["groups"] = sorted(donor_list, key=lambda x: x["total"], reverse=True)

        db.client.collection("contributions").document(committee_id).set(donorMap)
