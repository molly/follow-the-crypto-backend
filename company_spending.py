from fetch_committee_contributions import (
    should_omit,
    get_ids_to_omit,
)
from utils import openSecrets_fetch, FEC_fetch, pick


PICKED_FIELDS = [
    # Picked from Schedule A directly
    "contributor_name",
    "committee_id",
    "contribution_receipt_amount",
    "contribution_receipt_date",
    "pdf_url",
    "receipt_type",
    "receipt_type_full",
    "transaction_id",
    "entity_type",
    "contributor_aggregate_ytd",
    "memo_text",
    "receipt_type",
]

# Nested fields from the committee object
COMMITTEE_CONTRIBUTION_FIELDS = [
    "name",
    "candidate_ids",
    "committee_type",
    "committee_type_full",
    "designation",
    "designation_full",
    "party",
    "state",
]

# Calculated and added
ADDED_FIELDS = [
    "committee_name",  # via committee.name
    "efiled",
    "link",
]

CONTRIBUTION_FIELDS = PICKED_FIELDS + COMMITTEE_CONTRIBUTION_FIELDS + ADDED_FIELDS


def process_contribution(contrib):
    contribution = pick(contrib, PICKED_FIELDS)
    committee_fields = pick(contrib["committee"], COMMITTEE_CONTRIBUTION_FIELDS)
    committee_fields["committee_name"] = committee_fields["name"]
    del committee_fields["name"]
    contribution.update(committee_fields)
    contribution["amendment_chain"] = contrib.get("filing", {}).get(
        "amendment_chain", []
    )
    return contribution


def update_spending_by_company(db):
    for str_id, company in db.companies.items():
        # Sync companies with the constants dict
        company_id = company["os_id"]
        data = openSecrets_fetch(
            "company spending",
            "http://www.opensecrets.org/api/?method=orgSummary",
            params={"id": company_id},
        )
        company_data = data["response"]["organization"]["@attributes"]
        related_individuals = [
            individual
            for str_id, individual in db.individuals.items()
            if individual.get("company", "") == company["name"]
        ]
        related_individuals.sort(key=lambda x: x.get("title", "zzz"))
        db.client.collection("companies").document(str_id).set(
            {
                **company,
                "openSecrets": company_data,
                "relatedIndividuals": related_individuals,
            }
        )
        company_search_id = company.get("search_id", str_id.replace("-", " "))

        # Fetch contributions
        contributions = []
        contrib_ids = set()
        last_index = None
        last_contribution_receipt_date = None
        contribs_count = 0

        # Look for contributions by the company directly
        if str_id != "paradigm":
            while True:
                contribution_data = FEC_fetch(
                    "company contributions",
                    "https://api.open.fec.gov/v1/schedules/schedule_a/",
                    params={
                        "contributor_name": company_search_id,
                        "two_year_transaction_period": "2024",
                        "per_page": "100",
                        "sort": "-contribution_receipt_date",
                        "last_index": last_index,
                        "last_contribution_receipt_date": last_contribution_receipt_date,
                    },
                )
                if not contribution_data:
                    continue

                contribs_count += contribution_data["pagination"]["per_page"]
                results = contribution_data["results"]
                ids_to_omit = get_ids_to_omit(results)
                for contrib in results:
                    if should_omit(contrib, contrib_ids, ids_to_omit):
                        continue
                    contributions.append(process_contribution(contrib))
                    contrib_ids.add(contrib["transaction_id"])

                # Fetch more pages if they exist, or break
                if contribs_count >= contribution_data["pagination"]["count"]:
                    break
                else:
                    last_index = contribution_data["pagination"]["last_indexes"][
                        "last_index"
                    ]
                    last_contribution_receipt_date = contribution_data["pagination"][
                        "last_indexes"
                    ]["last_contribution_receipt_date"]

        # Now fetch efiled contributions that may have not yet been processed
        page = 1
        contribs_count = 0
        if str_id != "paradigm":
            while True:
                data = FEC_fetch(
                    "unprocessed committee contributions",
                    "https://api.open.fec.gov/v1/schedules/schedule_a/efile",
                    params={
                        "contributor_name": company_search_id,
                        "min_date": "2023-01-01",
                        "per_page": 100,
                        "sort": "-contribution_receipt_date",
                        "page": page,
                    },
                )

                if not data:
                    continue

                contribs_count += data["pagination"]["per_page"]
                results = data["results"]
                ids_to_omit = ids_to_omit.union(get_ids_to_omit(results))
                for contrib in results:
                    if should_omit(contrib, contrib_ids, ids_to_omit):
                        continue
                    contributions.append(
                        {**process_contribution(contrib), "efiled": True}
                    )

                # Fetch more pages if they exist, or break
                if page >= data["pagination"]["pages"]:
                    break
                else:
                    page += 1

        db.client.collection("rawCompanyContributions").document(str_id).set(
            {"contributions": contributions}
        )
