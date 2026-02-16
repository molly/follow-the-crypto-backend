from fetch_committee_contributions import (
    should_omit,
    get_ids_to_omit,
)
from utils import FEC_fetch, pick

MIN_CONTRIBUTION_AMOUNT = 1000


def is_high_level_individual(contrib, allowlists):
    """Check if a contribution is from a high-level individual based on occupation allowlist.
    This mirrors the logic in process_committee_contributions.py's is_redacted function."""
    if contrib.get("claimed", False):
        return True
    if contrib.get("entity_type") in {"ORG", "PAC", "COM"} or (
        not contrib.get("contributor_first_name") and not contrib.get("contributor_last_name")
    ):
        # Not an individual contribution
        return False
    if not contrib.get("contributor_occupation"):
        # No occupation listed, not a high-level individual
        return False
    occupation = contrib["contributor_occupation"].upper()
    return occupation in allowlists["equals"] or allowlists["contains"].search(occupation)


PICKED_FIELDS = [
    # Picked from Schedule A directly
    "contributor_name",
    "contributor_first_name",
    "contributor_last_name",
    "contributor_occupation",
    "contributor_employer",
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


def parse_search_id(term):
    """Parse a search_id term. Returns (stripped_term, is_exact)."""
    if term.startswith("^") and term.endswith("$"):
        return term[1:-1], True
    return term, False


def is_exact_match(contrib, exact_terms):
    """Check if contributor_name or contributor_employer exactly matches any exact term (case-insensitive)."""
    upper_terms = {t.upper() for t in exact_terms}
    name = (contrib.get("contributor_name") or "").upper()
    employer = (contrib.get("contributor_employer") or "").upper()
    return name in upper_terms or employer in upper_terms


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


def _should_skip(contrib, contrib_ids, ids_to_omit, exact_terms, search_param, occupation_allowlist):
    """Check if a contribution should be skipped."""
    if should_omit(contrib, contrib_ids, ids_to_omit):
        return True
    if abs(contrib.get("contribution_receipt_amount", 0)) < MIN_CONTRIBUTION_AMOUNT:
        return True
    if exact_terms and not is_exact_match(contrib, exact_terms):
        return True
    # Skip contributions to WinRed & ActBlue (same as in individuals.py)
    if contrib.get("committee_id") in ["C00694323", "C00401224"]:
        return True
    # If searching by employer, only include high-level individuals
    if search_param == "contributor_employer":
        if not is_high_level_individual(contrib, occupation_allowlist):
            return True
    return False


def _fetch_processed(
    session,
    search_param,
    search_values,
    contributions,
    contrib_ids,
    ids_to_omit,
    exact_terms,
    occupation_allowlist,
):
    """Fetch processed schedule_a contributions for a given search parameter."""
    last_index = None
    last_contribution_receipt_date = None
    contribs_count = 0
    while True:
        contribution_data = FEC_fetch(
            session,
            "company contributions",
            "https://api.open.fec.gov/v1/schedules/schedule_a/",
            params={
                search_param: search_values,
                "two_year_transaction_period": "2026",
                "per_page": "100",
                "sort": "-contribution_receipt_date",
                "last_index": last_index,
                "last_contribution_receipt_date": last_contribution_receipt_date,
                "min_amount": 1000,
            },
        )
        if not contribution_data:
            continue

        contribs_count += contribution_data["pagination"]["per_page"]
        results = contribution_data["results"]
        ids_to_omit.update(get_ids_to_omit(results))
        for contrib in results:
            if _should_skip(contrib, contrib_ids, ids_to_omit, exact_terms, search_param, occupation_allowlist):
                continue
            contributions.append(process_contribution(contrib))
            contrib_ids.add(contrib["transaction_id"])

        if contribs_count >= contribution_data["pagination"]["count"]:
            break
        else:
            last_index = contribution_data["pagination"]["last_indexes"]["last_index"]
            last_contribution_receipt_date = contribution_data["pagination"][
                "last_indexes"
            ]["last_contribution_receipt_date"]


def _fetch_efiled(
    session,
    search_param,
    search_values,
    contributions,
    contrib_ids,
    ids_to_omit,
    exact_terms,
    occupation_allowlist,
):
    """Fetch e-filed schedule_a contributions for a given search parameter."""
    page = 1
    contribs_count = 0
    while True:
        data = FEC_fetch(
            session,
            "unprocessed committee contributions",
            "https://api.open.fec.gov/v1/schedules/schedule_a/efile",
            params={
                search_param: search_values,
                "min_date": "2025-01-01",
                "per_page": 100,
                "sort": "-contribution_receipt_date",
                "page": page,
                "min_amount": 1000,
            },
        )

        if not data:
            continue

        contribs_count += data["pagination"]["per_page"]
        results = data["results"]
        ids_to_omit.update(get_ids_to_omit(results))
        for contrib in results:
            if _should_skip(contrib, contrib_ids, ids_to_omit, exact_terms, search_param, occupation_allowlist):
                continue
            contributions.append({**process_contribution(contrib), "efiled": True})
            contrib_ids.add(contrib["transaction_id"])

        if page >= data["pagination"]["pages"]:
            break
        else:
            page += 1


def update_spending_by_company(db, session):
    for str_id, company in db.companies.items():
        # Sync companies with the constants dict
        related_individuals = [
            individual
            for str_id, individual in db.individuals.items()
            if company["name"] in individual.get("company", [])
        ]
        related_individuals.sort(key=lambda x: x.get("title", "zzz"))
        db.client.collection("companies").document(str_id).set(
            {
                **company,
                "relatedIndividuals": related_individuals,
            }
        )
        search_id = company.get("search_id", str_id.replace("-", " "))
        if isinstance(search_id, list):
            raw_search_ids = search_id
        else:
            raw_search_ids = [search_id]

        # Parse into fuzzy and exact groups
        fuzzy_ids = []
        exact_ids = []
        for term in raw_search_ids:
            stripped, is_exact = parse_search_id(term)
            if is_exact:
                exact_ids.append(stripped)
            else:
                fuzzy_ids.append(stripped)

        # Build search jobs: (param_name, values, exact_filter_terms or None)
        search_jobs = []
        if fuzzy_ids:
            search_jobs.append(("contributor_name", fuzzy_ids, None))
            search_jobs.append(("contributor_employer", fuzzy_ids, None))
        if exact_ids:
            search_jobs.append(("contributor_name", exact_ids, exact_ids))
            search_jobs.append(("contributor_employer", exact_ids, exact_ids))

        contributions = []
        contrib_ids = set()
        # Initialize with company-specific duplicates from database (same as individuals.py)
        ids_to_omit = set(db.duplicate_contributions.get(str_id, []))

        for search_param, search_values, exact_terms in search_jobs:
            _fetch_processed(
                session,
                search_param,
                search_values,
                contributions,
                contrib_ids,
                ids_to_omit,
                exact_terms,
                db.occupation_allowlist,
            )
            _fetch_efiled(
                session,
                search_param,
                search_values,
                contributions,
                contrib_ids,
                ids_to_omit,
                exact_terms,
                db.occupation_allowlist,
            )

        db.client.collection("rawCompanyContributions").document(str_id).set(
            {"contributions": contributions}
        )
