from fetch_committee_contributions import (
    should_omit,
    get_ids_to_omit,
    EARLY_STOP_PATIENCE,
)
from utils import FEC_fetch, pick

MIN_CONTRIBUTION_AMOUNT = 1000


def is_high_level_individual(contrib, allowlists):
    """Check if a contribution is from a high-level individual based on occupation allowlist.
    This mirrors the logic in process_committee_contributions.py's is_redacted function.
    """
    if contrib.get("claimed", False):
        return True
    if contrib.get("entity_type") in {"ORG", "PAC", "COM"} or (
        not contrib.get("contributor_first_name")
        and not contrib.get("contributor_last_name")
    ):
        # Not an individual contribution
        return False
    if not contrib.get("contributor_occupation"):
        # No occupation listed, not a high-level individual
        return False
    occupation = contrib["contributor_occupation"].upper()
    return occupation in allowlists["equals"] or allowlists["contains"].search(
        occupation
    )


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
    "memo_code",
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


def _should_skip(
    contrib, contrib_ids, ids_to_omit, exact_terms, search_param, occupation_allowlist
):
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


def update_spending_by_company(db, session, full=False):
    """
    Store each tracked company's contributions in the "rawCompanyContributions" collection.

    By default this is INCREMENTAL: each search job's results are sorted newest-first, so once
    EARLY_STOP_PATIENCE consecutive pages contain no transaction IDs we don't already have stored,
    that job stops paginating; the newly-found contributions are unioned into the existing document.
    Pass full=True (the --full-fetch flag) to re-fetch everything and overwrite; run periodically.
    """
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
            },
            merge=True,
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

        old = (
            db.client.collection("rawCompanyContributions")
            .document(str_id)
            .get()
            .to_dict()
        )
        old_contributions = old.get("contributions", []) if old else []
        old_ids = set(c["transaction_id"] for c in old_contributions)
        incremental = not full and bool(old_ids)

        fetched = []  # contributions kept this run (everything if full; only-new if incremental)
        fetched_ids = set()
        # Initialize with company-specific duplicates from database (same as individuals.py)
        ids_to_omit = set(db.duplicate_contributions.get(str_id, []))

        def handle_page(results, efiled, exact_terms, search_param):
            """Process one page; return True if it held no transaction IDs we didn't already have."""
            ids_to_omit.update(get_ids_to_omit(results))
            page_has_new = False
            for contrib in results:
                tid = contrib["transaction_id"]
                if tid not in old_ids:
                    page_has_new = True
                if _should_skip(
                    contrib,
                    fetched_ids,
                    ids_to_omit,
                    exact_terms,
                    search_param,
                    db.occupation_allowlist,
                ):
                    continue
                if incremental and tid in old_ids:
                    fetched_ids.add(tid)
                    continue
                processed = process_contribution(contrib)
                if efiled:
                    processed["efiled"] = True
                fetched.append(processed)
                fetched_ids.add(tid)
            return not page_has_new

        def fetch_processed(search_param, search_values, exact_terms):
            last_index = None
            last_contribution_receipt_date = None
            contribs_count = 0
            dry_streak = 0
            while True:
                data = FEC_fetch(
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
                if not data:
                    continue
                contribs_count += data["pagination"]["per_page"]
                page_dry = handle_page(data["results"], False, exact_terms, search_param)
                if incremental:
                    dry_streak = dry_streak + 1 if page_dry else 0
                    if dry_streak >= EARLY_STOP_PATIENCE:
                        break
                if contribs_count >= data["pagination"]["count"]:
                    break
                else:
                    last_index = data["pagination"]["last_indexes"]["last_index"]
                    last_contribution_receipt_date = data["pagination"][
                        "last_indexes"
                    ]["last_contribution_receipt_date"]

        def fetch_efiled(search_param, search_values, exact_terms):
            page = 1
            contribs_count = 0
            dry_streak = 0
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
                page_dry = handle_page(data["results"], True, exact_terms, search_param)
                if incremental:
                    dry_streak = dry_streak + 1 if page_dry else 0
                    if dry_streak >= EARLY_STOP_PATIENCE:
                        break
                if page >= data["pagination"]["pages"]:
                    break
                else:
                    page += 1

        for search_param, search_values, exact_terms in search_jobs:
            fetch_processed(search_param, search_values, exact_terms)
            fetch_efiled(search_param, search_values, exact_terms)

        # Incremental unions the new contributions onto what's stored; full overwrites.
        merged = (old_contributions + fetched) if incremental else fetched
        db.client.collection("rawCompanyContributions").document(str_id).set(
            {"contributions": merged}
        )
