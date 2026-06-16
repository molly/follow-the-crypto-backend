from company_spending import parse_search_id, process_contribution
from fetch_committee_contributions import EARLY_STOP_PATIENCE
from utils import FEC_fetch

# Committees whose contributions we never store for individuals (WinRed & ActBlue conduits).
EXCLUDED_COMMITTEE_IDS = ["C00694323", "C00401224"]


def should_exclude_contribution(contrib, individual):
    """
    Drop contributions that match an individual's `excludeContributions` rules.

    Name-only searches (an individual with no employer/zip filter) can pull in
    same-named donors who are not the tracked individual. `excludeContributions`
    is a dict of {raw FEC field: [values]}; a contribution is dropped if any field
    matches any listed value (case-insensitive). E.g. {"contributor_state": ["IN"]}
    drops the Indiana namesake from anna-brockman's name-only results, now and for
    any future contributions she makes -- no per-transaction maintenance.
    """
    rules = individual.get("excludeContributions")
    if not rules:
        return False
    for field, values in rules.items():
        value = contrib.get(field)
        if value is None:
            continue
        if str(value).strip().upper() in {str(v).strip().upper() for v in values}:
            return True
    return False


def get_associated_company_ids(individual, companies):
    company_ids = []
    if "company" in individual:
        company_ids = [x["id"] for x in companies if x["name"] in individual["company"]]
    return company_ids


def get_individual_search_params(individual, companies, efiled=False):
    search_params = {}
    search_params["contributor_name"] = individual.get(
        "nameSearch", individual["id"].replace("-", " ")
    )

    # An explicit empty employerSearch ("" or [] or [""]) means search by NAME ONLY -- no employer
    # or zip constraint. This is for high-profile donors who give under several different employers
    # (e.g. Elon Musk via SpaceX / Tesla / xAI / X), where any single-employer filter would miss
    # most of their gifts. Checked before the employer/zip branches so nothing gets attached.
    if "employerSearch" in individual:
        es = individual["employerSearch"]
        if not es or (isinstance(es, list) and es[0] == "") or es == "":
            return search_params

    if "zip" in individual and not efiled:
        search_params["contributor_zip"] = individual["zip"]
    elif efiled and "city" in individual:
        search_params["contributor_city"] = individual["city"]
    elif companies:
        employer_params = []
        for company in companies:
            search_ids = [company["id"].replace("-", " ")]
            if "search_id" in company:
                if isinstance(company["search_id"], list):
                    search_ids.extend(company["search_id"])
                else:
                    search_ids.append(company["search_id"])
            employer_params.extend(
                parse_search_id(term)[0] for term in search_ids
            )
        search_params["contributor_employer"] = employer_params
    elif "company" in individual:
        search_params["contributor_employer"] = individual["company"]

    # A non-empty employerSearch appends extra employer terms to whatever was set above.
    if "employerSearch" in individual:
        if "contributor_employer" not in search_params:
            search_params["contributor_employer"] = []
        if isinstance(individual["employerSearch"], list):
            for query in individual["employerSearch"]:
                parsed = parse_search_id(query)
                if parsed[0] not in search_params["contributor_employer"]:
                    search_params["contributor_employer"].append(parsed[0])
        else:
            parsed = parse_search_id(individual["employerSearch"])
            if parsed[0] not in search_params["contributor_employer"]:
                search_params["contributor_employer"].append(parsed[0])
    return search_params


def update_spending_by_individuals(db, session, full=False):
    """
    Store each tracked individual's contributions in the "rawIndividualContributions" collection.

    By default this is INCREMENTAL: contributions are sorted newest-first, so once EARLY_STOP_PATIENCE
    consecutive pages contain no transaction IDs we don't already have stored, we stop paginating and
    union the newly-found contributions into the existing document. Pass full=True (the --full-fetch
    flag) to re-fetch everything and overwrite, reconciling amendments/deletions; run it periodically.
    """
    new_contributions = []
    for str_id, individual in db.individuals.items():
        old_contributions_dict = (
            db.client.collection("rawIndividualContributions")
            .document(str_id)
            .get()
            .to_dict()
        )
        if old_contributions_dict:
            old_contributions = old_contributions_dict.get("contributions", [])
            old_contribution_ids = set(x["transaction_id"] for x in old_contributions)
        else:
            old_contributions = []
            old_contribution_ids = set()

        # An incremental run only makes sense when we have a baseline to diff against.
        incremental = not full and bool(old_contribution_ids)

        associated_companies = get_associated_company_ids(
            individual, db.companies.values()
        )

        ids_to_omit = set(db.duplicate_contributions.get(str_id, []))
        fetched = []  # contributions kept this run (everything if full; only-new if incremental)
        fetched_ids = set()

        def handle_page(results, efiled):
            """Process one page; return True if it held no transaction IDs we didn't already have."""
            page_has_new = False
            for contrib in results:
                tid = contrib["transaction_id"]
                if tid not in old_contribution_ids:
                    page_has_new = True
                if (
                    tid in ids_to_omit
                    or contrib["committee_id"] in EXCLUDED_COMMITTEE_IDS
                    or should_exclude_contribution(contrib, individual)
                ):
                    # Duplicate transactions, contributions to WinRed & ActBlue, or
                    # same-named donors excluded via the individual's excludeContributions rule
                    continue
                if tid in fetched_ids or (incremental and tid in old_contribution_ids):
                    continue
                processed = process_contribution(contrib)
                if efiled:
                    processed["efiled"] = True
                fetched.append(processed)
                fetched_ids.add(tid)
                if tid not in old_contribution_ids:
                    new_contributions.append(processed)
            return not page_has_new

        # Get regularly filed contributions for individual
        last_index = None
        last_contribution_receipt_date = None
        contribs_count = 0
        dry_streak = 0
        search_params = get_individual_search_params(
            individual, [db.companies[company] for company in associated_companies]
        )
        while True:
            contribution_data = FEC_fetch(
                session,
                "committee contributions",
                "https://api.open.fec.gov/v1/schedules/schedule_a/",
                params={
                    **search_params,
                    "two_year_transaction_period": "2026",
                    "per_page": "100",
                    "sort": "-contribution_receipt_date",
                    "last_index": last_index,
                    "last_contribution_receipt_date": last_contribution_receipt_date,
                    "min_amount": 1000
                },
            )
            if not contribution_data:
                continue

            contribs_count += contribution_data["pagination"]["per_page"]
            page_dry = handle_page(contribution_data["results"], efiled=False)

            if incremental:
                dry_streak = dry_streak + 1 if page_dry else 0
                if dry_streak >= EARLY_STOP_PATIENCE:
                    break

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

        # Get efiled contributions for individual
        page = 1
        contribs_count = 0
        dry_streak = 0
        search_params = get_individual_search_params(
            individual,
            [db.companies[company] for company in associated_companies],
            efiled=True,
        )
        while True:
            data = FEC_fetch(
                session,
                "unprocessed committee contributions",
                "https://api.open.fec.gov/v1/schedules/schedule_a/efile",
                params={
                    **search_params,
                    "min_date": "2025-01-01",
                    "per_page": 100,
                    "sort": "-contribution_receipt_date",
                    "page": page,
                    "min_amount": 1000
                },
            )

            if not data:
                continue

            contribs_count += data["pagination"]["per_page"]
            page_dry = handle_page(data["results"], efiled=True)

            if incremental:
                dry_streak = dry_streak + 1 if page_dry else 0
                if dry_streak >= EARLY_STOP_PATIENCE:
                    break

            # Fetch more pages if they exist, or break
            if page >= data["pagination"]["pages"]:
                break
            else:
                page += 1

        # Incremental unions the new contributions onto what's stored; full overwrites.
        merged = (old_contributions + fetched) if incremental else fetched
        db.client.collection("rawIndividualContributions").document(str_id).set(
            {"contributions": merged, "associatedCompany": associated_companies or []}
        )
    return new_contributions
