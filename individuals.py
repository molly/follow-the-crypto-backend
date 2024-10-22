from company_spending import process_contribution
from utils import FEC_fetch


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
    if "zip" in individual and not efiled:
        search_params["contributor_zip"] = individual["zip"]
    elif efiled and "city" in individual:
        search_params["contributor_city"] = individual["city"]
    elif "employerSearch" in individual:
        if (
            len(individual["employerSearch"]) == 0
            or individual["employerSearch"][0] == ""
        ):
            return search_params
        search_params["contributor_employer"] = individual["employerSearch"]
    elif companies:
        search_params["contributor_employer"] = [
            company.get("search_id", company["id"].replace("-", " "))
            for company in companies
        ]
    return search_params


def update_spending_by_individuals(db):
    for str_id, individual in db.individuals.items():
        old_contributions_dict = (
            db.client.collection("rawIndividualContributions")
            .document(str_id)
            .get()
            .to_dict()
        )
        if old_contributions_dict:
            old_contribution_ids = set(
                x["transaction_id"]
                for x in old_contributions_dict.get("contributions", [])
            )
        else:
            old_contribution_ids = set()

        new_contributions = []
        contributions_data = {"contributions": [], "associatedCompany": []}
        associated_companies = get_associated_company_ids(
            individual, db.companies.values()
        )
        if associated_companies:
            contributions_data["associatedCompany"] = associated_companies

        ids_to_omit = set(db.duplicate_contributions.get(str_id, []))
        last_index = None
        last_contribution_receipt_date = None
        contribs_count = 0
        search_params = get_individual_search_params(
            individual, [db.companies[company] for company in associated_companies]
        )

        # Get regularly filed contributions for individual
        while True:
            contribution_data = FEC_fetch(
                "company contributions",
                "https://api.open.fec.gov/v1/schedules/schedule_a/",
                params={
                    **search_params,
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
            for contrib in results:
                if contrib["transaction_id"] in ids_to_omit or contrib[
                    "committee_id"
                ] in ["C00694323", "C00401224"]:
                    # Duplicate transactions, or contributions to WinRed & ActBlue
                    continue
                if (
                    str_id == "cameron-winklevoss"
                    and contrib["transaction_id"] == "SA11AI.122113936"
                ):
                    contrib["contribution_receipt_amount"] = 838089.15
                elif (
                    str_id == "tyler-winklevoss"
                    and contrib["transaction_id"] == "SA11AI.122113934"
                ):
                    contrib["contribution_receipt_amount"] = 838089.15
                processed = process_contribution(contrib)
                contributions_data["contributions"].append(processed)
                new_contributions.append(processed)
                if contrib["transaction_id"] not in old_contribution_ids:
                    new_contributions.append(processed)

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
        search_params = get_individual_search_params(
            individual,
            [db.companies[company] for company in associated_companies],
            efiled=True,
        )
        while True:
            data = FEC_fetch(
                "unprocessed committee contributions",
                "https://api.open.fec.gov/v1/schedules/schedule_a/efile",
                params={
                    **search_params,
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
            for contrib in results:
                if contrib["transaction_id"] in ids_to_omit or contrib[
                    "committee_id"
                ] in ["C00694323", "C00401224"]:
                    continue
                if (
                    str_id == "cameron-winklevoss"
                    and contrib["transaction_id"] == "SA11AI.122113936"
                ):
                    contrib["contribution_receipt_amount"] = 838089.15
                elif (
                    str_id == "tyler-winklevoss"
                    and contrib["transaction_id"] == "SA11AI.122113934"
                ):
                    contrib["contribution_receipt_amount"] = 838089.15
                processed = {**process_contribution(contrib), "efiled": True}
                contributions_data["contributions"].append(processed)
                if contrib["transaction_id"] not in old_contribution_ids:
                    new_contributions.append(processed)

            # Fetch more pages if they exist, or break
            if page >= data["pagination"]["pages"]:
                break
            else:
                page += 1

        db.client.collection("rawIndividualContributions").document(str_id).set(
            contributions_data
        )
    return new_contributions
