import logging
import traceback

import google.cloud.logging
from Database import Database

from committee_details import hydrate_committees
from fetch_committee_contributions import (
    update_committee_contributions,
)
from process_committee_contributions import process_committee_contributions
from committee_expenditures import (
    update_committee_expenditures,
)
from process_committee_expenditures import process_expenditures
from committee_disbursements import update_committee_disbursements
from races import update_race_details
from race_summary import summarize_races
from candidate_trim import trim_candidates
from candidate_images import get_candidates_without_images
from outside_spending import update_candidate_outside_spending
from pacs import get_top_raised_pacs
from candidate_expenditures import update_candidates_expenditures
from ads import get_ads
from individuals import update_spending_by_individuals
from company_spending import update_spending_by_company
from process_individual_contributions import process_individual_contributions
from process_company_contributions import process_company_contributions


def main():
    client = google.cloud.logging.Client()
    client.setup_logging()
    logging.info("test")

    diff = {
        "contributions": {},
        "expenditures": {},
        "disbursements": {},
        "ads": {},
        "new_candidates": [],
        "new_recipient_committees": set(),
        "new_opposition_spending": set(),
    }

    try:
        db = Database()
        db.get_constants()
        print("Hydrating committees")
        hydrate_committees(db)

        # Contributions to PACs
        print("Updating committee contributions")
        diff["contributions"] = update_committee_contributions(db)
        print("Processing committee contributions")
        process_committee_contributions(db)

        # Expenditures by PACs
        print("Updating committee expenditures")
        diff["expenditures"] = update_committee_expenditures(db)
        print("Processing committee expenditures")
        diff["new_opposition_spending"] = process_expenditures(db)

        # Disbursements by PACs
        print("Updating committee disbursements")
        diff["disbursements"] = update_committee_disbursements(db)

        # Race details
        print("Updating race details")
        update_race_details(db)
        print("Summarize races")
        summarize_races(db)
        print("Trimming candidate lists")
        trim_candidates(db)
        diff["new_candidates"] = get_candidates_without_images(db)

        # Outside spending
        print("Getting all outside spending for candidates")
        update_candidate_outside_spending(db)

        # PAC summaries
        print("Get top raised PACs")
        get_top_raised_pacs(db)

        # Group expenditures by candidate
        print("Update candidate expenditures")
        update_candidates_expenditures(db)

        # Get committee ads
        print("Get ads")
        diff["ads"] = get_ads(db)

        update_spending_by_company(db)
        update_spending_by_individuals(db)
        diff["new_recipient_committees"] = process_individual_contributions(db)
        diff["new_recipient_committees"].union(process_company_contributions(db))
    except Exception as e:
        traceback.print_exc()
        logging.exception(e)
    finally:
        print(diff)


if __name__ == "__main__":
    main()
