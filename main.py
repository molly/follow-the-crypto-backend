import google.cloud.logging
from Database import Database

from committee_details import hydrate_committees
from fetch_committee_contributions import (
    update_committee_contributions,
    update_recent_committee_contributions,
)
from process_committee_contributions import process_committee_contributions
from committee_expenditures import (
    update_committee_expenditures,
    update_recent_committee_expenditures,
)
from process_committee_expenditures import process_recent_expenditures
from races import update_race_details
from race_summary import summarize_races
from candidate_trim import trim_candidates
from outside_spending import update_candidate_outside_spending
from pacs import get_top_raised_pacs
from candidate_expenditures import update_candidates_expenditures
from ads import get_ads


def main():
    """Wipe everything out and then re-fetch all data. Mostly used for development."""
    client = google.cloud.logging.Client()
    client.setup_logging()

    db = Database()
    db.get_constants()
    print("Hydrating committees")
    hydrate_committees(db)
    print("Updating committee contributions")
    update_committee_contributions(db)
    update_recent_committee_contributions(db)
    print("Processing committee contributions")
    process_committee_contributions(db)
    print("Updating committee expenditures")
    update_committee_expenditures(db)
    update_recent_committee_expenditures(db)
    print("Processing committee expenditures")
    process_recent_expenditures(db)
    print("Updating race details")
    update_race_details(db)
    print("Summarize races")
    summarize_races(db)
    print("Trimming candidate lists")
    trim_candidates(db)
    print("Getting outside spending for candidates")
    update_candidate_outside_spending(db)
    print("Get top raised PACs")
    get_top_raised_pacs(db)
    print("Update candidate expenditures")
    update_candidates_expenditures(db)
    print("Get ads")
    get_ads(db)


if __name__ == "__main__":
    main()
