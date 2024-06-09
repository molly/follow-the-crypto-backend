import google.cloud.logging
from Database import Database

from committee_details import hydrate_committees
from committee_contributions import update_committee_contributions
from committee_expenditures import update_committee_expenditures
from races import update_race_details
from race_summary import summarize_races
from pacs import get_top_raised_pacs
from candidate_expenditures import update_candidates_expenditures
from ads import get_ads


def main():
    client = google.cloud.logging.Client()
    client.setup_logging()

    db = Database()
    db.get_constants()
    hydrate_committees(db)
    # update_committee_contributions(db)
    # update_committee_expenditures(db)
    # update_race_details(db)
    # summarize_races(db)
    get_top_raised_pacs(db)
    update_candidates_expenditures(db)
    get_ads(db)


if __name__ == "__main__":
    main()
