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
    print("Hydrating committees")
    hydrate_committees(db)
    print("Updating committee contributions")
    update_committee_contributions(db)
    print("Updating committee expenditures")
    update_committee_expenditures(db)
    print("Updating race details")
    update_race_details(db)
    print("Summarize races")
    summarize_races(db)
    print("Get top raised PACs")
    get_top_raised_pacs(db)
    print("Update candidate expenditures")
    update_candidates_expenditures(db)
    print("Get ads")
    get_ads(db)


if __name__ == "__main__":
    main()
