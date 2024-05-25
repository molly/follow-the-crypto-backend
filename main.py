import google.cloud.logging
from Database import Database

from committee_details import hydrate_committees
from committee_contributions import update_committee_contributions
from committee_expenditures import update_committee_expenditures
from races import update_race_details
from tmp import tmp


def main():
    client = google.cloud.logging.Client()
    client.setup_logging()

    db = Database()
    db.get_constants()
    tmp(db)
    # hydrate_committees(db)
    # update_committee_contributions(db)
    # update_committee_expenditures(db)
    # update_race_details(db)


if __name__ == "__main__":
    main()
