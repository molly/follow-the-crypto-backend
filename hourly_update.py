import google.cloud.logging
from Database import Database

from fetch_committee_contributions import (
    update_recent_committee_contributions,
    update_committee_contributions,
)
from process_committee_contributions import process_committee_contributions


def main():
    """Do hourly update."""
    client = google.cloud.logging.Client()
    client.setup_logging()

    db = Database()
    db.get_constants()
    update_recent_committee_contributions(db)
    process_committee_contributions(db)


if __name__ == "__main__":
    main()
