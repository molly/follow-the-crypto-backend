from Database import Database
from committee_contributions import update_committee_contributions
import google.cloud.logging


def main():
    client = google.cloud.logging.Client()
    client.setup_logging()

    db = Database()
    db.get_constants()
    update_committee_contributions(db)


if __name__ == "__main__":
    main()
