#!/usr/bin/env python3
"""
Command to add a new committee to track and fetch their initial data.

Usage:
    python -m commands.add_committee --id "C00123456" --name "Example PAC" --description "A PAC focused on crypto"
    python -m commands.add_committee --id "C00654321" --name "Tech Freedom Fund"
"""

import argparse
import logging
from Database import Database
from utils import FEC_fetch, pick
import requests


def hydrate_committee(db, committee_id, committee_data, session=None):
    """
    Fetch committee details and totals from FEC API for a specific committee.
    Similar to the hydrate_committees task but for a single committee.
    """
    if session is None:
        session = requests.Session()

    committee_processed = {
        "details_fetched": False,
        "totals_fetched": False,
        "data": None,
    }

    # Fetch committee details
    details_data = FEC_fetch(
        session,
        "committee details",
        "https://api.open.fec.gov/v1/committee/" + committee_id,
    )

    if details_data and "results" in details_data and details_data["results"]:
        details = details_data["results"][0]
        picked = pick(
            details,
            [
                "affiliated_committee_name",
                "candidate_ids",
                "committee_type",
                "committee_type_full",
                "cycles",
                "designation",
                "designation_full",
                "first_f1_date",
                "leadership_pac",
                "organization_type",
                "organization_type_full",
                "party",
                "party_full",
                "party_type",
                "party_type_full",
                "sponsor_candidate_ids",
                "website",
            ],
        )
        picked["fec_name"] = details["name"]
        full_committee_data = {**committee_data, **picked}
        committee_processed["details_fetched"] = True

        # Fetch committee totals
        totals_data = FEC_fetch(
            session,
            "committee totals",
            "https://api.open.fec.gov/v1/committee/{}/totals".format(committee_id),
            params={"cycle": 2026},
        )

        if (
            totals_data
            and "results" in totals_data
            and len(totals_data["results"])
            and totals_data["results"][0]
        ):
            totals = totals_data["results"][0]
            full_committee_data.update(
                **pick(
                    totals,
                    [
                        "contributions",
                        "contribution_refunds",
                        "disbursements",
                        "last_cash_on_hand_end_period",
                        "net_contributions",
                        "receipts",
                        "independent_expenditures",
                    ],
                ),
            )
            committee_processed["totals_fetched"] = True

        # Save to Firestore committees collection
        db.client.collection("committees").document(committee_id).set(full_committee_data)
        committee_processed["data"] = full_committee_data

        logging.info(
            f"Committee {committee_id} hydrated - Details: {committee_processed['details_fetched']}, "
            f"Totals: {committee_processed['totals_fetched']}"
        )
    else:
        logging.warning(f"Could not fetch details for committee {committee_id}")

    return committee_processed


def add_committee(committee_id: str, committee_data: dict, fetch_immediately: bool = True):
    """
    Add a new committee to track and optionally fetch their data immediately.

    Args:
        committee_id: FEC committee ID (e.g., 'C00123456')
        committee_data: Dictionary containing committee details (must include 'id' field)
        fetch_immediately: Whether to fetch FEC data immediately

    Returns:
        dict: Summary of the operation
    """
    db = Database()
    db.get_constants()

    # Validate committee ID format (FEC committee IDs start with 'C' followed by 8 digits)
    if not committee_id.startswith('C') or len(committee_id) != 9:
        logging.warning(f"Committee ID '{committee_id}' doesn't match standard FEC format (C########)")

    # Ensure the committee_data has the required 'id' field
    if "id" not in committee_data:
        committee_data["id"] = committee_id

    # Check if committee already exists
    if committee_id in db.committees:
        raise ValueError(f"Committee '{committee_id}' already exists.")

    # Add to constants collection
    current_committees = db.committees.copy()
    current_committees[committee_id] = committee_data

    # Update Firestore
    db.client.collection("constants").document("committees").set(current_committees)

    # Update local cache
    db.committees = current_committees

    result = {
        "committee_id": committee_id,
        "added": True,
        "data_fetched": False,
        "details_fetched": False,
        "totals_fetched": False,
    }

    if fetch_immediately:
        logging.info(f"Fetching FEC data for committee {committee_id}")

        try:
            # Hydrate this specific committee with FEC data
            committee_processed = hydrate_committee(db, committee_id, committee_data)

            result["data_fetched"] = True
            result["details_fetched"] = committee_processed["details_fetched"]
            result["totals_fetched"] = committee_processed["totals_fetched"]

            if committee_processed["data"]:
                result["fetched_data"] = {
                    "fec_name": committee_processed["data"].get("fec_name"),
                    "committee_type": committee_processed["data"].get("committee_type_full"),
                    "party": committee_processed["data"].get("party_full"),
                    "receipts": committee_processed["data"].get("receipts"),
                }

        except Exception as e:
            logging.error(f"Error fetching committee data: {e}")
            raise e

    return result


def main():
    parser = argparse.ArgumentParser(description="Add a new committee to track")
    parser.add_argument("--id", required=True, help="FEC Committee ID (e.g., 'C00123456')")
    parser.add_argument("--name", required=True, help="Committee name")
    parser.add_argument("--description", help="Committee description (HTML allowed)")
    parser.add_argument("--no-fetch", action="store_true", help="Don't fetch FEC data immediately")

    args = parser.parse_args()

    # Build committee data - match existing structure exactly
    committee_data = {
        "id": args.id,
        "name": args.name,
    }

    if args.description:
        committee_data["description"] = args.description

    # Set up logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    try:
        result = add_committee(args.id, committee_data, fetch_immediately=not args.no_fetch)
        print(f"Successfully added committee '{args.id}' to constants")

        if result["data_fetched"]:
            print(f"Fetched FEC data:")
            if result["details_fetched"]:
                print(f"   Committee details fetched")
            if result["totals_fetched"]:
                print(f"   Financial totals fetched")

            if "fetched_data" in result:
                data = result["fetched_data"]
                if data.get("fec_name"):
                    print(f"   FEC Name: {data['fec_name']}")
                if data.get("committee_type"):
                    print(f"   Type: {data['committee_type']}")
                if data.get("party"):
                    print(f"   Party: {data['party']}")
                if data.get("receipts"):
                    print(f"   Receipts: ${data['receipts']:,.2f}")

        if not result["data_fetched"]:
            print("Committee added to constants. Run pipeline to fetch FEC data.")

    except Exception as e:
        print(f"Error adding committee: {e}")
        raise


if __name__ == "__main__":
    main()
