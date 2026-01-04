#!/usr/bin/env python3
"""
Command to add a new company to track and fetch their initial data.

Usage:
    python -m commands.add_company --id "example-corp" --name "Example Corp" --category "exchange"
    python -m commands.add_company --id "crypto-ventures" --name "Crypto Ventures" --category "capital" --country "USA"
"""

import argparse
import logging
from Database import Database
from company_spending import update_spending_by_company
from company_utils import update_company_contributions_selective


def update_specific_companies(db, company_ids):
    """
    Update only the specific companies.
    More efficient than reprocessing all companies.
    """
    if not company_ids:
        return set()

    logging.info(f"Updating specific companies: {company_ids}")

    # Update just these specific companies
    original_companies = db.companies.copy()
    temp_companies = {cid: db.companies[cid] for cid in company_ids}
    db.companies = temp_companies

    try:
        # Update company spending for just these companies
        update_spending_by_company(db)

        # Restore full companies list
        db.companies = original_companies

        # Now update the company documents with relatedIndividuals
        for company_id in company_ids:
            company = db.companies[company_id]
            related_individuals = [
                individual
                for ind_id, individual in db.individuals.items()
                if company["name"] in individual.get("company", [])
            ]
            related_individuals.sort(key=lambda x: x.get("title", "zzz"))

            # Update the company document
            db.client.collection("companies").document(company_id).set(
                {
                    **company,
                    "relatedIndividuals": related_individuals,
                },
                merge=True
            )

        # Process contributions for just these companies
        return update_company_contributions_selective(db, company_ids)

    finally:
        # Ensure companies list is restored
        db.companies = original_companies


def add_company(company_id: str, company_data: dict, fetch_immediately: bool = True):
    """
    Add a new company to track and optionally fetch their data immediately.

    Args:
        company_id: Unique identifier for the company
        company_data: Dictionary containing company details (must include 'id' field)
        fetch_immediately: Whether to fetch contribution data immediately

    Returns:
        dict: Summary of the operation
    """
    db = Database()
    db.get_constants()

    # Ensure the company_data has the required 'id' field
    if "id" not in company_data:
        company_data["id"] = company_id

    # Check if company already exists
    if company_id in db.companies:
        raise ValueError(f"Company '{company_id}' already exists.")

    # Add to constants collection
    current_companies = db.companies.copy()
    current_companies[company_id] = company_data

    # Update Firestore
    db.client.collection("constants").document("companies").set(current_companies)

    # Update local cache
    db.companies = current_companies

    result = {
        "company_id": company_id,
        "added": True,
        "data_fetched": False,
        "contributions_processed": False,
        "individuals_linked": False
    }

    if fetch_immediately:
        logging.info(f"Fetching contribution data for {company_id}")

        try:
            # Update this specific company
            new_recipients = update_specific_companies(db, [company_id])

            result["data_fetched"] = True
            result["contributions_processed"] = True
            result["new_recipients"] = list(new_recipients)

            # Check if any individuals are linked to this company
            company_name = company_data.get("name")
            if company_name:
                linked_individuals = [
                    ind_id
                    for ind_id, individual in db.individuals.items()
                    if company_name in individual.get("company", [])
                ]
                if linked_individuals:
                    result["individuals_linked"] = True
                    result["linked_individuals"] = linked_individuals
                    logging.info(f"Found {len(linked_individuals)} individuals linked to {company_name}")

        except Exception as e:
            logging.error(f"Error fetching company data: {e}")
            raise e

    return result


def main():
    parser = argparse.ArgumentParser(description="Add a new company to track")
    parser.add_argument("--id", required=True, help="Unique identifier (e.g., 'example-corp')")
    parser.add_argument("--name", required=True, help="Company name")
    parser.add_argument("--search-id", help="Search identifier for FEC queries (optional)")
    parser.add_argument("--category", action="append", help="Company category (can be specified multiple times: exchange, capital, etc.)")
    parser.add_argument("--description", help="Company description")
    parser.add_argument("--country", default="USA", help="Country (defaults to USA)")
    parser.add_argument("--no-fetch", action="store_true", help="Don't fetch data immediately")

    args = parser.parse_args()

    # Build company data - match existing structure exactly
    company_data = {
        "id": args.id,
        "name": args.name,
        "country": args.country,
    }

    # Add optional fields
    if args.search_id:
        company_data["search_id"] = args.search_id
    else:
        # Default search_id to lowercase name
        company_data["search_id"] = args.name.lower()

    if args.category:
        company_data["category"] = args.category
    else:
        # Require at least one category
        parser.error("At least one --category is required")

    if args.description:
        company_data["description"] = args.description

    # Set up logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    try:
        result = add_company(args.id, company_data, fetch_immediately=not args.no_fetch)
        print(f"Successfully added company '{args.id}'")

        if result["data_fetched"]:
            print(f"Fetched and processed company data")

        if result["contributions_processed"]:
            print(f"Processed contributions, found {len(result.get('new_recipients', []))} new recipients")

        if result.get("individuals_linked"):
            linked = result.get('linked_individuals', [])
            print(f"Found {len(linked)} linked individuals: {', '.join(linked)}")

        if not result["data_fetched"]:
            print("Company added to constants. Run pipeline to fetch contribution data.")

    except Exception as e:
        print(f"Error adding company: {e}")
        raise


if __name__ == "__main__":
    main()
