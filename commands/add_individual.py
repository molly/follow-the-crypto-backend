#!/usr/bin/env python3
"""
Command to add a new individual to track and fetch their initial data.

Usage:
    python -m commands.add_individual --id "john-doe" --name "John Doe" --zip "12345"
    python -m commands.add_individual --id "jane-smith" --employer-search "Coinbase,Crypto Corp"
"""

import argparse
import logging
from Database import Database
from individuals import update_spending_by_individuals
from process_individual_contributions import process_individual_contributions
from company_spending import update_spending_by_company
from process_company_contributions import process_company_contributions
from company_spending import update_spending_by_company
from process_company_contributions import process_company_contributions


def add_individual(individual_id: str, individual_data: dict, fetch_immediately: bool = True):
    """
    Add a new individual to track and optionally fetch their data immediately.
    
    Args:
        individual_id: Unique identifier for the individual
        individual_data: Dictionary containing individual details (must include 'id' field)
        fetch_immediately: Whether to fetch contribution data immediately
    
    Returns:
        dict: Summary of the operation
    """
    db = Database()
    db.get_constants()
    
    # Ensure the individual_data has the required 'id' field
    if "id" not in individual_data:
        individual_data["id"] = individual_id
    
    # Check if individual already exists
    if individual_id in db.individuals:
        raise ValueError(f"Individual '{individual_id}' already exists. Use fetch_individual to update their data.")
    
    # Add to constants collection
    current_individuals = db.individuals.copy()
    current_individuals[individual_id] = individual_data
    
    # Update Firestore
    db.client.collection("constants").document("individuals").set(current_individuals)
    
    # Update local cache
    db.individuals = current_individuals
    
    result = {
        "individual_id": individual_id,
        "added": True,
        "data_fetched": False,
        "contributions_processed": False,
        "companies_updated": False
    }
    
    if fetch_immediately:
        logging.info(f"Fetching contribution data for {individual_id}")
        
        # Create a temporary individuals dict with just this person
        temp_individuals = {individual_id: individual_data}
        original_individuals = db.individuals
        db.individuals = temp_individuals
        
        try:
            # Fetch their contributions
            new_contributions = update_spending_by_individuals(db)
            result["data_fetched"] = True
            result["new_contributions_count"] = len(new_contributions)
            
            # Restore full individuals list for processing
            db.individuals = original_individuals
            
            # Process the contributions
            new_recipients = process_individual_contributions(db)
            result["contributions_processed"] = True
            result["new_recipients"] = list(new_recipients)
            
            # Update company data if this person is associated with companies
            if "company" in individual_data and individual_data["company"]:
                logging.info(f"Updating company data for associated companies: {individual_data['company']}")
                
                # Need to update company spending to refresh relatedIndividuals
                update_spending_by_company(db)
                
                # Process company contributions to include this individual's data
                company_new_recipients = process_company_contributions(db)
                
                result["companies_updated"] = True
                result["company_new_recipients"] = list(company_new_recipients)
                
        except Exception as e:
            # Restore full individuals list in case of error
            db.individuals = original_individuals
            raise e
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Add a new individual to track")
    parser.add_argument("--id", required=True, help="Unique identifier (e.g., 'john-doe')")
    parser.add_argument("--name", help="Display name (optional, defaults to formatted ID)")
    parser.add_argument("--name-search", help="Alternative name for searching")
    parser.add_argument("--zip", help="ZIP code for contribution search")
    parser.add_argument("--city", help="City for efiled contribution search")
    parser.add_argument("--employer-search", help="Comma-separated list of employers to search (for employerSearch field)")
    parser.add_argument("--company", help="Comma-separated list of associated companies (for company field)")
    parser.add_argument("--title", help="Job title")
    parser.add_argument("--photo-credit", help="Photo credit URL (for photoCredit field)")
    parser.add_argument("--no-fetch", action="store_true", help="Don't fetch data immediately")
    
    args = parser.parse_args()
    
    # Build individual data - match existing structure exactly
    individual_data = {
        "id": args.id,  # Always include id field to match existing structure
    }
    
    # Set name - use provided name or derive from ID
    individual_data["name"] = args.name if args.name else args.id.replace("-", " ").title()
    
    if args.name_search:
        individual_data["nameSearch"] = args.name_search
    
    if args.zip:
        individual_data["zip"] = args.zip
    
    if args.city:
        individual_data["city"] = args.city
    
    if args.employer_search:
        # Split by comma and clean whitespace
        employers = [e.strip() for e in args.employer_search.split(",") if e.strip()]
        if employers:  # Only add if non-empty
            individual_data["employerSearch"] = employers
    
    if args.company:
        # Split by comma and clean whitespace - always an array
        companies = [c.strip() for c in args.company.split(",") if c.strip()]
        if companies:  # Only add if non-empty
            individual_data["company"] = companies
    
    if args.title:
        individual_data["title"] = args.title
    
    if args.photo_credit:
        individual_data["photoCredit"] = args.photo_credit
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    try:
        result = add_individual(args.id, individual_data, fetch_immediately=not args.no_fetch)
        print(f"✅ Successfully added individual '{args.id}'")
        
        if result["data_fetched"]:
            print(f"📊 Fetched {result['new_contributions_count']} contributions")
        
        if result["contributions_processed"]:
            print(f"🔄 Processed contributions, found {len(result['new_recipients'])} new recipients")
        
        if result.get("companies_updated"):
            print(f"🏢 Updated company data, found {len(result.get('company_new_recipients', []))} new company recipients")
            print(f"ℹ️  Companies associated with {args.id}: {', '.join(individual_data.get('company', []))}")
        
        if result.get("companies_updated"):
            print(f"🏢 Updated company data, found {len(result.get('company_new_recipients', []))} new company recipients")
            print(f"ℹ️  Companies associated with {args.id}: {', '.join(individual_data.get('company', []))}")
        
        if not result["data_fetched"]:
            print("ℹ️  Use 'python -m commands.fetch_individual --id {}' to fetch contribution data later".format(args.id))
            
    except Exception as e:
        print(f"❌ Error adding individual: {e}")
        raise


if __name__ == "__main__":
    main()