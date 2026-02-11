#!/usr/bin/env python3
"""
Command to fetch contribution data for an existing individual.

Usage:
    python -m commands.fetch_individual --id "john-doe"
    python -m commands.fetch_individual --id "jane-smith" --force
"""

import argparse
import logging
from Database import Database
from individuals import update_spending_by_individuals
from process_individual_contributions import process_individual_contributions
from company_spending import update_spending_by_company
from process_company_contributions import process_company_contributions
# Import the selective update functions from add_individual
from commands.add_individual import update_specific_companies


def fetch_individual_data(individual_id: str, force: bool = False):
    """
    Fetch contribution data for a specific individual.
    
    Args:
        individual_id: The ID of the individual to fetch data for
        force: Whether to refetch even if data already exists
    
    Returns:
        dict: Summary of the operation
    """
    db = Database()
    db.get_constants()
    
    # Check if individual exists
    if individual_id not in db.individuals:
        raise ValueError(f"Individual '{individual_id}' not found in constants")
    
    individual_data = db.individuals[individual_id]
    
    # Check if data already exists
    existing_data = db.client.collection("rawIndividualContributions").document(individual_id).get()
    if existing_data.exists and not force:
        existing_contribs = existing_data.to_dict().get("contributions", [])
        if existing_contribs:
            print(f"ℹ️  Individual '{individual_id}' already has {len(existing_contribs)} contributions. Use --force to refetch.")
            return {
                "individual_id": individual_id,
                "skipped": True,
                "existing_contributions": len(existing_contribs)
            }
    
    logging.info(f"Fetching contribution data for {individual_id}")
    
    # Create a temporary individuals dict with just this person
    temp_individuals = {individual_id: individual_data}
    original_individuals = db.individuals
    db.individuals = temp_individuals
    
    try:
        # Fetch their contributions
        new_contributions = update_spending_by_individuals(db)
        
        # Process the contributions
        new_recipients = process_individual_contributions(db)
        
        # Update company data if this person is associated with companies
        company_new_recipients = set()
        companies_updated = False
        optimization = "none"
        if "company" in individual_data and individual_data["company"]:
            logging.info(f"Selectively updating companies: {individual_data['company']}")
            
            # Use selective update for daily operations efficiency
            company_new_recipients = update_specific_companies(db, individual_data, individual_id)
            companies_updated = True
            optimization = "selective_company_update"
        
        return {
            "individual_id": individual_id,
            "data_fetched": True,
            "new_contributions_count": len(new_contributions),
            "contributions_processed": True,
            "new_recipients": list(new_recipients),
            "companies_updated": companies_updated,
            "company_new_recipients": list(company_new_recipients),
            "optimization": optimization
        }
        
    finally:
        # Restore full individuals list
        db.individuals = original_individuals


def main():
    parser = argparse.ArgumentParser(description="Fetch contribution data for an individual")
    parser.add_argument("--id", required=True, help="Individual ID to fetch data for")
    parser.add_argument("--force", action="store_true", help="Refetch even if data exists")
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    try:
        result = fetch_individual_data(args.id, force=args.force)
        
        if result.get("skipped"):
            print(f"⏭️  Skipped '{args.id}' (already has {result['existing_contributions']} contributions)")
        else:
            print(f"✅ Successfully fetched data for '{args.id}'")
            print(f"📊 Fetched {result['new_contributions_count']} contributions")
            print(f"🔄 Found {len(result['new_recipients'])} new recipients")
            
            if result.get("companies_updated"):
                companies = []
                # Get company info from database
                if individual_id in db.individuals:
                    companies = db.individuals[individual_id].get('company', [])
                optimization = result.get('optimization', 'full_reprocess')
                print(f"🏢 Updated {len(companies)} company(ies), found {len(result.get('company_new_recipients', []))} new company recipients")
                print(f"ℹ️  Optimization: {optimization}")
            
    except Exception as e:
        print(f"❌ Error fetching individual data: {e}")
        raise


if __name__ == "__main__":
    main()