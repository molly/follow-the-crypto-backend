#!/usr/bin/env python3
"""
Optimized batch processing for daily operations.

This command is designed for efficient daily pipeline runs where you want to:
1. Add multiple individuals without immediate processing
2. Batch process all new individuals together  
3. Minimize redundant company reprocessing

Usage:
    python -m commands.batch_daily --add "person1:Company A" --add "person2:Company B"
    python -m commands.batch_daily --process-pending
    python -m commands.batch_daily --add "person1:Company A" --process-immediately
"""

import argparse
import logging
import requests
import time
from Database import Database
from individuals import update_spending_by_individuals
from process_individual_contributions import process_individual_contributions
from company_spending import update_spending_by_company
from process_company_contributions import process_company_contributions


def add_individuals_batch(individuals_data, process_immediately=False):
    """
    Add multiple individuals efficiently for daily operations.
    
    Args:
        individuals_data: List of tuples (individual_id, individual_data)
        process_immediately: Whether to process all at once after adding
    
    Returns:
        dict: Summary of operations
    """
    db = Database()
    db.get_constants()
    
    # Add all individuals to constants first
    current_individuals = db.individuals.copy()
    added_individuals = []
    
    for individual_id, individual_data in individuals_data:
        if individual_id in current_individuals:
            logging.warning(f"Individual '{individual_id}' already exists, skipping")
            continue
            
        # Ensure id field is set
        if "id" not in individual_data:
            individual_data["id"] = individual_id
            
        current_individuals[individual_id] = individual_data
        added_individuals.append((individual_id, individual_data))
    
    if not added_individuals:
        return {"status": "no_new_individuals", "added_count": 0}
    
    # Update constants in Firestore
    db.client.collection("constants").document("individuals").set(current_individuals)
    db.individuals = current_individuals
    
    logging.info(f"Added {len(added_individuals)} individuals to constants")
    
    result = {
        "added_individuals": [ind_id for ind_id, _ in added_individuals],
        "added_count": len(added_individuals),
        "status": "added",
        "processed": False
    }
    
    if process_immediately:
        result.update(process_pending_individuals(db, [ind_id for ind_id, _ in added_individuals]))
    
    return result


def process_pending_individuals(db=None, specific_individuals=None):
    """
    Process individuals that have been added but not yet processed.
    Optimized for daily batch operations.
    """
    if db is None:
        db = Database()
        db.get_constants()
    
    # Determine which individuals to process
    if specific_individuals:
        individuals_to_process = specific_individuals
    else:
        # Find individuals without processed data
        individuals_to_process = []
        for ind_id in db.individuals.keys():
            existing_data = db.client.collection("individuals").document(ind_id).get()
            if not existing_data.exists or not existing_data.to_dict().get("contributions"):
                individuals_to_process.append(ind_id)
    
    if not individuals_to_process:
        return {
            "status": "no_pending_individuals",
            "processed_count": 0,
            "companies_updated": 0
        }
    
    logging.info(f"Processing {len(individuals_to_process)} individuals: {individuals_to_process}")
    start_time = time.time()
    session = requests.Session()

    # Step 1: Fetch contribution data for these individuals
    temp_individuals = {ind_id: db.individuals[ind_id] for ind_id in individuals_to_process}
    original_individuals = db.individuals
    db.individuals = temp_individuals
    
    try:
        new_contributions = update_spending_by_individuals(db, session)
        logging.info(f"Fetched {len(new_contributions)} total contributions")
    finally:
        db.individuals = original_individuals
    
    # Step 2: Process individual contributions
    new_recipients = process_individual_contributions(db, session)
    logging.info(f"Found {len(new_recipients)} new recipients")
    
    # Step 3: Update company data efficiently
    affected_companies = get_affected_companies(db, individuals_to_process)
    companies_updated = 0
    
    if affected_companies:
        logging.info(f"Updating {len(affected_companies)} affected companies: {list(affected_companies)}")
        
        # Update company data selectively
        companies_updated = update_companies_selective(db, affected_companies)
    
    elapsed_time = time.time() - start_time
    
    return {
        "status": "processed",
        "processed": True,
        "processed_individuals": individuals_to_process,
        "processed_count": len(individuals_to_process),
        "new_contributions": len(new_contributions),
        "new_recipients": len(new_recipients),
        "affected_companies": list(affected_companies),
        "companies_updated": companies_updated,
        "elapsed_time_seconds": round(elapsed_time, 2),
        "optimization": "batch_selective_processing"
    }


def get_affected_companies(db, individual_ids):
    """
    Get the set of companies that need updating based on the individuals processed.
    """
    affected_companies = set()
    
    for ind_id in individual_ids:
        if ind_id in db.individuals:
            individual = db.individuals[ind_id]
            if "company" in individual:
                affected_companies.update(individual["company"])
    
    return affected_companies


def update_companies_selective(db, company_names):
    """
    Update only the specific companies that are affected by the new individuals.
    """
    # Find company IDs that match the names
    company_ids_to_update = []
    for company_id, company in db.companies.items():
        if company["name"] in company_names:
            company_ids_to_update.append(company_id)
    
    if not company_ids_to_update:
        logging.warning(f"No company IDs found for names: {company_names}")
        return 0
    
    # Update company spending for these companies
    original_companies = db.companies.copy()
    temp_companies = {cid: db.companies[cid] for cid in company_ids_to_update}
    db.companies = temp_companies
    
    session = requests.Session()
    try:
        update_spending_by_company(db, session)
    finally:
        db.companies = original_companies

    # Process company contributions for these specific companies
    from commands.add_individual import update_company_contributions_selective
    update_company_contributions_selective(db, company_ids_to_update)
    
    return len(company_ids_to_update)


def parse_individual_spec(spec):
    """
    Parse individual specification: "id:Company Name" or "id:Company1,Company2" or just "id"
    """
    parts = spec.split(":", 1)
    individual_id = parts[0].strip()
    
    individual_data = {
        "id": individual_id,
        "name": individual_id.replace("-", " ").title()
    }
    
    if len(parts) > 1 and parts[1].strip():
        companies = [c.strip() for c in parts[1].split(",") if c.strip()]
        if companies:
            individual_data["company"] = companies
    
    return individual_id, individual_data


def main():
    parser = argparse.ArgumentParser(description="Batch processing for daily operations")
    parser.add_argument("--add", action="append", help="Add individual (format: 'id:Company Name' or 'id:Company1,Company2')")
    parser.add_argument("--process-pending", action="store_true", help="Process individuals that haven't been processed yet")
    parser.add_argument("--process-immediately", action="store_true", help="Process individuals immediately after adding")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    # Set up logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")
    
    try:
        total_result = {"operations": []}
        
        # Add individuals if specified
        if args.add:
            individuals_data = []
            for spec in args.add:
                individual_id, individual_data = parse_individual_spec(spec)
                individuals_data.append((individual_id, individual_data))
            
            print(f"📝 Adding {len(individuals_data)} individuals...")
            add_result = add_individuals_batch(individuals_data, args.process_immediately)
            total_result["operations"].append({"type": "add", "result": add_result})
            
            if add_result["added_count"] > 0:
                print(f"✅ Added {add_result['added_count']} individuals: {', '.join(add_result['added_individuals'])}")
                
                if add_result.get("processed"):
                    print(f"⚡ Processed immediately (optimization: {add_result.get('optimization', 'unknown')})")
                    print(f"📊 Total contributions: {add_result.get('new_contributions', 0)}")
                    print(f"🏢 Companies updated: {add_result.get('companies_updated', 0)}")
                    print(f"⏱️  Elapsed time: {add_result.get('elapsed_time_seconds', 0)}s")
            else:
                print("ℹ️  No new individuals were added (may already exist)")
        
        # Process pending individuals if specified
        if args.process_pending and not args.process_immediately:
            print("⚙️  Processing pending individuals...")
            process_result = process_pending_individuals()
            total_result["operations"].append({"type": "process", "result": process_result})
            
            if process_result["processed_count"] > 0:
                print(f"✅ Processed {process_result['processed_count']} individuals")
                print(f"📊 New contributions: {process_result['new_contributions']}")
                print(f"🏢 Companies updated: {process_result['companies_updated']}")
                print(f"⏱️  Elapsed time: {process_result['elapsed_time_seconds']}s")
                print(f"🚀 Optimization: {process_result['optimization']}")
            else:
                print("ℹ️  No pending individuals found to process")
        
        # Show usage tips for daily operations
        if not args.add and not args.process_pending:
            print("💡 Daily Operations Tips:")
            print("   Add without processing: --add 'person-id:Company Name'")
            print("   Process all pending: --process-pending")
            print("   Add and process immediately: --add 'person-id:Company' --process-immediately")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error in batch processing: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())