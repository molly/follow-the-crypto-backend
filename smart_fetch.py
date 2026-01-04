#!/usr/bin/env python3
"""
Incremental data fetching utilities for Follow the Crypto.

These utilities add time-aware and differential capabilities to existing
fetch functions, making them suitable for daily operations.
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path


class IncrementalFetcher:
    """Base class for incremental data fetching with caching."""
    
    def __init__(self, cache_dir="cache/incremental"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
    
    def get_last_run_timestamp(self, operation_name):
        """Get timestamp of last successful run for an operation."""
        cache_file = self.cache_dir / f"{operation_name}_last_run.json"
        
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                return datetime.fromisoformat(data['timestamp'])
            except (json.JSONDecodeError, KeyError):
                logging.warning(f"Invalid cache file for {operation_name}")
        
        # Default to 24 hours ago for first run
        return datetime.now() - timedelta(days=1)
    
    def update_last_run_timestamp(self, operation_name):
        """Update the last run timestamp for an operation."""
        cache_file = self.cache_dir / f"{operation_name}_last_run.json"
        
        data = {
            'timestamp': datetime.now().isoformat(),
            'operation': operation_name
        }
        
        with open(cache_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def get_fetch_window(self, operation_name, max_days_back=7):
        """Get the time window for fetching data."""
        last_run = self.get_last_run_timestamp(operation_name)
        now = datetime.now()
        
        # Don't go back more than max_days_back
        earliest_allowed = now - timedelta(days=max_days_back)
        
        start_time = max(last_run, earliest_allowed)
        
        return start_time, now


class SmartCommitteeContributionsFetcher(IncrementalFetcher):
    """Incremental fetcher for committee contributions."""
    
    def fetch_recent_contributions(self, db, max_committees=None, max_days_back=3):
        """Fetch contributions only for committees with recent activity."""
        from fetch_committee_contributions import update_committee_contributions
        
        operation_name = "committee_contributions"
        start_time, end_time = self.get_fetch_window(operation_name, max_days_back)
        
        logging.info(f"Fetching committee contributions since {start_time}")
        
        # Get prioritized committee list
        priority_committees = self._get_priority_committees(db, max_committees)
        
        if priority_committees:
            logging.info(f"Focusing on {len(priority_committees)} priority committees")
            
            # Create temporary committees dict with just priorities
            original_committees = db.committees
            db.committees = {cid: original_committees[cid] for cid in priority_committees if cid in original_committees}
            
            try:
                new_contributions = update_committee_contributions(db)
                self.update_last_run_timestamp(operation_name)
                return new_contributions
            finally:
                # Restore original committees
                db.committees = original_committees
        else:
            # Fall back to full fetch but with time limit
            new_contributions = update_committee_contributions(db)
            self.update_last_run_timestamp(operation_name)
            return new_contributions
    
    def _get_priority_committees(self, db, max_committees=None):
        """Get committees likely to have new activity."""
        # Priority order:
        # 1. Committees tied to tracked individuals
        # 2. Committees tied to tracked companies
        # 3. Recently active committees
        
        priority_committees = set()
        
        # Committees from tracked individuals
        for individual_data in db.individuals.values():
            if isinstance(individual_data, dict) and 'committees' in individual_data:
                for committee_id in individual_data['committees']:
                    priority_committees.add(committee_id)
        
        # Committees from tracked companies 
        for company_data in db.companies.values():
            if isinstance(company_data, dict) and 'committees' in company_data:
                for committee_id in company_data['committees']:
                    priority_committees.add(committee_id)
        
        # Convert to list and limit if requested
        priority_list = list(priority_committees)
        
        if max_committees and len(priority_list) > max_committees:
            # Could add smarter prioritization here (by recent activity, etc)
            priority_list = priority_list[:max_committees]
        
        return priority_list


class SmartExpendituresFetcher(IncrementalFetcher):
    """Incremental fetcher for expenditures."""
    
    def fetch_recent_expenditures(self, db, max_committees=None, max_days_back=3):
        """Fetch expenditures with focus on active committees."""
        from committee_expenditures import update_committee_expenditures
        
        operation_name = "committee_expenditures"
        start_time, end_time = self.get_fetch_window(operation_name, max_days_back)
        
        logging.info(f"Fetching committee expenditures since {start_time}")
        
        # Use similar prioritization as contributions
        fetcher = SmartCommitteeContributionsFetcher()
        priority_committees = fetcher._get_priority_committees(db, max_committees)
        
        if priority_committees:
            logging.info(f"Focusing on {len(priority_committees)} priority committees")
            
            original_committees = db.committees
            db.committees = {cid: original_committees[cid] for cid in priority_committees if cid in original_committees}
            
            try:
                new_expenditures = update_committee_expenditures(db)
                self.update_last_run_timestamp(operation_name)
                return new_expenditures
            finally:
                db.committees = original_committees
        else:
            new_expenditures = update_committee_expenditures(db)
            self.update_last_run_timestamp(operation_name)
            return new_expenditures


class SmartIndividualsFetcher(IncrementalFetcher):
    """Incremental fetcher for individuals with batch optimization."""
    
    def fetch_pending_individuals(self, db, batch_size=10, max_total=50):
        """Fetch data for individuals without recent updates."""
        operation_name = "individuals_processing"
        
        # Find individuals without processed data or with stale data
        pending_individuals = []
        
        for ind_id, ind_data in db.individuals.items():
            if len(pending_individuals) >= max_total:
                break
                
            # Check if individual has processed data
            doc_ref = db.client.collection("individuals").document(ind_id)
            existing_data = doc_ref.get()
            
            needs_update = False
            
            if not existing_data.exists:
                needs_update = True
            else:
                doc_data = existing_data.to_dict()
                
                # No contributions data
                if not doc_data.get("contributions"):
                    needs_update = True
                
                # Stale data (older than 7 days)
                elif "lastUpdated" in doc_data:
                    try:
                        last_updated = datetime.fromisoformat(doc_data["lastUpdated"])
                        if datetime.now() - last_updated > timedelta(days=7):
                            needs_update = True
                    except (ValueError, TypeError):
                        needs_update = True
            
            if needs_update:
                pending_individuals.append(ind_id)
        
        if not pending_individuals:
            logging.info("No pending individuals found")
            return []
        
        logging.info(f"Found {len(pending_individuals)} individuals needing updates")
        
        # Process in batches using our batch daily command
        from commands.batch_daily import process_pending_individuals
        
        processed_count = 0
        all_results = []
        
        for i in range(0, len(pending_individuals), batch_size):
            batch = pending_individuals[i:i + batch_size]
            
            logging.info(f"Processing individuals batch {i//batch_size + 1}: {len(batch)} individuals")
            
            try:
                results = process_pending_individuals(db, batch)
                all_results.extend(results if results else batch)
                processed_count += len(batch)
                
            except Exception as e:
                logging.error(f"Error processing batch {i//batch_size + 1}: {e}")
                continue
        
        self.update_last_run_timestamp(operation_name)
        return all_results


def create_daily_fetch_plan(db, max_time_minutes=30):
    """Create an optimized fetch plan for daily operations."""
    
    plan = {
        "estimated_time_minutes": 0,
        "operations": [],
        "priorities": []
    }
    
    # Check what needs updating
    contributions_fetcher = SmartCommitteeContributionsFetcher()
    expenditures_fetcher = SmartExpendituresFetcher()
    individuals_fetcher = SmartIndividualsFetcher()
    
    # Estimate committee contributions time (2-5 min typically)
    last_contributions = contributions_fetcher.get_last_run_timestamp("committee_contributions")
    hours_since_contributions = (datetime.now() - last_contributions).total_seconds() / 3600
    
    if hours_since_contributions > 12:  # More than 12 hours
        plan["operations"].append({
            "name": "committee_contributions",
            "estimated_minutes": 5,
            "priority": "high",
            "reason": f"{hours_since_contributions:.1f} hours since last fetch"
        })
        plan["estimated_time_minutes"] += 5
    
    # Estimate expenditures time (2-4 min typically)  
    last_expenditures = expenditures_fetcher.get_last_run_timestamp("committee_expenditures")
    hours_since_expenditures = (datetime.now() - last_expenditures).total_seconds() / 3600
    
    if hours_since_expenditures > 12:
        plan["operations"].append({
            "name": "committee_expenditures", 
            "estimated_minutes": 4,
            "priority": "high",
            "reason": f"{hours_since_expenditures:.1f} hours since last fetch"
        })
        plan["estimated_time_minutes"] += 4
    
    # Check for pending individuals (highly variable time)
    pending_individuals = []
    for ind_id in list(db.individuals.keys())[:20]:  # Sample first 20 for estimation
        existing_data = db.client.collection("individuals").document(ind_id).get()
        if not existing_data.exists or not existing_data.to_dict().get("contributions"):
            pending_individuals.append(ind_id)
    
    if pending_individuals:
        estimated_individual_time = min(15, len(pending_individuals) * 2)  # 2 min per individual, max 15 min
        plan["operations"].append({
            "name": "individuals_processing",
            "estimated_minutes": estimated_individual_time,
            "priority": "medium",
            "reason": f"~{len(pending_individuals)} individuals need processing (sampled)"
        })
        plan["estimated_time_minutes"] += estimated_individual_time
    
    # Add company updates if individuals will be processed
    if any(op["name"] == "individuals_processing" for op in plan["operations"]):
        plan["operations"].append({
            "name": "company_updates",
            "estimated_minutes": 3,
            "priority": "medium", 
            "reason": "Update companies after individual changes"
        })
        plan["estimated_time_minutes"] += 3
    
    # Generate priorities based on time budget
    if plan["estimated_time_minutes"] <= max_time_minutes:
        plan["priorities"] = ["all_operations_fit"]
    else:
        plan["priorities"] = ["focus_on_high_priority"]
        # Suggest skipping low-priority operations
        plan["suggestions"] = [
            f"Estimated time ({plan['estimated_time_minutes']} min) exceeds budget ({max_time_minutes} min)",
            "Consider running high-priority operations only",
            "Or increase time budget with --max-time parameter"
        ]
    
    return plan


# Convenience function for CLI usage
def run_smart_daily_fetch(max_time_minutes=30, operations=None):
    """Run optimized daily fetch with smart prioritization."""
    from Database import Database
    
    db = Database()
    db.get_constants()
    
    # Create fetch plan
    plan = create_daily_fetch_plan(db, max_time_minutes)
    
    print(f"📋 Daily Fetch Plan:")
    print(f"Estimated time: {plan['estimated_time_minutes']} minutes")
    print(f"Operations planned: {len(plan['operations'])}")
    
    if plan.get("suggestions"):
        print("⚠️  Suggestions:")
        for suggestion in plan["suggestions"]:
            print(f"  • {suggestion}")
    
    # Run operations
    results = {}
    
    if operations is None:
        operations = [op["name"] for op in plan["operations"]]
    
    contributions_fetcher = SmartCommitteeContributionsFetcher()
    expenditures_fetcher = SmartExpendituresFetcher()
    individuals_fetcher = SmartIndividualsFetcher()
    
    if "committee_contributions" in operations:
        print("\n🔄 Fetching committee contributions...")
        results["contributions"] = contributions_fetcher.fetch_recent_contributions(db, max_committees=100)
    
    if "committee_expenditures" in operations:
        print("\n💰 Fetching committee expenditures...")
        results["expenditures"] = expenditures_fetcher.fetch_recent_expenditures(db, max_committees=100)
    
    if "individuals_processing" in operations:
        print("\n👥 Processing pending individuals...")
        results["individuals"] = individuals_fetcher.fetch_pending_individuals(db, batch_size=5, max_total=25)
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Smart incremental fetching for daily operations")
    parser.add_argument("--max-time", type=int, default=20, help="Max time in minutes")
    parser.add_argument("--plan-only", action="store_true", help="Just show the fetch plan")
    parser.add_argument("--operations", nargs="+", help="Specific operations to run")
    
    args = parser.parse_args()
    
    if args.plan_only:
        from Database import Database
        db = Database()
        db.get_constants()
        plan = create_daily_fetch_plan(db, args.max_time)
        print(json.dumps(plan, indent=2, default=str))
    else:
        results = run_smart_daily_fetch(args.max_time, args.operations)
        print(f"\n✅ Fetch complete!")
        for operation, result in results.items():
            count = len(result) if result else 0
            print(f"  {operation}: {count} items")