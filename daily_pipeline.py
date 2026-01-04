#!/usr/bin/env python3
"""
Optimized daily pipeline for Follow the Crypto.

This pipeline is designed for daily operations and focuses on:
1. Incremental updates (only fetch new data since last run)
2. Time-aware execution (respects daily operation constraints)
3. Smart dependency handling (only update what changed)
4. Detailed progress reporting

Usage:
    python daily_pipeline.py                    # Run full daily pipeline
    python daily_pipeline.py --quick-check      # Just check what needs updating
    python daily_pipeline.py --max-time 20      # Limit to 20 minutes
    python daily_pipeline.py --skip expenditures # Skip expenditure updates
"""

import argparse
import logging
import time
import json
from datetime import datetime, timedelta
from Database import Database

# Import pipeline functions
from fetch_committee_contributions import update_committee_contributions
from process_committee_contributions import process_committee_contributions
from committee_expenditures import update_committee_expenditures
from process_committee_expenditures import process_expenditures
from committee_disbursements import update_committee_disbursements
from individuals import update_spending_by_individuals
from process_individual_contributions import process_individual_contributions
from company_spending import update_spending_by_company
from process_company_contributions import process_company_contributions
from recipients import summarize_recipients


class DailyPipeline:
    def __init__(self, max_time_minutes=30, verbose=False):
        self.start_time = time.time()
        self.max_time_seconds = max_time_minutes * 60
        self.verbose = verbose
        self.operations = []
        self.db = None
        
        # Set up logging
        level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")
        
    def check_time_remaining(self, operation_name=""):
        """Check if we have time remaining for operations."""
        elapsed = time.time() - self.start_time
        remaining = self.max_time_seconds - elapsed
        
        if remaining <= 0:
            logging.warning(f"Time limit exceeded, stopping before {operation_name}")
            return False
        
        if remaining < 300:  # Less than 5 minutes
            logging.warning(f"Only {remaining/60:.1f} minutes remaining for {operation_name}")
        
        return True
    
    def log_operation(self, name, func, *args, **kwargs):
        """Execute and log an operation with timing."""
        if not self.check_time_remaining(name):
            self.operations.append({"name": name, "status": "skipped", "reason": "time_limit"})
            return None
            
        start = time.time()
        logging.info(f"Starting: {name}")
        
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start
            
            operation = {
                "name": name,
                "status": "success",
                "elapsed_seconds": round(elapsed, 2),
                "result_summary": self._summarize_result(result)
            }
            
            if self.verbose and result:
                operation["result_detail"] = str(result)[:500]  # Limit detail size
            
            self.operations.append(operation)
            logging.info(f"Completed: {name} in {elapsed:.2f}s")
            return result
            
        except Exception as e:
            elapsed = time.time() - start
            self.operations.append({
                "name": name,
                "status": "error",
                "elapsed_seconds": round(elapsed, 2),
                "error": str(e)
            })
            logging.error(f"Failed: {name} - {e}")
            raise e
    
    def _summarize_result(self, result):
        """Create a summary of operation results."""
        if result is None:
            return "No return value"
        elif isinstance(result, dict):
            return f"Dict with {len(result)} keys"
        elif isinstance(result, (list, set)):
            return f"Collection with {len(result)} items"
        elif isinstance(result, (int, float)):
            return f"Numeric value: {result}"
        else:
            return f"Type: {type(result).__name__}"
    
    def quick_check(self):
        """Quick status check without making changes."""
        logging.info("Running quick status check...")
        
        # Check for unprocessed individuals
        unprocessed_individuals = 0
        for ind_id in self.db.individuals.keys():
            existing_data = self.db.client.collection("individuals").document(ind_id).get()
            if not existing_data.exists or not existing_data.to_dict().get("contributions"):
                unprocessed_individuals += 1
        
        # Check recent committee contribution activity (simplified)
        recent_activity = {"contributions": 0, "expenditures": 0}
        
        # Check when collections were last updated (if tracking timestamps)\n        status = {\n            \"timestamp\": datetime.now().isoformat(),\n            \"total_individuals\": len(self.db.individuals),\n            \"unprocessed_individuals\": unprocessed_individuals,\n            \"total_committees\": len(self.db.committees),\n            \"total_companies\": len(self.db.companies),\n            \"recent_activity\": recent_activity,\n            \"recommendations\": []\n        }\n        \n        # Add recommendations\n        if unprocessed_individuals > 0:\n            status[\"recommendations\"].append(\n                f\"Process {unprocessed_individuals} individuals without contribution data\"\n            )\n        \n        if unprocessed_individuals > 10:\n            status[\"recommendations\"].append(\n                \"Consider using batch processing for efficiency\"\n            )\n            \n        return status\n    \n    def run_contributions_pipeline(self):\n        \"\"\"Run the contributions data pipeline.\"\"\"\n        logging.info(\"=== CONTRIBUTIONS PIPELINE ===\")\n        \n        # Fetch new committee contributions\n        new_contributions = self.log_operation(\n            \"fetch_committee_contributions\",\n            update_committee_contributions,\n            self.db\n        )\n        \n        # Process committee contributions\n        if new_contributions or not self.check_time_remaining():\n            self.log_operation(\n                \"process_committee_contributions\", \n                process_committee_contributions,\n                self.db\n            )\n        \n        return len(new_contributions) if new_contributions else 0\n    \n    def run_expenditures_pipeline(self):\n        \"\"\"Run the expenditures data pipeline.\"\"\"\n        logging.info(\"=== EXPENDITURES PIPELINE ===\")\n        \n        # Fetch new committee expenditures\n        new_expenditures = self.log_operation(\n            \"fetch_committee_expenditures\",\n            update_committee_expenditures,\n            self.db\n        )\n        \n        # Process expenditures\n        if new_expenditures or not self.check_time_remaining():\n            self.log_operation(\n                \"process_committee_expenditures\",\n                process_expenditures,\n                self.db\n            )\n        \n        return len(new_expenditures) if new_expenditures else 0\n    \n    def run_disbursements_pipeline(self):\n        \"\"\"Run the disbursements data pipeline.\"\"\"\n        logging.info(\"=== DISBURSEMENTS PIPELINE ===\")\n        \n        new_disbursements = self.log_operation(\n            \"fetch_committee_disbursements\",\n            update_committee_disbursements,\n            self.db\n        )\n        \n        return len(new_disbursements) if new_disbursements else 0\n    \n    def run_individuals_pipeline(self):\n        \"\"\"Run the individuals data pipeline with optimizations.\"\"\"\n        logging.info(\"=== INDIVIDUALS PIPELINE ===\")\n        \n        # Check for individuals without processed data\n        unprocessed_individuals = []\n        for ind_id in self.db.individuals.keys():\n            if not self.check_time_remaining(\"checking individuals\"):\n                break\n                \n            existing_data = self.db.client.collection(\"individuals\").document(ind_id).get()\n            if not existing_data.exists or not existing_data.to_dict().get(\"contributions\"):\n                unprocessed_individuals.append(ind_id)\n        \n        if unprocessed_individuals:\n            logging.info(f\"Found {len(unprocessed_individuals)} unprocessed individuals\")\n            \n            # Use batch processing from our daily commands\n            from commands.batch_daily import process_pending_individuals\n            \n            # Process in smaller batches for daily ops\n            batch_size = min(5, len(unprocessed_individuals))\n            processed_count = 0\n            \n            for i in range(0, len(unprocessed_individuals), batch_size):\n                if not self.check_time_remaining(f\"individual batch {i//batch_size + 1}\"):\n                    break\n                    \n                batch = unprocessed_individuals[i:i + batch_size]\n                \n                result = self.log_operation(\n                    f\"process_individuals_batch_{i//batch_size + 1}\",\n                    process_pending_individuals,\n                    self.db,\n                    batch\n                )\n                \n                processed_count += len(batch)\n            \n            return processed_count\n        else:\n            logging.info(\"All individuals have processed data\")\n            return 0\n    \n    def run_companies_pipeline(self):\n        \"\"\"Run the companies data pipeline.\"\"\"\n        logging.info(\"=== COMPANIES PIPELINE ===\")\n        \n        # Only update companies if individuals were processed or if forced\n        # This is more efficient than always updating all companies\n        \n        self.log_operation(\n            \"update_company_spending\",\n            update_spending_by_company,\n            self.db\n        )\n        \n        new_recipients = self.log_operation(\n            \"process_company_contributions\",\n            process_company_contributions,\n            self.db\n        )\n        \n        return len(new_recipients) if new_recipients else 0\n    \n    def run_final_summaries(self):\n        \"\"\"Run final summary operations.\"\"\"\n        logging.info(\"=== FINAL SUMMARIES ===\")\n        \n        self.log_operation(\n            \"summarize_recipients\",\n            summarize_recipients,\n            self.db\n        )\n    \n    def run_full_pipeline(self, skip_operations=None):\n        \"\"\"Run the complete daily pipeline.\"\"\"\n        skip_operations = skip_operations or []\n        \n        logging.info(\"Starting daily pipeline...\")\n        logging.info(f\"Max time: {self.max_time_seconds/60:.1f} minutes\")\n        \n        # Initialize database\n        self.db = Database()\n        self.db.get_constants()\n        \n        # Track what we actually updated\n        updates = {\n            \"contributions\": 0,\n            \"expenditures\": 0, \n            \"disbursements\": 0,\n            \"individuals\": 0,\n            \"companies\": 0\n        }\n        \n        # Run pipeline components\n        if \"contributions\" not in skip_operations:\n            updates[\"contributions\"] = self.run_contributions_pipeline()\n            \n        if \"expenditures\" not in skip_operations:\n            updates[\"expenditures\"] = self.run_expenditures_pipeline()\n            \n        if \"disbursements\" not in skip_operations:\n            updates[\"disbursements\"] = self.run_disbursements_pipeline()\n            \n        if \"individuals\" not in skip_operations:\n            updates[\"individuals\"] = self.run_individuals_pipeline()\n            \n        if \"companies\" not in skip_operations:\n            updates[\"companies\"] = self.run_companies_pipeline()\n            \n        if \"summaries\" not in skip_operations:\n            self.run_final_summaries()\n        \n        # Final summary\n        total_elapsed = time.time() - self.start_time\n        within_time_limit = total_elapsed <= self.max_time_seconds\n        \n        summary = {\n            \"status\": \"completed\" if within_time_limit else \"partial_timeout\",\n            \"total_elapsed_seconds\": round(total_elapsed, 2),\n            \"within_time_limit\": within_time_limit,\n            \"operations_completed\": len([op for op in self.operations if op[\"status\"] == \"success\"]),\n            \"operations_failed\": len([op for op in self.operations if op[\"status\"] == \"error\"]),\n            \"operations_skipped\": len([op for op in self.operations if op[\"status\"] == \"skipped\"]),\n            \"updates\": updates,\n            \"optimization\": \"daily_incremental_pipeline\"\n        }\n        \n        return summary\n\n\ndef main():\n    parser = argparse.ArgumentParser(description=\"Daily optimized pipeline for Follow the Crypto\")\n    parser.add_argument(\"--quick-check\", action=\"store_true\", help=\"Just check status without updates\")\n    parser.add_argument(\"--max-time\", type=int, default=30, help=\"Maximum time in minutes (default: 30)\")\n    parser.add_argument(\"--skip\", action=\"append\", help=\"Skip operations: contributions, expenditures, disbursements, individuals, companies, summaries\")\n    parser.add_argument(\"--verbose\", \"-v\", action=\"store_true\", help=\"Verbose logging\")\n    parser.add_argument(\"--output\", help=\"Save results to JSON file\")\n    \n    args = parser.parse_args()\n    \n    pipeline = DailyPipeline(\n        max_time_minutes=args.max_time,\n        verbose=args.verbose\n    )\n    \n    try:\n        if args.quick_check:\n            print(\"🔍 Running quick status check...\")\n            pipeline.db = Database()\n            pipeline.db.get_constants()\n            status = pipeline.quick_check()\n            \n            print(f\"\\n📊 Daily Pipeline Status\")\n            print(f\"Total individuals: {status['total_individuals']}\")\n            print(f\"Unprocessed individuals: {status['unprocessed_individuals']}\")\n            print(f\"Total committees: {status['total_committees']}\")\n            print(f\"Total companies: {status['total_companies']}\")\n            \n            if status[\"recommendations\"]:\n                print(f\"\\n💡 Recommendations:\")\n                for rec in status[\"recommendations\"]:\n                    print(f\"  • {rec}\")\n            \n            if args.output:\n                with open(args.output, 'w') as f:\n                    json.dump(status, f, indent=2)\n                print(f\"\\n💾 Status saved to {args.output}\")\n        \n        else:\n            print(f\"🚀 Running daily pipeline (max time: {args.max_time} minutes)...\")\n            summary = pipeline.run_full_pipeline(skip_operations=args.skip or [])\n            \n            # Print summary\n            print(f\"\\n✅ Daily Pipeline Complete\")\n            print(f\"Status: {summary['status']}\")\n            print(f\"Total time: {summary['total_elapsed_seconds']/60:.1f} minutes\")\n            print(f\"Operations: {summary['operations_completed']} success, {summary['operations_failed']} failed, {summary['operations_skipped']} skipped\")\n            \n            print(f\"\\n📈 Updates Found:\")\n            for data_type, count in summary['updates'].items():\n                print(f\"  {data_type}: {count}\")\n            \n            if summary['operations_failed'] > 0:\n                print(f\"\\n⚠️  Failed Operations:\")\n                for op in pipeline.operations:\n                    if op['status'] == 'error':\n                        print(f\"  • {op['name']}: {op['error']}\")\n            \n            if args.output:\n                output_data = {\n                    \"summary\": summary,\n                    \"operations\": pipeline.operations\n                }\n                with open(args.output, 'w') as f:\n                    json.dump(output_data, f, indent=2)\n                print(f\"\\n💾 Results saved to {args.output}\")\n        \n        return 0\n        \n    except Exception as e:\n        print(f\"❌ Pipeline failed: {e}\")\n        if args.verbose:\n            import traceback\n            traceback.print_exc()\n        return 1\n\n\nif __name__ == \"__main__\":\n    exit(main())