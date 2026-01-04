#!/usr/bin/env python3
"""
Command to list all tracked individuals and their data status.

Usage:
    python -m commands.list_individuals
    python -m commands.list_individuals --verbose
"""

import argparse
from Database import Database

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False


def list_individuals(verbose: bool = False):
    """
    List all tracked individuals and their data status.
    
    Args:
        verbose: Whether to show detailed information
    
    Returns:
        dict: Summary of individuals
    """
    db = Database()
    db.get_constants()
    
    if not db.individuals:
        print("No individuals are currently being tracked.")
        return {"count": 0}
    
    # Gather data about each individual
    individuals_data = []
    
    for individual_id, individual_info in db.individuals.items():
        # Check if contribution data exists
        raw_data = db.client.collection("rawIndividualContributions").document(individual_id).get()
        processed_data = db.client.collection("individuals").document(individual_id).get()
        
        raw_contribs = 0
        if raw_data.exists:
            raw_contribs = len(raw_data.to_dict().get("contributions", []))
        
        processed_contribs = 0
        if processed_data.exists:
            processed_contribs = len(processed_data.to_dict().get("contributions", []))
        
        row_data = {
            "ID": individual_id,
            "Name": individual_info.get("name", individual_id.replace("-", " ").title()),
            "Raw Contributions": raw_contribs,
            "Processed Groups": processed_contribs,
            "Has ZIP": "✓" if "zip" in individual_info else "✗",
            "Has Employer Search": "✓" if "employerSearch" in individual_info else "✗",
            "Has Company": "✓" if "company" in individual_info else "✗",
        }
        
        if verbose:
            row_data.update({
                "ZIP": individual_info.get("zip", "-"),
                "City": individual_info.get("city", "-"),
                "Company": ", ".join(individual_info.get("company", [])) or "-",
                "Title": individual_info.get("title", "-"),
                "Employer Search": ", ".join(individual_info.get("employerSearch", [])) or "-",
                "Name Search": individual_info.get("nameSearch", "-"),
                "Photo Credit": "✓" if "photoCredit" in individual_info else "✗",
            })
        
        individuals_data.append(row_data)
    
    # Sort by name
    individuals_data.sort(key=lambda x: x["Name"])
    
    # Display table
    headers = list(individuals_data[0].keys()) if individuals_data else []
    
    print(f"\nTracked Individuals ({len(individuals_data)} total):")
    
    if HAS_TABULATE:
        table = tabulate(individuals_data, headers=headers, tablefmt="grid")
        print(table)
    else:
        # Fallback display without tabulate
        if individuals_data:
            # Print headers
            print("\n" + " | ".join(f"{h:20}" for h in headers))
            print("-" * (21 * len(headers) + len(headers) - 1))
            
            # Print rows
            for row in individuals_data:
                values = [str(row.get(h, "-"))[:20] for h in headers]
                print(" | ".join(f"{v:20}" for v in values))
    
    # Summary stats
    total_raw = sum(row["Raw Contributions"] for row in individuals_data)
    total_processed = sum(row["Processed Groups"] for row in individuals_data)
    
    print(f"\nSummary:")
    print(f"  Total individuals: {len(individuals_data)}")
    print(f"  Total raw contributions: {total_raw:,}")
    print(f"  Total processed groups: {total_processed:,}")
    
    return {
        "count": len(individuals_data),
        "total_raw_contributions": total_raw,
        "total_processed_groups": total_processed,
        "individuals": individuals_data
    }


def main():
    parser = argparse.ArgumentParser(description="List all tracked individuals")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed information")
    
    args = parser.parse_args()
    
    try:
        result = list_individuals(verbose=args.verbose)
        
        if result["count"] == 0:
            print("\n💡 To add your first individual, use:")
            print("   python -m commands.add_individual --id 'person-name' --name 'Person Name' --zip '12345'")
            
    except Exception as e:
        print(f"❌ Error listing individuals: {e}")
        raise


if __name__ == "__main__":
    main()