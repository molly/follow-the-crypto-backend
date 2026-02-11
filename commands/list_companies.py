#!/usr/bin/env python3
"""
Command to list all tracked companies and their status.

Usage:
    python -m commands.list_companies
    python -m commands.list_companies --verbose
"""

import argparse
from Database import Database

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False


def list_companies(verbose=False):
    """
    List all tracked companies and their data status.

    Args:
        verbose: If True, show detailed information
    """
    db = Database()
    db.get_constants()

    if not db.companies:
        print("No companies are currently being tracked.")
        return

    print(f"\nTotal Companies: {len(db.companies)}\n")

    # Collect data for each company
    companies_info = []

    for company_id, company_data in db.companies.items():
        # Check if company has been processed
        company_doc = db.client.collection("companies").document(company_id).get()
        has_data = company_doc.exists

        if has_data:
            company_full = company_doc.to_dict()
            contributions_count = len(company_full.get("contributions", []))
            related_individuals_count = len(company_full.get("relatedIndividuals", []))
            total_contributions = sum(c.get("total", 0) for c in company_full.get("contributions", []))
        else:
            contributions_count = 0
            related_individuals_count = 0
            total_contributions = 0

        company_info = {
            "id": company_id,
            "name": company_data.get("name", "N/A"),
            "category": ", ".join(company_data.get("category", [])),
            "country": company_data.get("country", "N/A"),
            "has_data": "✅" if has_data else "⏳",
            "contributions": contributions_count,
            "individuals": related_individuals_count,
            "total_amount": total_contributions,
        }

        if verbose:
            company_info.update({
                "search_id": company_data.get("search_id", "N/A"),
                "description": company_data.get("description", "N/A")[:50] + "..." if company_data.get("description") and len(company_data.get("description", "")) > 50 else company_data.get("description", "N/A"),
            })

        companies_info.append(company_info)

    # Sort by name
    companies_info.sort(key=lambda x: x["name"])

    # Display the results
    if HAS_TABULATE:
        if verbose:
            headers = ["ID", "Name", "Category", "Country", "Search ID", "Status", "Recipients", "People", "Total $", "Description"]
            rows = [
                [
                    c["id"],
                    c["name"],
                    c["category"],
                    c["country"],
                    c["search_id"],
                    c["has_data"],
                    c["contributions"],
                    c["individuals"],
                    f"${c['total_amount']:,.2f}" if c['total_amount'] > 0 else "-",
                    c["description"]
                ]
                for c in companies_info
            ]
        else:
            headers = ["ID", "Name", "Category", "Country", "Status", "Recipients", "People", "Total $"]
            rows = [
                [
                    c["id"],
                    c["name"],
                    c["category"],
                    c["country"],
                    c["has_data"],
                    c["contributions"],
                    c["individuals"],
                    f"${c['total_amount']:,.2f}" if c['total_amount'] > 0 else "-",
                ]
                for c in companies_info
            ]

        print(tabulate(rows, headers=headers, tablefmt="simple"))
    else:
        # Fallback to simple text output
        for c in companies_info:
            print(f"\n{c['name']} ({c['id']})")
            print(f"  Category: {c['category']}")
            print(f"  Country: {c['country']}")
            print(f"  Status: {c['has_data']}")
            print(f"  Recipients: {c['contributions']}")
            print(f"  Related People: {c['individuals']}")
            if c['total_amount'] > 0:
                print(f"  Total Contributions: ${c['total_amount']:,.2f}")

            if verbose:
                print(f"  Search ID: {c['search_id']}")
                if c['description'] != "N/A":
                    print(f"  Description: {c['description']}")

    # Summary statistics
    print(f"\nSummary:")
    processed = sum(1 for c in companies_info if c["has_data"] == "✅")
    pending = len(companies_info) - processed
    total_contributions = sum(c["total_amount"] for c in companies_info)

    print(f"  Processed: {processed}/{len(companies_info)}")
    if pending > 0:
        print(f"  Pending: {pending}")
    print(f"  Total Contributions Tracked: ${total_contributions:,.2f}")


def main():
    parser = argparse.ArgumentParser(description="List all tracked companies")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed information")

    args = parser.parse_args()

    if not HAS_TABULATE:
        print("Tip: Install 'tabulate' for better formatted output: pip install tabulate\n")

    list_companies(verbose=args.verbose)


if __name__ == "__main__":
    main()
