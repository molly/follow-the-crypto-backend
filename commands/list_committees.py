#!/usr/bin/env python3
"""
Command to list all tracked committees and their status.

Usage:
    python -m commands.list_committees
    python -m commands.list_committees --verbose
"""

import argparse
from Database import Database

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False


def list_committees(verbose=False):
    """
    List all tracked committees and their data status.

    Args:
        verbose: If True, show detailed information
    """
    db = Database()
    db.get_constants()

    if not db.committees:
        print("No committees are currently being tracked.")
        return

    print(f"\nTotal Committees: {len(db.committees)}\n")

    # Collect data for each committee
    committees_info = []

    for committee_id, committee_data in db.committees.items():
        # Check if committee has been hydrated with FEC data
        committee_doc = db.client.collection("committees").document(committee_id).get()
        has_data = committee_doc.exists

        if has_data:
            committee_full = committee_doc.to_dict()
            fec_name = committee_full.get("fec_name", committee_data.get("name"))
            committee_type = committee_full.get("committee_type_full", "N/A")
            party = committee_full.get("party_full", "N/A")
            receipts = committee_full.get("receipts", 0)
            disbursements = committee_full.get("disbursements", 0)
        else:
            fec_name = committee_data.get("name")
            committee_type = "N/A"
            party = "N/A"
            receipts = 0
            disbursements = 0

        committee_info = {
            "id": committee_id,
            "name": committee_data.get("name", "N/A"),
            "fec_name": fec_name,
            "has_data": "✅" if has_data else "⏳",
            "type": committee_type,
            "party": party,
            "receipts": receipts,
            "disbursements": disbursements,
        }

        if verbose:
            description = committee_data.get("description", "N/A")
            # Strip HTML tags for display
            if description and description != "N/A":
                import re
                description = re.sub('<[^<]+?>', '', description)
                if len(description) > 80:
                    description = description[:77] + "..."

            committee_info.update({
                "description": description,
            })

        committees_info.append(committee_info)

    # Sort by name
    committees_info.sort(key=lambda x: x["name"])

    # Display the results
    if HAS_TABULATE:
        if verbose:
            headers = ["ID", "Name", "FEC Name", "Type", "Party", "Status", "Receipts", "Disbursements", "Description"]
            rows = [
                [
                    c["id"],
                    c["name"],
                    c["fec_name"] if c["fec_name"] != c["name"] else "-",
                    c["type"],
                    c["party"],
                    c["has_data"],
                    f"${c['receipts']:,.0f}" if c['receipts'] > 0 else "-",
                    f"${c['disbursements']:,.0f}" if c['disbursements'] > 0 else "-",
                    c["description"]
                ]
                for c in committees_info
            ]
        else:
            headers = ["ID", "Name", "Type", "Party", "Status", "Receipts", "Disbursements"]
            rows = [
                [
                    c["id"],
                    c["name"],
                    c["type"],
                    c["party"],
                    c["has_data"],
                    f"${c['receipts']:,.0f}" if c['receipts'] > 0 else "-",
                    f"${c['disbursements']:,.0f}" if c['disbursements'] > 0 else "-",
                ]
                for c in committees_info
            ]

        print(tabulate(rows, headers=headers, tablefmt="simple"))
    else:
        # Fallback to simple text output
        for c in committees_info:
            print(f"\n{c['name']} ({c['id']})")
            print(f"  Status: {c['has_data']}")
            if c['has_data'] == "✅":
                if c['fec_name'] != c['name']:
                    print(f"  FEC Name: {c['fec_name']}")
                print(f"  Type: {c['type']}")
                print(f"  Party: {c['party']}")
                if c['receipts'] > 0:
                    print(f"  Receipts: ${c['receipts']:,.2f}")
                if c['disbursements'] > 0:
                    print(f"  Disbursements: ${c['disbursements']:,.2f}")

            if verbose and c.get('description') and c['description'] != "N/A":
                print(f"  Description: {c['description']}")

    # Summary statistics
    print(f"\nSummary:")
    processed = sum(1 for c in committees_info if c["has_data"] == "✅")
    pending = len(committees_info) - processed
    total_receipts = sum(c["receipts"] for c in committees_info)
    total_disbursements = sum(c["disbursements"] for c in committees_info)

    print(f"  Hydrated: {processed}/{len(committees_info)}")
    if pending > 0:
        print(f"  Pending: {pending}")
    if total_receipts > 0:
        print(f"  Total Receipts: ${total_receipts:,.2f}")
    if total_disbursements > 0:
        print(f"  Total Disbursements: ${total_disbursements:,.2f}")


def main():
    parser = argparse.ArgumentParser(description="List all tracked committees")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed information")

    args = parser.parse_args()

    if not HAS_TABULATE:
        print("Tip: Install 'tabulate' for better formatted output: pip install tabulate\n")

    list_committees(verbose=args.verbose)


if __name__ == "__main__":
    main()
