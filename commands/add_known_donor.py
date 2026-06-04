#!/usr/bin/env python3
"""
Command to record a publicly-REPORTED donor to a tracked org that does not
disclose its donors in FEC filings (e.g. a 501(c)(4) dark-money group).

These contributions don't appear in FEC data, so they're hand-curated onto the
recipient company's entry in the `constants/companies` document, under a
`knownDonors` list. The frontend renders them with a "reported" treatment and a
source citation. No pipeline reprocessing is required — only a frontend
revalidate so the static pages pick up the constant change.

Usage:
    python -m commands.add_known_donor \
        --company public-first-action \
        --donor-name "Anthropic" \
        --donor-id anthropic \
        --amount 20000000 \
        --date 2025-09-01 \
        --source "The New York Times" \
        --source-url "https://www.nytimes.com/..."

Re-running with the same --donor-name (and --donor-id) updates the existing
entry rather than duplicating it.
"""

import argparse
import logging

from Database import Database


def add_known_donor(company_id, donor):
    """Add or update a reported donor on a company's `knownDonors` list.

    Args:
        company_id: recipient company slug (must already be tracked)
        donor: dict with name/amount and optional id/idType/date/source/sourceUrl

    Returns:
        dict summary of the operation
    """
    db = Database()
    db.get_constants()

    if company_id not in db.companies:
        raise ValueError(
            f"Company '{company_id}' is not tracked. Add it with add_company first."
        )

    companies = db.companies.copy()
    company = dict(companies[company_id])
    known_donors = list(company.get("knownDonors", []))

    # Match an existing entry by id when present, otherwise by name, so re-runs
    # update in place instead of duplicating.
    def is_same(existing):
        if donor.get("id") and existing.get("id"):
            return existing["id"] == donor["id"]
        return existing.get("name") == donor.get("name")

    updated = False
    for index, existing in enumerate(known_donors):
        if is_same(existing):
            known_donors[index] = donor
            updated = True
            break
    if not updated:
        known_donors.append(donor)

    company["knownDonors"] = known_donors
    companies[company_id] = company

    db.client.collection("constants").document("companies").set(companies)
    db.companies = companies

    return {"company_id": company_id, "donor": donor, "updated": updated}


def main():
    parser = argparse.ArgumentParser(
        description="Record a publicly-reported donor to a non-disclosing org"
    )
    parser.add_argument(
        "--company", required=True, help="Recipient company slug (e.g. public-first-action)"
    )
    parser.add_argument("--donor-name", required=True, help="Donor display name")
    parser.add_argument(
        "--amount", required=True, type=float, help="Reported contribution amount in dollars"
    )
    parser.add_argument(
        "--donor-id", help="Slug of the donor if it's a tracked company/individual (for linking)"
    )
    parser.add_argument(
        "--donor-id-type",
        choices=["company", "individual"],
        default="company",
        help="Whether --donor-id points at a company or individual page (default: company)",
    )
    parser.add_argument("--date", help="ISO date of the gift, when known (YYYY-MM-DD)")
    parser.add_argument("--source", help="Short citation label, e.g. 'The New York Times'")
    parser.add_argument("--source-url", help="URL backing the citation")

    args = parser.parse_args()

    donor = {"name": args.donor_name, "amount": args.amount}
    if args.donor_id:
        donor["id"] = args.donor_id
        donor["idType"] = args.donor_id_type
    if args.date:
        donor["date"] = args.date
    if args.source:
        donor["source"] = args.source
    if args.source_url:
        donor["sourceUrl"] = args.source_url

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    result = add_known_donor(args.company, donor)
    verb = "Updated" if result["updated"] else "Added"
    print(
        f"{verb} reported donor '{donor['name']}' (${donor['amount']:,.0f}) "
        f"on company '{result['company_id']}'."
    )
    print(
        "Constant updated. Trigger a frontend revalidate to surface it on the site; "
        "no pipeline reprocessing needed."
    )


if __name__ == "__main__":
    main()
