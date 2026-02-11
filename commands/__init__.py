"""
Command-line utilities for managing individuals, companies, and committees.

Available commands:

Individuals:
- add_individual: Add a new individual to track
- fetch_individual: Fetch contribution data for an existing individual
- list_individuals: List all tracked individuals
- batch_daily: Optimized batch processing for daily operations

Companies:
- add_company: Add a new company to track
- list_companies: List all tracked companies

Committees:
- add_committee: Add a new committee to track
- list_committees: List all tracked committees
"""

__all__ = [
    "add_individual",
    "fetch_individual",
    "list_individuals",
    "batch_daily",
    "add_company",
    "list_companies",
    "add_committee",
    "list_committees",
]