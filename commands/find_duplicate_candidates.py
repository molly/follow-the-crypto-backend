#!/usr/bin/env python3
"""
Find candidates who appear under multiple candidate IDs.

This helps identify candidates who have switched races (e.g., House to Senate)
and need to be added to the candidateAliases constants table.

Usage:
    python -m commands.find_duplicate_candidates
"""

from collections import defaultdict
from Database import Database


def normalize_name(name):
    """Normalize name for comparison."""
    if not name:
        return ""
    # Uppercase, strip whitespace, remove common suffixes
    name = name.upper().strip()
    for suffix in [" JR", " JR.", " SR", " SR.", " II", " III", " IV"]:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
    return name


def find_duplicate_candidates():
    """Find candidates with multiple IDs based on name matching."""
    db = Database()

    # Get recipientsWithContribs
    recipients = (
        db.client.collection("allRecipients")
        .document("recipientsWithContribs")
        .get()
        .to_dict()
    )

    if not recipients:
        print("No recipients data found.")
        return

    # Also get existing aliases to exclude already-handled cases
    db.get_constants()
    existing_aliases = set(db.candidate_aliases.keys()) | set(
        db.candidate_aliases.values()
    )

    # Group candidates by normalized name
    name_to_candidates = defaultdict(list)

    for recipient_id, data in recipients.items():
        # Only look at candidates (IDs starting with H, S, or P)
        if recipient_id[0] not in ("H", "S", "P"):
            continue

        candidate_details = data.get("candidate_details", {})
        name = candidate_details.get("name", "")
        if not name:
            continue

        normalized = normalize_name(name)
        office = candidate_details.get("office", "?")
        state = candidate_details.get("state", "?")
        district = candidate_details.get("district", "")
        total = data.get("total", 0)
        is_running = candidate_details.get("isRunningThisCycle", False)

        name_to_candidates[normalized].append(
            {
                "id": recipient_id,
                "name": name,
                "office": office,
                "state": state,
                "district": district,
                "total": total,
                "is_running": is_running,
                "already_aliased": recipient_id in existing_aliases,
            }
        )

    # Find duplicates
    duplicates = {
        name: candidates
        for name, candidates in name_to_candidates.items()
        if len(candidates) > 1
    }

    if not duplicates:
        print("No duplicate candidates found.")
        return

    print(f"Found {len(duplicates)} candidates with multiple IDs:\n")

    for name, candidates in sorted(duplicates.items()):
        # Check if all are already aliased
        all_aliased = all(c["already_aliased"] for c in candidates)
        if all_aliased:
            continue

        print(f"{candidates[0]['name']}")
        print("-" * 50)

        for c in sorted(candidates, key=lambda x: -x["total"]):
            office_str = c["office"]
            if c["district"]:
                office_str += f"-{c['district']}"

            status = []
            if c["is_running"]:
                status.append("running")
            if c["already_aliased"]:
                status.append("aliased")
            status_str = f" ({', '.join(status)})" if status else ""

            print(
                f"  {c['id']}: {c['state']}-{office_str} "
                f"${c['total']:,.0f}{status_str}"
            )

        print()

    # Print suggested aliases format
    print("\nSuggested candidateAliases format (choose canonical ID manually):")
    print("{")
    for name, candidates in sorted(duplicates.items()):
        if all(c["already_aliased"] for c in candidates):
            continue
        ids = [c["id"] for c in candidates]
        # Just show the IDs, user picks which is canonical
        print(f'  // {candidates[0]["name"]}: {", ".join(ids)}')
    print("}")


def main():
    find_duplicate_candidates()


if __name__ == "__main__":
    main()
