"""
Compare constants/committees, constants/companies, and constants/individuals
between two Firestore databases and interactively merge missing entries.

Usage: python merge_constants.py
"""

import json
import sys

import firebase_admin
from firebase_admin import credentials
from google.cloud import firestore

SOURCE_DB = "follow-the-crypto-2026"
DEST_DB = "techfunded"
DOCS_TO_COMPARE = ["committees", "companies", "individuals"]


def make_client(cred, project_id, database):
    gcreds = cred.get_credential()
    return firestore.Client(credentials=gcreds, project=project_id, database=database)


def prompt_merge(key, value):
    print(f"\n  Key:   {key}")
    print(f"  Value: {json.dumps(value, indent=4, default=str)}")
    while True:
        answer = input("  Merge to techfunded? [y/n/q] ").strip().lower()
        if answer in ("y", "n", "q"):
            return answer
        print("  Please enter y, n, or q.")


def main():
    cred = credentials.Certificate("service.json")
    app = firebase_admin.initialize_app(cred)
    project_id = app.project_id

    source = make_client(app.credential, project_id, SOURCE_DB)
    dest = make_client(app.credential, project_id, DEST_DB)

    for doc_name in DOCS_TO_COMPARE:
        print(f"\n{'=' * 60}")
        print(f"Comparing constants/{doc_name}")
        print(f"{'=' * 60}")

        source_data = source.collection("constants").document(doc_name).get().to_dict() or {}
        dest_data = dest.collection("constants").document(doc_name).get().to_dict() or {}

        missing_keys = [k for k in source_data if k not in dest_data]
        if not missing_keys:
            print("  No missing entries.")
            continue

        print(f"  {len(missing_keys)} entries in {SOURCE_DB} not found in {DEST_DB}.")

        to_merge = {}
        for key in missing_keys:
            answer = prompt_merge(key, source_data[key])
            if answer == "q":
                print("\nQuitting.")
                sys.exit(0)
            if answer == "y":
                to_merge[key] = source_data[key]

        if to_merge:
            dest.collection("constants").document(doc_name).set(to_merge, merge=True)
            print(f"\n  Merged {len(to_merge)} entries into {DEST_DB}/constants/{doc_name}.")
        else:
            print(f"\n  Nothing merged for constants/{doc_name}.")

    print("\nDone.")


if __name__ == "__main__":
    main()
