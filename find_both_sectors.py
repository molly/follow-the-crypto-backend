#!/usr/bin/env python3
"""Find all races that have both crypto and AI PAC spending."""

import firebase_admin
from firebase_admin import credentials
from google.cloud import firestore

cred = credentials.Certificate("service.json")
app = firebase_admin.initialize_app(cred)
gcreds = app.credential.get_credential()
project_id = app.project_id
client = firestore.Client(credentials=gcreds, project=project_id, database="techfunded")

race_docs = client.collection("raceDetails").stream()

results = []

for doc in race_docs:
    doc_id = doc.id
    # Determine state prefix
    last_underscore = doc_id.rfind("_")
    if last_underscore != -1 and doc_id[last_underscore + 1:].isdigit():
        state = doc_id[:last_underscore]
    else:
        state = doc_id

    elections_by_race = doc.to_dict()
    for race_id, election_group in elections_by_race.items():
        candidates = election_group.get("candidates", {})

        race_crypto = sum(
            (c.get("crypto_support_total") or 0) + (c.get("crypto_oppose_total") or 0)
            for c in candidates.values()
        )
        race_ai = sum(
            (c.get("ai_support_total") or 0) + (c.get("ai_oppose_total") or 0)
            for c in candidates.values()
        )

        if race_crypto > 0 and race_ai > 0:
            full_race_id = f"{state}-{race_id}"
            results.append({
                "race_id": full_race_id,
                "crypto_total": race_crypto,
                "ai_total": race_ai,
                "both_total": race_crypto + race_ai,
            })

results.sort(key=lambda x: x["both_total"], reverse=True)

print(f"{'Race ID':<30} {'Crypto Spending':>18} {'AI Spending':>18} {'Combined':>18}")
print("-" * 88)
for r in results:
    print(f"{r['race_id']:<30} ${r['crypto_total']:>17,.2f} ${r['ai_total']:>17,.2f} ${r['both_total']:>17,.2f}")

print(f"\nTotal races with both crypto and AI spending: {len(results)}")
