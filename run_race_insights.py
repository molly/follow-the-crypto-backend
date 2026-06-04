#!/usr/bin/env python3
"""One-off script to populate raceInsights documents in Firestore."""

from Database import Database
from race_insights import compute_race_insights

db = Database()
db.get_constants()
compute_race_insights(db)
print("Done.")
