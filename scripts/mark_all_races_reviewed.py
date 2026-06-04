"""
Mark every race in raceDetails as reviewed, clearing the review queue at
/admin/edit/raceDetails/review in one shot.

The review screen flags a race whenever its manualRacesUpdated or
scrapedRacesUpdated timestamp is newer than lastReviewed (or it has never been
reviewed). This script sets lastReviewed to the current time for every race
group, which is the same field the "Mark Reviewed" button writes — so it
acknowledges the current data as reviewed without changing any reviewed roster
(the `races` field) or the scraped/manual data itself.

Use this when you've reviewed everything and are confident nothing meaningful
has changed, to clear out accumulated noise.

Run directly (writes by default):
    python -m scripts.mark_all_races_reviewed

Preview without writing:
    python -m scripts.mark_all_races_reviewed --dry-run
"""

import time

from race_utils import get_all_races, update_race


def mark_all_races_reviewed(db, dry_run: bool = False) -> dict:
    now = int(time.time() * 1000)
    all_races = get_all_races(db.client)

    marked = 0
    for state, races in all_races.items():
        for race_id in races:
            if dry_run:
                print(f"  would mark {state}/{race_id} reviewed")
            else:
                update_race(
                    db.client, state, race_id, {f"{race_id}.lastReviewed": now}
                )
            marked += 1

    action = "Would mark" if dry_run else "Marked"
    print(f"{action} {marked} race groups reviewed (lastReviewed={now}).")
    return {"marked": marked, "lastReviewed": now}


if __name__ == "__main__":
    import sys

    from Database import Database

    mark_all_races_reviewed(Database(), dry_run="--dry-run" in sys.argv)
