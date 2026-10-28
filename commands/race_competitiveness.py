#!/usr/bin/env python3
"""
Write a review file of the contests that tracked PACs spent in where Cook's general election
rating can't say how competitive the contest was, for the /analysis/battlegrounds page.

Cook rates general elections by party, so it says nothing about primaries (a "Solid D" seat can
have a close Democratic primary) or about generals between two candidates of the same party
(top-two states). Generals missing from the Cook ratings (e.g. specials already decided before
the ratings were collected) are included too. This does NOT guess which of those contests were
competitive. It lists every
one tracked PACs spent in -- candidates, results, and tracked-PAC support/oppose per candidate --
and you mark each `competitive: true`, `"likely"`, or `false`. The page reads the marks directly; for
contests marked not competitive it works out from the results whether the money backed the
winner or the loser.

Nothing is written to Firestore. One JSON file is emitted into the frontend repo:
  - raceCompetitiveness.review.json  -- every such contest, for you to mark. (source of truth)

Re-running keeps existing marks, so it's safe to run again after the pipeline picks up new
spending; new contests are sorted to the top.

Usage:
    python -m commands.race_competitiveness
"""

import json
import logging
import os
import re

from Database import Database

FRONTEND_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "follow-the-crypto",
)
REVIEW_PATH = os.path.join(
    FRONTEND_DIR, "src", "app", "data", "raceCompetitiveness.review.json"
)
# Read only to show each contest's general election rating as context while reviewing
COOK_RATINGS_PATH = os.path.join(FRONTEND_DIR, "src", "app", "data", "cookRatings.ts")


def load_cook_ratings():
    try:
        with open(COOK_RATINGS_PATH) as fh:
            return dict(re.findall(r'"([A-Z]{2}-[^"]+)": "([a-z-]+)"', fh.read()))
    except OSError:
        return {}


def load_existing_marks():
    """Read prior review marks so re-running doesn't wipe them. Returns {key: mark}."""
    if not os.path.exists(REVIEW_PATH):
        return {}
    try:
        with open(REVIEW_PATH) as fh:
            prev = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {}
    return {
        c["key"]: {
            "competitive": c.get("competitive"),
            "favorite": c.get("favorite", ""),
            "note": c.get("note", ""),
        }
        for c in prev.get("contests", [])
    }


def candidate_party(group, name):
    party = (group.get("candidates") or {}).get(name, {}).get("party")
    # Older summaries stored party as a one-item list
    return party[0] if isinstance(party, list) else party


def find_race(group, race_type, party):
    """The race entry a candidate's spending in `race_type` belongs to.

    Partisan primaries are split by party; top-two and other nonpartisan primaries are a single
    race with party "N" (or none) that every candidate shares.
    """
    races = [r for r in group.get("races") or [] if r.get("type") == race_type]
    for race in races:
        if race.get("party") == party:
            return race
    for race in races:
        if race.get("party") in (None, "N"):
            return race
    return None


def is_same_party_general(group):
    general = find_race(group, "general", None)
    if not general:
        return False
    parties = {
        c.get("party")
        for c in general["candidates"]
        if not c.get("withdrew") and not c.get("died") and not c.get("placeholder")
    }
    return len(parties) == 1 and None not in parties


def build_contests(db):
    ratings = load_cook_ratings()
    contests = {}
    for doc in db.client.collection("raceDetails").stream():
        state = doc.id.rsplit("_", 1)[0]
        for short_race_id, group in doc.to_dict().items():
            race_id = f"{state}-{short_race_id}"
            # Cook's rating settles a general unless it's between two members of the
            # same party, or Cook doesn't rate the race at all
            review_general = race_id not in ratings or is_same_party_general(group)
            for spending in (group.get("spending") or {}).values():
                for subrace, sub in (spending.get("subraces") or {}).items():
                    if subrace == "general" and not review_general:
                        continue
                    for name, amounts in sub["candidates"].items():
                        if not amounts["support"] and not amounts["oppose"]:
                            continue
                        party = candidate_party(group, name)
                        race = find_race(group, subrace, party)
                        contest_party = (race or {}).get("party") or "N"
                        key = f"{race_id}|{subrace}|{contest_party}"
                        if key not in contests:
                            contests[key] = {
                                "key": key,
                                "race_id": race_id,
                                "type": subrace,
                                "party": contest_party,
                                "date": (race or {}).get("date"),
                                "general_rating": ratings.get(race_id),
                                "total": 0,
                                "candidates": {
                                    c["name"]: {
                                        "name": c["name"],
                                        "party": c.get("party"),
                                        "incumbent": bool(c.get("incumbent")),
                                        "won": c.get("won"),
                                        "percentage": c.get("percentage"),
                                        "support": 0,
                                        "oppose": 0,
                                    }
                                    for c in (race or {}).get("candidates", [])
                                },
                            }
                        contest = contests[key]
                        entry = contest["candidates"].setdefault(
                            name,
                            {
                                "name": name,
                                "party": party,
                                "incumbent": False,
                                "won": None,
                                "percentage": None,
                                "support": 0,
                                "oppose": 0,
                            },
                        )
                        entry["support"] += amounts["support"]
                        entry["oppose"] += amounts["oppose"]
                        contest["total"] += amounts["support"] + amounts["oppose"]
    return list(contests.values())


def write_review(contests):
    marks = load_existing_marks()
    unreviewed = 0
    for contest in contests:
        mark = marks.get(contest["key"], {})
        contest["competitive"] = mark.get("competitive")
        contest["favorite"] = mark.get("favorite", "")
        contest["note"] = mark.get("note", "")
        # Funded candidates first, then the rest of the field
        contest["candidates"] = sorted(
            contest["candidates"].values(),
            key=lambda c: -(c["support"] + c["oppose"]),
        )
        if contest["competitive"] is None:
            unreviewed += 1
    contests.sort(key=lambda c: (c["competitive"] is not None, -c["total"]))
    payload = {
        "instructions": (
            "Each entry is a contest tracked PACs spent in that Cook's general election rating "
            "can't classify: a primary, a general between two candidates of the same party, or a "
            "general Cook doesn't rate. "
            "Unreviewed contests (competitive=null) are sorted to the TOP. Set competitive=true "
            "if the contest was genuinely competitive, \"likely\" if it was a real contest with "
            "a clear favorite (like a Cook Likely seat), or false if the winner was a safe bet. "
            "For contests marked \"likely\" or false, the page uses the `won` results to tell whether the money backed the winner "
            "or the loser; when there are no results yet (e.g. a general that hasn't happened), set "
            "`favorite` to the name of the candidate who was the safe bet. Optionally add a "
            "`note`. Re-run python -m commands.race_competitiveness "
            "to pick up new spending; marks are kept."
        ),
        "contest_count": len(contests),
        "unreviewed_count": unreviewed,
        "contests": contests,
    }
    with open(REVIEW_PATH, "w") as fh:
        json.dump(payload, fh, indent=2)
        fh.write("\n")
    logging.info(
        "Wrote review file: %s (%d contests, %d still unreviewed)",
        REVIEW_PATH,
        len(contests),
        unreviewed,
    )


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    write_review(build_contests(Database()))


if __name__ == "__main__":
    main()
