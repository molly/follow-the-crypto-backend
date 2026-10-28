#!/usr/bin/env python3
"""
Interactively mark the contests in raceCompetitiveness.review.json, one state at a time.

Walks the contests written by commands.race_competitiveness, grouped by state, showing each
contest's candidates, results, and tracked-PAC support/oppose. Each answer is saved to the
review file immediately, so it's safe to quit at any point and pick up later.

Marks:
  c  competitive       the contest was genuinely competitive
  l  likely            a real contest, but with a clear favorite (like a Cook Likely seat)
  s  safe              the winner was a safe bet
  u  unset             clear the mark
For "likely" and "safe" contests without results yet, you'll be asked who the favorite was.

Usage:
    python -m commands.review_competitiveness               # unreviewed contests, all states
    python -m commands.review_competitiveness --state NY,CA # only these states
    python -m commands.review_competitiveness --all         # include already-marked contests
"""

import argparse
import json
import os
import sys
from itertools import groupby

from commands.race_competitiveness import REVIEW_PATH
from states import STATES_BY_ABBR

MARK_LABELS = {True: "competitive", "likely": "likely", False: "safe", None: "unreviewed"}
CONTEST_TYPES = {
    "general": "General election",
    "primary": "primary",
    "primary_runoff": "primary runoff",
}
PARTY_NAMES = {"D": "Democratic", "R": "Republican", "N": "Nonpartisan"}
HELP = (
    "[c]ompetitive  [l]ikely  [s]afe  [u]nset  [f]avorite  [n]ote  "
    "[enter] skip  [b]ack  [x] skip state  [q]uit"
)


def money(amount):
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.1f}M"
    if amount >= 1_000:
        return f"${amount / 1_000:.0f}K"
    return f"${amount:.0f}"


def contest_label(contest):
    if contest["type"] == "general":
        return CONTEST_TYPES["general"]
    party = PARTY_NAMES.get(contest["party"], contest["party"])
    return f"{party} {CONTEST_TYPES.get(contest['type'], contest['type'])}"


TYPE_ORDER = {"primary": 0, "primary_runoff": 1, "general": 2}


def ballot_order(contest):
    """Senate before House, House by district, then each seat's contests by date."""
    parts = contest["race_id"].split("-")
    office = 0 if parts[1] == "S" else 1
    district = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
    special = parts[-1] == "special"
    return (
        office,
        district,
        special,
        contest.get("date") or "9999-99-99",
        TYPE_ORDER.get(contest["type"], 3),
    )


def has_results(contest):
    return any(c.get("won") is not None for c in contest["candidates"])


def load():
    with open(REVIEW_PATH) as fh:
        return json.load(fh)


def save(review):
    review["unreviewed_count"] = sum(
        1 for c in review["contests"] if c.get("competitive") is None
    )
    # Write to a temp file and swap it in, so quitting mid-write can't corrupt the review
    tmp_path = f"{REVIEW_PATH}.tmp"
    with open(tmp_path, "w") as fh:
        json.dump(review, fh, indent=2)
        fh.write("\n")
    os.replace(tmp_path, REVIEW_PATH)


def show(contest, position, count):
    rating = contest.get("general_rating") or "unrated"
    print()
    print(f"  [{position}/{count}] {contest['race_id']} -- {contest_label(contest)}")
    print(
        f"  {contest.get('date') or 'no date'} · seat rated {rating} · "
        f"{money(contest['total'])} tracked PAC spending · "
        f"currently: {MARK_LABELS.get(contest.get('competitive'), contest.get('competitive'))}"
    )
    if contest["type"] == "general" and contest.get("general_rating"):
        print(
            "  Both finalists are from the same party, so Cook's rating only says which "
            "party wins, not which candidate."
        )
    elif contest["type"] == "general":
        print("  Cook doesn't rate this race, so it needs a hand rating.")
    if contest.get("favorite"):
        print(f"  favorite: {contest['favorite']}")
    if contest.get("note"):
        print(f"  note: {contest['note']}")
    print()
    for i, c in enumerate(contest["candidates"], 1):
        result = {True: "won", False: "lost"}.get(c.get("won"), "-")
        pct = f"{c['percentage']}%" if c.get("percentage") else ""
        flags = ", ".join(f for f in [c.get("party"), "incumbent" if c.get("incumbent") else None] if f)
        money_cols = []
        if c.get("support"):
            money_cols.append(f"for {money(c['support'])}")
        if c.get("oppose"):
            money_cols.append(f"against {money(c['oppose'])}")
        name = f"{c['name']} ({flags})" if flags else c["name"]
        print(f"    {i:>2}. {name:<42} {result:<5} {pct:<6} {'  '.join(money_cols)}")


def pick_favorite(contest):
    candidates = contest["candidates"]
    while True:
        answer = input("  Favorite (number, or enter to leave unset): ").strip()
        if not answer:
            return
        if answer.isdigit() and 1 <= int(answer) <= len(candidates):
            contest["favorite"] = candidates[int(answer) - 1]["name"]
            return
        print("  Not a candidate number.")


def review_contests(review, contests):
    """Prompt for each contest. Returns False if the reviewer quit."""
    i = 0
    while i < len(contests):
        contest = contests[i]
        show(contest, i + 1, len(contests))
        answer = input(f"  {HELP}\n  > ").strip().lower()
        if answer == "q":
            return False
        if answer == "x":
            return True
        if answer == "b":
            i = max(i - 1, 0)
            continue
        if answer in ("c", "l", "s", "u"):
            contest["competitive"] = {"c": True, "l": "likely", "s": False, "u": None}[answer]
            if answer in ("l", "s") and not has_results(contest) and not contest.get("favorite"):
                print("  No results yet, so the page needs to know who the favorite was.")
                pick_favorite(contest)
            save(review)
        elif answer == "f":
            pick_favorite(contest)
            save(review)
            continue
        elif answer == "n":
            contest["note"] = input("  Note: ").strip()
            save(review)
            continue
        elif answer:
            print("  Didn't recognize that.")
            continue
        i += 1
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--state", help="Comma-separated state abbreviations to review")
    parser.add_argument("--all", action="store_true", help="Include contests already marked")
    args = parser.parse_args()

    if not os.path.exists(REVIEW_PATH):
        sys.exit(f"No review file at {REVIEW_PATH}. Run python -m commands.race_competitiveness first.")
    review = load()
    states = {s.strip().upper() for s in args.state.split(",")} if args.state else None

    def state_of(contest):
        return contest["race_id"].split("-")[0]

    todo = [
        c
        for c in review["contests"]
        if (args.all or c.get("competitive") is None)
        and (states is None or state_of(c) in states)
    ]
    if not todo:
        print("Nothing to review.")
        return
    todo.sort(key=lambda c: (state_of(c), ballot_order(c)))
    print(f"{len(todo)} contests to review in {len({state_of(c) for c in todo})} states. "
          "Answers are saved as you go.")

    try:
        for state, group in groupby(todo, key=state_of):
            contests = list(group)
            print()
            print(f"=== {STATES_BY_ABBR.get(state, state)} "
                  f"({len(contests)} {'contest' if len(contests) == 1 else 'contests'}, "
                  f"{money(sum(c['total'] for c in contests))}) ===")
            if not review_contests(review, contests):
                break
    except (KeyboardInterrupt, EOFError):
        print()
    save(review)
    print(f"Saved. {review['unreviewed_count']} contests still unreviewed.")


if __name__ == "__main__":
    main()
