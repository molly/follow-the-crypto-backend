"""
Find "orphan" independent expenditures: spending that DOES show on a committee's
page (committees/[committeeId] -> "By candidate") but does NOT show on the race
page (elections/[raceId]) for the candidate it names.

Why the two disagree
--------------------
- The committee page (CommitteeExpendituresByCandidate.tsx) is *expenditure-driven*:
  it lists every expenditure for the committee and derives the race link straight
  from the expenditure's own candidate_office / candidate_office_district fields.
  It never checks whether the candidate is actually rostered in a race.

- The race page (RaceSummary.tsx) is *roster-driven*: per sub-race it builds
  raceCandidateIds from that sub-race's candidate list (electionData.candidates[name]
  .candidate_id) and keeps only expenditures whose candidate_id is a substring of a
  rostered id -- and only when the sub-race has a party (primary). It also requires
  expenditure.subrace === the sub-race's type.

So an expenditure vanishes from the race page whenever it fails roster
reconciliation, even though it is correctly attributed to the committee.

This script replicates the race-page visibility predicate exactly and reports the
expenditures that pass the committee page but fail every sub-race view, grouped by
the reason they were dropped.

Read-only. Run: python diagnose_orphan_expenditures.py
"""

from collections import defaultdict

from Database import Database
from utils import compare_names


def get_state_and_key(full_race_id):
    """'CA-H-45' -> ('CA', 'H-45'); 'TX-S-special' -> ('TX', 'S-special')."""
    parts = full_race_id.split("-")
    return parts[0], "-".join(parts[1:])


def get_candidate_display(e):
    """Mirror getCandidateName() in CommitteeExpendituresByCandidate.tsx."""
    last = e.get("candidate_last_name")
    if last:
        first = (e.get("candidate_first_name") or "").split(" ")[0]
        parts = [first, e.get("candidate_middle_name"), last]
    else:
        parts = [e.get("candidate_first_name"), e.get("candidate_name")]
    return " ".join(p for p in parts if p).strip() or (e.get("candidate_name") or "")


def load_race_details(db):
    """state -> {race_key -> race_obj}, merging any sharded STATE_n docs."""
    races_by_state = defaultdict(dict)
    for doc in db.client.collection("raceDetails").stream():
        doc_id = doc.id
        # Strip a trailing _<digit> shard suffix (matches fetchAllRaceIds).
        if "_" in doc_id and doc_id.rsplit("_", 1)[1].isdigit():
            state = doc_id.rsplit("_", 1)[0]
        else:
            state = doc_id
        for race_key, race_obj in doc.to_dict().items():
            if isinstance(race_obj, dict):
                races_by_state[state][race_key] = race_obj
    return races_by_state


def is_visible_on_race_page(e, race_obj):
    """
    True if expenditure e would appear under at least one sub-race view.
    Replicates RaceSummary.tsx lines 76-98.
    """
    e_sub = e.get("subrace")
    e_cid = e.get("candidate_id")
    roster = race_obj.get("candidates", {})
    for sub in race_obj.get("races", []):
        if sub.get("type") != e_sub:
            continue
        if sub.get("party") is None:
            # General election: no candidate filtering, all same-subrace IE shown.
            return True
        # Primary: keep only expenditures whose candidate_id substring-matches a
        # rostered candidate IN THIS SUB-RACE.
        sub_names = [c.get("name") for c in sub.get("candidates", [])]
        race_candidate_ids = [
            roster[n]["candidate_id"]
            for n in sub_names
            if n in roster and roster[n].get("candidate_id")
        ]
        if e_cid and any(e_cid in rid for rid in race_candidate_ids):
            return True
    return False


def classify(e, race_obj):
    """Why an expenditure that's invisible on the race page got dropped."""
    if race_obj is None:
        return "NO_RACE"  # attributed to a race that doesn't exist in raceDetails

    e_sub = e.get("subrace")
    if not e_sub:
        return "SUBRACE_MISSING"  # no subrace tag -> matches no sub-race view

    matching = [s for s in race_obj.get("races", []) if s.get("type") == e_sub]
    if not matching:
        return "SUBRACE_MISMATCH"  # subrace tag not among this race's sub-races

    roster = race_obj.get("candidates", {})
    last = (e.get("candidate_last_name") or "").strip()
    matched_name = next((n for n in roster if last and compare_names(last, n)), None)

    if matched_name is None:
        return "CANDIDATE_NOT_IN_ROSTER"  # named candidate isn't in the race roster
    if not roster[matched_name].get("candidate_id"):
        return "ROSTER_CANDIDATE_NO_ID"  # rostered but no FEC id -> filtered by Boolean()

    e_cid = e.get("candidate_id")
    if not (e_cid and e_cid in roster[matched_name]["candidate_id"]):
        return "ID_MISMATCH"  # ids don't substring-match (e.g. House->Senate switch)

    # Named candidate reconciles, but isn't listed in the matching sub-race's
    # candidate array (so raceCandidateIds for that view excludes them).
    return "CANDIDATE_NOT_IN_SUBRACE"


def get_roster_candidate_id(e, race_obj):
    """The candidate_id the race roster holds for the expenditure's named
    candidate, matched by last name (mirrors classify()). None if the candidate
    isn't rostered. For ID_MISMATCH this is the id that fails to match the
    expenditure's own candidate_id."""
    if race_obj is None:
        return None
    roster = race_obj.get("candidates", {})
    last = (e.get("candidate_last_name") or "").strip()
    matched_name = next((n for n in roster if last and compare_names(last, n)), None)
    if matched_name is None:
        return None
    return roster[matched_name].get("candidate_id")


REASON_BLURB = {
    "NO_RACE": "Expenditure's race is absent from raceDetails (no race page roster).",
    "SUBRACE_MISSING": "Expenditure has no subrace tag, so it matches no sub-race view.",
    "SUBRACE_MISMATCH": "Expenditure subrace isn't among the race's sub-races.",
    "CANDIDATE_NOT_IN_ROSTER": "Named candidate isn't in the race's candidate roster.",
    "ROSTER_CANDIDATE_NO_ID": "Rostered candidate has no candidate_id (dropped by Boolean filter).",
    "ID_MISMATCH": "Expenditure candidate_id doesn't substring-match the rostered id.",
    "CANDIDATE_NOT_IN_SUBRACE": "Candidate reconciles but isn't in the matching sub-race's list.",
}


def find_orphan_expenditures(db, all_exp=None, states_exp=None, races_by_state=None):
    """Find IEs visible on committee pages but hidden on race pages.

    Returns by_reason: {reason -> [ {committee, candidate, race, subrace, amount,
    count}, ... ]}. Pass already-loaded all_exp / states_exp / races_by_state to
    avoid re-reading Firestore (e.g. from the healthcheck). Read-only.
    """
    if all_exp is None:
        all_exp = (
            db.client.collection("expenditures").document("all").get().to_dict() or {}
        )
    if states_exp is None:
        states_exp = (
            db.client.collection("expenditures").document("states").get().to_dict()
            or {}
        )
    if races_by_state is None:
        races_by_state = load_race_details(db)

    committees = db.committees or {}

    # Aggregate orphans by (committee, candidate, full_race_id, subrace, reason).
    orphans = defaultdict(lambda: {"support": 0.0, "oppose": 0.0, "count": 0})
    seen = set()

    for state, state_exp in states_exp.items():
        for full_race_id, group in state_exp.get("by_race", {}).items():
            _, race_key = get_state_and_key(full_race_id)
            race_obj = races_by_state.get(state, {}).get(race_key)

            for exp_id in group.get("expenditures", []):
                if exp_id in seen:
                    continue
                seen.add(exp_id)
                e = all_exp.get(exp_id)
                if not e or not e.get("expenditure_amount"):
                    continue

                committee_id = str(e.get("committee_id"))
                # Committee page only exists for tracked committees.
                if committee_id not in committees:
                    continue
                # Committee page needs a state + a derivable candidate name.
                if not e.get("candidate_office_state"):
                    continue
                name = get_candidate_display(e)
                if not name:
                    continue

                if race_obj is not None and is_visible_on_race_page(e, race_obj):
                    continue  # shows on both pages -- fine

                reason = classify(e, race_obj)
                key = (
                    committee_id,
                    name,
                    e.get("candidate_id") or "(none)",
                    get_roster_candidate_id(e, race_obj) or "",
                    full_race_id,
                    e.get("subrace") or "(none)",
                    reason,
                )
                amount = e.get("expenditure_amount") or 0
                if e.get("support_oppose_indicator") == "S":
                    orphans[key]["support"] += amount
                else:
                    orphans[key]["oppose"] += amount
                orphans[key]["count"] += 1

    by_reason = defaultdict(list)
    for (
        committee_id,
        name,
        candidate_id,
        roster_id,
        full_race_id,
        subrace,
        reason,
    ), v in orphans.items():
        by_reason[reason].append(
            {
                "committee": committees.get(committee_id, {}).get("name", committee_id),
                "candidate": name,
                "candidate_id": candidate_id,
                "roster_id": roster_id,
                "race": full_race_id,
                "subrace": subrace,
                "amount": v["support"] + v["oppose"],
                "count": v["count"],
            }
        )
    return by_reason


def main():
    db = Database()
    db.get_constants()

    print("Loading expenditures/all, expenditures/states, raceDetails...")
    by_reason = find_orphan_expenditures(db)

    total_amount = sum(
        r["amount"] for rows in by_reason.values() for r in rows
    )
    total_rows = sum(len(rows) for rows in by_reason.values())

    lines = []
    lines.append(
        f"Found {total_rows} orphaned committee/candidate/race groups "
        f"(${total_amount:,.0f}) visible on committee pages but hidden on race pages.\n"
    )
    # Order reasons by total dollars so the biggest gaps surface first.
    reason_order = sorted(
        by_reason,
        key=lambda r: -sum(x["amount"] for x in by_reason[r]),
    )
    width = 116
    for reason in reason_order:
        rows = sorted(by_reason[reason], key=lambda x: -x["amount"])
        subtotal = sum(x["amount"] for x in rows)
        lines.append(f"\n{'=' * width}")
        lines.append(f"{reason}  ({len(rows)} groups, ${subtotal:,.0f})")
        lines.append(f"  {REASON_BLURB.get(reason, '')}")
        lines.append("=" * width)
        lines.append(
            f"{'Committee':<24}  {'Candidate':<22}  {'Exp. cand. ID':<13}  "
            f"{'Roster ID':<13}  {'Race':<14}  {'Subrace':<11}  {'Amount':>11}"
        )
        lines.append("-" * width)
        for r in rows:
            # Only surface the rostered id when it actually differs from the
            # expenditure's own id (i.e. the mismatch that hides the spending).
            roster_id = (
                r["roster_id"] if r["roster_id"] and r["roster_id"] != r["candidate_id"]
                else ""
            )
            lines.append(
                f"{r['committee'][:24]:<24}  {r['candidate'][:22]:<22}  "
                f"{r['candidate_id']:<13}  {roster_id:<13}  "
                f"{r['race']:<14}  {r['subrace']:<11}  ${r['amount']:>10,.0f}"
            )

    report = "\n".join(lines)
    print(report)

    out_path = "orphan_expenditures.txt"
    with open(out_path, "w") as f:
        f.write(report + "\n")
    print(f"\nSaved to {out_path}")


if __name__ == "__main__":
    main()
