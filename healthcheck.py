"""
Post-pipeline data healthcheck.

Aggregates the data-integrity and manual-review checks worth watching after a
pipeline run into one report. Read-only. Run as the `healthcheck` pipeline task,
or standalone:

    python -m healthcheck      (or: python healthcheck.py)

Sections:
  - special_election_drift   SPECIAL_ELECTIONS map vs actual buckets/details.
  - orphaned_spending        Spending with no raceDetails entry, ranked by $.
  - empty_race_details       raceDetails with spending but no candidates.
  - stale_special_elections  current-cycle SPECIAL_ELECTIONS entries with no spending.
  - orphan_expenditures      IEs visible on committee pages, hidden on race pages
                             (also covers expenditures whose candidate isn't in
                             the race roster).
  - incomplete_committees    Committees missing a description/affiliation.
  - races_needing_review     raceDetails with unreviewed scraped/manual races.
  - unreviewed_contributions Committee + company contributions awaiting review.
  - candidates_without_images Candidates with no image in the storage bucket.
  - candidates_awaiting_results Past-dated subraces with no called outcome.
  - notable_patterns_premises Editorial claims in the NotablePatterns home-page
                             cards that no longer hold (figures auto-update, but
                             the prose needs a human rewrite when flagged).
"""

import datetime
import logging
import os

from states import SPECIAL_ELECTIONS, CURRENT_CYCLE
from race_utils import get_all_races, validate_special_elections
from recipient_utils import get_all_recipients, compute_significant_direct_support
from diagnose_orphan_expenditures import find_orphan_expenditures
from candidate_images import get_candidates_without_images


def _short_id(state, race_id):
    parts = race_id.split("-")
    return "-".join(parts[1:]) if parts[0] == state else race_id


def _bucket_total(bucket):
    """Dollar total for a by_race entry ({'total': ...}) or a by_race_companies
    entry ({company_id: amount}) or a bare number."""
    if isinstance(bucket, dict):
        if "total" in bucket:
            return bucket.get("total", 0) or 0
        return sum(v for v in bucket.values() if isinstance(v, (int, float)))
    if isinstance(bucket, (int, float)):
        return bucket
    return 0


def orphaned_spending(states_data, detail_ids, significant_company_races):
    """Races with recorded spending but no raceDetails entry, ranked by $.

    PAC orphans (by_race) are always flagged — PAC spending should always be
    hydrated. Company-only orphans are flagged only when the race clears the
    per-candidate direct-support gate (i.e. some candidate in it would trigger a
    scrape, so the race *should* have a raceDetails entry). A race whose company
    money is spread across several sub-threshold candidates is excluded from
    scraping by design and is not an orphan, even if its race-level total is large
    — `significant_company_races` is the set of full race ids that pass the gate,
    computed once by compute_significant_direct_support.
    """
    pac, company = [], []
    for state, data in states_data.items():
        if state in ("US", "None", None):
            continue
        have = detail_ids.get(state, set())
        by_race = data.get("by_race", {})
        for race_id, bucket in by_race.items():
            if _short_id(state, race_id) not in have:
                pac.append((race_id, _bucket_total(bucket)))
        for race_id, bucket in data.get("by_race_companies", {}).items():
            if race_id in by_race:
                continue  # already covered by the PAC bucket
            if (
                _short_id(state, race_id) not in have
                and race_id in significant_company_races
            ):
                company.append((race_id, _bucket_total(bucket)))
    pac.sort(key=lambda x: -x[1])
    company.sort(key=lambda x: -x[1])
    return {"pac": pac, "company": company}


def empty_race_details(states_data, all_races):
    """raceDetails entries that have spending but no candidates populated.

    Uses the spending buckets (reliable) rather than the stored spending field:
    a race that appears in by_race/by_race_companies and has a detail doc but an
    empty candidates dict is a hollow scrape / failed summarize match.
    """
    findings = []
    seen = set()
    for state, data in states_data.items():
        if state in ("US", "None", None):
            continue
        races = all_races.get(state, {})
        keys = set(data.get("by_race", {})) | set(data.get("by_race_companies", {}))
        for race_id in keys:
            short = _short_id(state, race_id)
            rd = races.get(short)
            if rd is None:
                continue  # orphan, reported elsewhere
            if not rd.get("candidates") and race_id not in seen:
                seen.add(race_id)
                findings.append(race_id)
    return sorted(findings)


def stale_special_elections(states_data):
    """SPECIAL_ELECTIONS entries that look obsolete.

    no_spending: a current-cycle special seat with no spending under any of its
    canonical keys (possibly a wrong or premature entry).

    Past-cycle entries are intentionally NOT reported: prior-year specials stay in
    the map on purpose (they still route and document their historical spending),
    so flagging them only adds noise. They're skipped from the no_spending check
    too, since "no current spending" isn't meaningful for an election that's over.
    """
    no_spending = []
    for seat, entry in SPECIAL_ELECTIONS.items():
        if entry["year"] < CURRENT_CYCLE:
            continue
        state = seat.split("-")[0]
        data = states_data.get(state, {})
        keys = set(data.get("by_race", {})) | set(data.get("by_race_companies", {}))
        # Obsolete = no money for this seat under ANY key (regular or special).
        # Spending under the "wrong" key is a routing bug, reported as drift, not
        # an obsolete entry — so check both forms here, not just canonical keys.
        if seat not in keys and f"{seat}-special" not in keys:
            no_spending.append(seat)
    return {"no_spending": no_spending}


def incomplete_committees(db):
    """Committees referenced as recipients that lack a description/affiliation.

    Mirrors the /admin/edit/constants completeness rule: a committee is complete
    if it is FEC-linked (has candidate_ids/sponsor_candidate_ids), OR has a
    description, OR has a party/candidate-ids affiliation. The universe is the
    recipients (the committees shown on drilldown pages).
    """
    recipients = get_all_recipients(db)
    descriptions = db.all_committees or {}
    affiliations = db.committee_affiliations or {}
    incomplete = []
    for cid, rec in recipients.items():
        if rec.get("candidate_ids") or rec.get("sponsor_candidate_ids"):
            continue
        aff = affiliations.get(cid) or {}
        if aff.get("candidate_ids") or aff.get("sponsor_candidate_ids"):
            continue
        if aff.get("party"):
            continue
        desc = descriptions.get(cid)
        if desc and str(desc).strip():
            continue
        incomplete.append((cid, rec.get("committee_name") or cid))
    return sorted(incomplete, key=lambda x: x[1].lower())


def races_needing_review(all_races):
    """raceDetails with unreviewed scraped or manual races.

    Mirrors /admin/edit/raceDetails/review: needs review when there are scraped
    or manual races and they were updated after the last review (or never
    reviewed).
    """
    findings = []
    for state, races in all_races.items():
        for race_id, rd in races.items():
            last = rd.get("lastReviewed", 0) or 0
            never = last == 0
            manual = rd.get("manualRaces") or []
            scraped = rd.get("scrapedRaces") or []
            mu = rd.get("manualRacesUpdated", 0) or 0
            su = rd.get("scrapedRacesUpdated", 0) or 0
            unrev_manual = len(manual) > 0 and (never or mu > last)
            unrev_scraped = len(scraped) > 0 and (never or su > last)
            if unrev_manual or unrev_scraped:
                findings.append(f"{state}-{race_id}")
    return sorted(findings)


def _count_unreviewed_groups(groups):
    """Count (total, unreviewed) contributions across a list of groups, where a
    contribution is unreviewed when it has no manualReview field."""
    total = unreviewed = 0
    for group in groups or []:
        for c in group.get("contributions", []):
            total += 1
            if not c.get("manualReview"):
                unreviewed += 1
    return total, unreviewed


def unreviewed_contributions(db):
    """Unreviewed committee + company contributions awaiting manual review.

    Mirrors /admin/edit/contributions/review and /company-review: a contribution
    is unreviewed when it lacks a manualReview field.
    """
    committee_total = committee_unrev = committee_docs = 0
    for doc in db.client.collection("contributions").stream():
        data = doc.to_dict() or {}
        t, u = _count_unreviewed_groups(data.get("groups", []))
        committee_total += t
        committee_unrev += u
        if u:
            committee_docs += 1

    company_total = company_unrev = company_docs = 0
    for doc in db.client.collection("companies").stream():
        data = doc.to_dict() or {}
        t, u = _count_unreviewed_groups(data.get("contributions", []))
        company_total += t
        company_unrev += u
        if u:
            company_docs += 1

    return {
        "committee": {
            "total": committee_total,
            "unreviewed": committee_unrev,
            "docs_with_unreviewed": committee_docs,
        },
        "company": {
            "total": company_total,
            "unreviewed": company_unrev,
            "docs_with_unreviewed": company_docs,
        },
    }


def candidates_awaiting_results(all_races, today=None):
    """Past-dated subraces with an incomplete outcome.

    A subrace whose date is on/before today is awaiting results when any candidate
    still lacks a non-null `won` flag (per the RaceCandidate.won result model). The
    scraper assigns `won` to every candidate once a winner exists, so a fully called
    race has none missing; a lone `None` is a real gap -- e.g. a top-two primary
    whose first slot is called while the second is still being decided, or a
    manually merged candidate whose result hasn't been recorded yet.
    """
    today = today or datetime.date.today().isoformat()
    findings = []
    for state, races in all_races.items():
        for race_id, rd in races.items():
            for race in rd.get("races", []):
                date = race.get("date")
                if not date or date > today:
                    continue  # undated or upcoming
                candidates = race.get("candidates", [])
                if not candidates:
                    continue
                if all(c.get("won") is not None for c in candidates):
                    continue  # every candidate has a recorded outcome
                label = race.get("type") or "race"
                party = race.get("party")
                tag = f"{label}/{party}" if party else label
                findings.append(f"{state}-{race_id} ({tag}, {date})")
    return sorted(findings)


# Editorial premises asserted by the frontend NotablePatterns home-page cards
# (src/app/components/home/NotablePatterns.tsx). The cards' dollar/count figures
# auto-update from this same data, but the hand-written narrative does not: if
# any premise below flips, the prose is stale and must be rewritten. Keep these
# constants in sync with the card copy.
_DEFEND_AMERICAN_JOBS = "C00836221"  # crypto, "Defend American Jobs"
_AMERICAN_MISSION = "C00916692"  # AI, "American Mission"
_EXPECTED_INTRA_SECTOR_CONFLICTS = {"NY-H-12"}
_EXPECTED_TOP_OPPOSE_TARGETS = {"Juliana Stratton", "Alex Bores"}
# Republicans should remain a negligible share of opposition spending.
_OPPOSE_REP_SHARE_LIMIT = 0.10


def _supported(committee):
    return {c["candidate"] for c in committee.get("candidates", []) if (c.get("support") or 0) > 0}


def _opposed(committee):
    return {c["candidate"] for c in committee.get("candidates", []) if (c.get("oppose") or 0) > 0}


def _intra_sector_conflicts(races):
    """Races with a candidate both supported and opposed by committees of the
    same sector -- the precise definition of an intra-sector conflict (does not
    rely on the is_adversarial flag, which over-counts dropped-out candidates)."""
    conflicts = set()
    for r in races:
        for pos in r.get("candidate_positions", []):
            sup_secs = {c.get("sector") for c in pos.get("supporting_committees", [])}
            opp_secs = {c.get("sector") for c in pos.get("opposing_committees", [])}
            if sup_secs & opp_secs:
                conflicts.add(r["race_id"])
    return conflicts


def notable_patterns_premises(db):
    """Check the editorial premises behind the NotablePatterns cards.

    Returns a list of {premise, ok, detail} dicts. ok=False means the data has
    drifted away from what the hand-written card copy claims, so the prose needs
    a human rewrite (separate from the figures, which update automatically).
    """
    from collections import Counter

    races = (
        db.client.collection("raceInsights").document("races").get().to_dict() or {}
    ).get("races", [])
    by_party = (
        db.client.collection("expenditures").document("by_party").get().to_dict() or {}
    )
    cross = [r for r in races if r.get("is_cross_sector")]
    checks = []

    # Card 1: the most common cross-sector pairing is Defend American Jobs + American Mission.
    pair_counts = Counter()
    for r in cross:
        crypto_ids = {c["id"] for c in r.get("committees", []) if c.get("sector") == "crypto"}
        ai_ids = {c["id"] for c in r.get("committees", []) if c.get("sector") == "ai"}
        for a in crypto_ids:
            for b in ai_ids:
                pair_counts[frozenset((a, b))] += 1
    expected_pair = frozenset((_DEFEND_AMERICAN_JOBS, _AMERICAN_MISSION))
    expected_count = pair_counts.get(expected_pair, 0)
    max_count = max(pair_counts.values()) if pair_counts else 0
    checks.append({
        "premise": "Defend American Jobs + American Mission is the most common cross-sector pairing",
        "ok": expected_count > 0 and expected_count == max_count,
        "detail": f"DAJ+American Mission co-spend in {expected_count} cross-sector races; "
        f"busiest cross-sector pairing has {max_count}",
    })

    # Card 1: where both spend, they always back the same single candidate (never opposing, never rivals).
    shared = [
        r for r in cross
        if {_DEFEND_AMERICAN_JOBS, _AMERICAN_MISSION}
        <= {c["id"] for c in r.get("committees", [])}
    ]
    divergent = []
    for r in shared:
        daj = next(c for c in r["committees"] if c["id"] == _DEFEND_AMERICAN_JOBS)
        am = next(c for c in r["committees"] if c["id"] == _AMERICAN_MISSION)
        daj_s, am_s = _supported(daj), _supported(am)
        if daj_s != am_s or not daj_s or _opposed(daj) or _opposed(am):
            divergent.append(r["race_id"])
    checks.append({
        "premise": "Defend American Jobs + American Mission always support the same candidate",
        "ok": not divergent,
        "detail": f"{len(shared)} shared races"
        + (f"; DIVERGENT in {', '.join(divergent)}" if divergent else "; all aligned"),
    })

    # Card 2: NY-H-12 is the only intra-sector conflict.
    conflicts = _intra_sector_conflicts(races)
    checks.append({
        "premise": "NY-H-12 is the only intra-sector conflict",
        "ok": conflicts == _EXPECTED_INTRA_SECTOR_CONFLICTS,
        "detail": f"intra-sector conflicts: {', '.join(sorted(conflicts)) or 'none'}",
    })

    # Card 3: crypto IEs lean Republican; AI IEs lean Democratic.
    crypto = by_party.get("crypto", {})
    ai = by_party.get("ai", {})
    c_rep, c_dem = crypto.get("rep_support", 0) or 0, crypto.get("dem_support", 0) or 0
    a_rep, a_dem = ai.get("rep_support", 0) or 0, ai.get("dem_support", 0) or 0
    checks.append({
        "premise": "Crypto support spending leans Republican",
        "ok": c_rep > c_dem,
        "detail": f"crypto support: {_money(c_rep)} R vs {_money(c_dem)} D",
    })
    checks.append({
        "premise": "AI support spending leans Democratic",
        "ok": a_dem > a_rep,
        "detail": f"AI support: {_money(a_dem)} D vs {_money(a_rep)} R",
    })

    # Card 4: nearly all opposition spending targets Democrats.
    allp = by_party.get("all", {})
    dem_opp = allp.get("dem_oppose", 0) or 0
    rep_opp = allp.get("rep_oppose", 0) or 0
    total_opp = dem_opp + rep_opp
    rep_share = rep_opp / total_opp if total_opp else 0
    checks.append({
        "premise": "Opposition spending overwhelmingly targets Democrats",
        "ok": rep_share <= _OPPOSE_REP_SHARE_LIMIT,
        "detail": f"{_money(dem_opp)} vs Democrats, {_money(rep_opp)} vs Republicans "
        f"(R share {rep_share:.0%})",
    })

    # Card 4: the two largest opposition targets are Stratton and Bores.
    opp_by_cand = Counter()
    for r in races:
        for pos in r.get("candidate_positions", []):
            amt = pos.get("oppose_total", 0) or 0
            if amt > 0:
                opp_by_cand[pos["candidate"]] += amt
    top2 = {name for name, _ in opp_by_cand.most_common(2)}
    checks.append({
        "premise": "Largest opposition targets are Juliana Stratton and Alex Bores",
        "ok": top2 == _EXPECTED_TOP_OPPOSE_TARGETS,
        "detail": "top opposition targets: "
        + (", ".join(f"{n} ({_money(a)})" for n, a in opp_by_cand.most_common(2)) or "none"),
    })

    return checks


def run_healthcheck(db, check_images=True):
    """Run all healthcheck sections and return a structured report dict.

    Logs a readable, sectioned summary. Read-only. Set check_images=False to skip
    the per-candidate storage-bucket lookups (the slowest section).
    """
    all_races = get_all_races(db.client)
    detail_ids = {state: set(races.keys()) for state, races in all_races.items()}
    states_data = (
        db.client.collection("expenditures").document("states").get().to_dict() or {}
    )
    all_exp = (
        db.client.collection("expenditures").document("all").get().to_dict() or {}
    )

    report = {}
    significant_company_races = compute_significant_direct_support(db)["race_ids"]
    report["special_election_drift"] = validate_special_elections(
        db,
        detail_ids=detail_ids,
        states_data=states_data,
        significant_company_races=significant_company_races,
    )
    report["orphaned_spending"] = orphaned_spending(
        states_data, detail_ids, significant_company_races
    )
    report["empty_race_details"] = empty_race_details(states_data, all_races)
    report["stale_special_elections"] = stale_special_elections(states_data)
    report["orphan_expenditures"] = find_orphan_expenditures(
        db, all_exp=all_exp, states_exp=states_data, races_by_state=all_races
    )
    report["incomplete_committees"] = incomplete_committees(db)
    report["races_needing_review"] = races_needing_review(all_races)
    report["unreviewed_contributions"] = unreviewed_contributions(db)
    report["candidates_awaiting_results"] = candidates_awaiting_results(all_races)
    try:
        report["notable_patterns_premises"] = notable_patterns_premises(db)
    except Exception as error:  # editorial check is best-effort; never break the report
        logging.warning("notable_patterns_premises check failed: %s", error)
        report["notable_patterns_premises"] = None

    if check_images:
        try:
            report["candidates_without_images"] = get_candidates_without_images(db)
        except Exception as error:  # storage is best-effort; never break the report
            logging.warning("candidates_without_images check failed: %s", error)
            report["candidates_without_images"] = None
    else:
        report["candidates_without_images"] = None

    _log_report(report)
    return report


def _money(amount):
    return f"${amount:,.0f}"


def _log_report(report):
    lines = ["", "=" * 72, "DATA HEALTHCHECK", "=" * 72]

    drift = report["special_election_drift"]
    lines.append(f"\nSPECIAL_ELECTIONS drift: {len(drift)}")
    for m in drift:
        lines.append(f"  - {m}")

    orphans = report["orphaned_spending"]
    lines.append(
        f"\nOrphaned spending (no raceDetails): "
        f"{len(orphans['pac'])} PAC, {len(orphans['company'])} company-only"
    )
    for race_id, amount in orphans["pac"]:
        lines.append(f"  - PAC      {race_id:18} {_money(amount)}")
    for race_id, amount in orphans["company"]:
        lines.append(f"  - company  {race_id:18} {_money(amount)}")

    empty = report["empty_race_details"]
    lines.append(f"\nEmpty race details (spending, no candidates): {len(empty)}")
    for race_id in empty:
        lines.append(f"  - {race_id}")

    stale = report["stale_special_elections"]
    lines.append(
        f"\nStale SPECIAL_ELECTIONS entries: "
        f"{len(stale['no_spending'])} with no spending"
    )
    for seat in stale["no_spending"]:
        lines.append(f"  - no spending: {seat}")

    orph_exp = report["orphan_expenditures"]
    total_rows = sum(len(rows) for rows in orph_exp.values())
    total_amt = sum(r["amount"] for rows in orph_exp.values() for r in rows)
    lines.append(
        f"\nOrphan expenditures (committee page only): "
        f"{total_rows} groups, {_money(total_amt)}"
    )
    for reason in sorted(orph_exp, key=lambda r: -sum(x["amount"] for x in orph_exp[r])):
        rows = sorted(orph_exp[reason], key=lambda x: -x["amount"])
        subtotal = sum(x["amount"] for x in rows)
        lines.append(f"  - {reason}: {len(rows)} groups, {_money(subtotal)}")
        for r in rows:
            # Only surface the rostered id when it differs from the expenditure's
            # own id (the mismatch that actually hides the spending).
            roster_id = (
                r["roster_id"]
                if r["roster_id"] and r["roster_id"] != r["candidate_id"]
                else ""
            )
            id_part = r["candidate_id"] or "(no id)"
            if roster_id:
                id_part += f" -> roster {roster_id}"
            subrace = f"/{r['subrace']}" if r["subrace"] else ""
            lines.append(
                f"      {r['race']}{subrace}  {r['candidate']} [{id_part}]  "
                f"{_money(r['amount'])}  via {r['committee']}"
            )

    committees = report["incomplete_committees"]
    lines.append(f"\nCommittees missing description/affiliation: {len(committees)}")
    for cid, name in committees[:25]:
        lines.append(f"  - {cid}  {name}")
    if len(committees) > 25:
        lines.append(f"  ... and {len(committees) - 25} more")

    needs_review = report["races_needing_review"]
    lines.append(f"\nRaces needing review: {len(needs_review)}")
    for race_id in needs_review[:25]:
        lines.append(f"  - {race_id}")
    if len(needs_review) > 25:
        lines.append(f"  ... and {len(needs_review) - 25} more")

    contribs = report["unreviewed_contributions"]
    lines.append(
        f"\nUnreviewed contributions: "
        f"committee {contribs['committee']['unreviewed']}/{contribs['committee']['total']} "
        f"({contribs['committee']['docs_with_unreviewed']} committees); "
        f"company {contribs['company']['unreviewed']}/{contribs['company']['total']} "
        f"({contribs['company']['docs_with_unreviewed']} companies)"
    )

    awaiting = report["candidates_awaiting_results"]
    lines.append(f"\nSubraces awaiting results: {len(awaiting)}")
    for race in awaiting[:25]:
        lines.append(f"  - {race}")
    if len(awaiting) > 25:
        lines.append(f"  ... and {len(awaiting) - 25} more")

    premises = report.get("notable_patterns_premises")
    if premises is None:
        lines.append("\nNotablePatterns premises: (skipped)")
    else:
        stale = [c for c in premises if not c["ok"]]
        lines.append(
            f"\nNotablePatterns premises: {len(stale)} STALE of {len(premises)}"
            + (" -- rewrite card copy" if stale else "")
        )
        for c in premises:
            mark = "STALE" if not c["ok"] else "ok"
            lines.append(f"  - [{mark:5}] {c['premise']}")
            lines.append(f"            {c['detail']}")

    images = report["candidates_without_images"]
    if images is None:
        lines.append("\nCandidates without images: (skipped)")
    else:
        lines.append(f"\nCandidates without images: {len(images)}")
        for name in images[:25]:
            lines.append(f"  - {name}")
        if len(images) > 25:
            lines.append(f"  ... and {len(images) - 25} more")

    lines.append("\n" + "=" * 72)
    text = "\n".join(lines)
    logging.info(text)

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "healthcheck.txt")
    try:
        with open(out_path, "w") as f:
            f.write(text + "\n")
        logging.info("Wrote healthcheck report to %s", out_path)
    except OSError as error:  # file write is best-effort; never break the report
        logging.warning("Could not write %s: %s", out_path, error)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    from Database import Database

    _db = Database()
    _db.get_constants()
    run_healthcheck(_db, False)
