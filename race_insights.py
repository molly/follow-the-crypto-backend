"""Compute race-level spending insights.

Surfaces races where tracked PACs interact, classifying each as:
  - adversarial: PACs spending toward conflicting outcomes (one supports a
    candidate another opposes, or different PACs back rival candidates)
  - coordinated: multiple PACs spending the same direction on the same candidate
  - cross_sector: both crypto and AI money in the race
  - multi_pac: >=2 tracked PACs spending

A race can carry several flags at once.
"""

import logging
from race_utils import get_all_races


def _build_committee_breakdown(spending, committee_ids, committees_meta):
    """Build a per-committee list with a candidate support/oppose breakdown.

    For each committee with spending in this race, aggregate support/oppose
    amounts per candidate across all subraces (flattened). Each committee also
    carries its own support_total/oppose_total.

    Returns a list of committee dicts sorted by committee total descending.
    """
    committees_list = []
    for cid in committee_ids:
        committee_spending = spending.get(cid)
        if not committee_spending:
            continue
        total = committee_spending.get("total") or 0
        if total <= 0:
            continue

        # Aggregate per-candidate support/oppose across all subraces
        candidate_totals = {}
        for subrace_data in committee_spending.get("subraces", {}).values():
            for candidate, amounts in subrace_data.get("candidates", {}).items():
                entry = candidate_totals.setdefault(
                    candidate, {"support": 0, "oppose": 0}
                )
                entry["support"] += amounts.get("support", 0) or 0
                entry["oppose"] += amounts.get("oppose", 0) or 0

        candidates_list = [
            {
                "candidate": candidate,
                "support": round(amounts["support"], 2),
                "oppose": round(amounts["oppose"], 2),
            }
            for candidate, amounts in candidate_totals.items()
        ]
        candidates_list.sort(
            key=lambda c: c["support"] + c["oppose"], reverse=True
        )

        committee_info = committees_meta.get(cid, {})
        committees_list.append({
            "id": cid,
            "name": committee_info.get("name", cid),
            "sector": committee_info.get("sector") or "tech",
            "total": round(total, 2),
            "support_total": round(sum(c["support"] for c in candidates_list), 2),
            "oppose_total": round(sum(c["oppose"] for c in candidates_list), 2),
            "candidates": candidates_list,
        })

    committees_list.sort(key=lambda x: x["total"], reverse=True)
    return committees_list


def _build_candidate_positions(committees_list):
    """Pivot the committee breakdown into a per-candidate view of which PACs
    are supporting vs. opposing each candidate.

    Returns a list of candidate-position dicts sorted by total money involved.
    """
    positions = {}
    for committee in committees_list:
        ref = {
            "id": committee["id"],
            "name": committee["name"],
            "sector": committee["sector"],
        }
        for c in committee["candidates"]:
            entry = positions.setdefault(
                c["candidate"],
                {
                    "candidate": c["candidate"],
                    "support_total": 0,
                    "oppose_total": 0,
                    "supporting_committees": [],
                    "opposing_committees": [],
                },
            )
            if c["support"] > 0:
                entry["support_total"] = round(
                    entry["support_total"] + c["support"], 2
                )
                entry["supporting_committees"].append({**ref, "amount": c["support"]})
            if c["oppose"] > 0:
                entry["oppose_total"] = round(
                    entry["oppose_total"] + c["oppose"], 2
                )
                entry["opposing_committees"].append({**ref, "amount": c["oppose"]})

    for entry in positions.values():
        sup_ids = {c["id"] for c in entry["supporting_committees"]}
        opp_ids = {c["id"] for c in entry["opposing_committees"]}
        # Contested only if a *different* PAC opposes than supports — a lone PAC
        # hedging both ways on one candidate is not a clash.
        entry["contested"] = bool(sup_ids and opp_ids) and not (
            len(sup_ids) == 1 and sup_ids == opp_ids
        )

    return sorted(
        positions.values(),
        key=lambda p: p["support_total"] + p["oppose_total"],
        reverse=True,
    )


def compute_race_insights(db):
    """Compute and store raceInsights/races: every race with >=2 tracked PACs
    and/or cross-sector activity, classified for the adversarial and
    coordination views."""
    all_race_data = get_all_races(db.client)
    tracked_committee_ids = set(db.committees.keys()) if db.committees else set()

    races = []

    for state, state_data in all_race_data.items():
        for race_id, race_data in state_data.items():
            full_race_id = f"{state}-{race_id}"
            candidates = race_data.get("candidates", {})
            spending = race_data.get("spending", {})

            active_tracked = [
                cid for cid in spending
                if cid in tracked_committee_ids and (spending[cid].get("total") or 0) > 0
            ]
            committees_list = _build_committee_breakdown(
                spending, active_tracked, db.committees
            )
            if not committees_list:
                continue

            candidate_positions = _build_candidate_positions(committees_list)

            # Sector totals from candidate-level aggregates (a "tech" committee
            # counts toward both crypto and AI).
            crypto_total = sum(
                (c.get("crypto_support_total") or 0) + (c.get("crypto_oppose_total") or 0)
                for c in candidates.values()
            )
            ai_total = sum(
                (c.get("ai_support_total") or 0) + (c.get("ai_oppose_total") or 0)
                for c in candidates.values()
            )

            is_cross_sector = crypto_total > 0 and ai_total > 0
            is_multi_pac = len(committees_list) >= 2

            # Adversarial: a candidate is both supported and opposed (clash), or
            # different PACs back different candidates (backing rivals).
            supported_candidates = [
                p for p in candidate_positions if p["supporting_committees"]
            ]
            distinct_supporters = {
                c["id"]
                for p in supported_candidates
                for c in p["supporting_committees"]
            }
            has_clash = any(p["contested"] for p in candidate_positions)
            # >=2 candidates drawing support from >=2 distinct PACs means at least
            # one PAC is backing a different candidate than another PAC.
            backs_rivals = (
                len(supported_candidates) >= 2 and len(distinct_supporters) >= 2
            )
            is_adversarial = has_clash or backs_rivals
            adversarial_reasons = []
            if has_clash:
                adversarial_reasons.append("contested_candidate")
            if backs_rivals:
                adversarial_reasons.append("rival_candidates")

            # Coordinated: >=2 PACs push the same direction on the same candidate.
            is_coordinated = any(
                len(p["supporting_committees"]) >= 2
                or len(p["opposing_committees"]) >= 2
                for p in candidate_positions
            )

            if not (is_cross_sector or is_multi_pac):
                continue

            races.append({
                "race_id": full_race_id,
                "total": round(sum(c["total"] for c in committees_list), 2),
                "crypto_total": round(crypto_total, 2),
                "ai_total": round(ai_total, 2),
                "pac_count": len(committees_list),
                "is_cross_sector": is_cross_sector,
                "is_multi_pac": is_multi_pac,
                "is_adversarial": is_adversarial,
                "adversarial_reasons": adversarial_reasons,
                "is_coordinated": is_coordinated,
                "candidate_positions": candidate_positions,
                "committees": committees_list,
            })

    races.sort(key=lambda x: x["total"], reverse=True)

    insights_col = db.client.collection("raceInsights")
    insights_col.document("races").set({"races": races})

    # Remove superseded single-purpose documents from earlier versions.
    for stale in ("crossSector", "multiPac"):
        insights_col.document(stale).delete()

    logging.info(
        f"Wrote {len(races)} race insights ("
        f"{sum(r['is_adversarial'] for r in races)} adversarial, "
        f"{sum(r['is_coordinated'] for r in races)} coordinated, "
        f"{sum(r['is_cross_sector'] for r in races)} cross-sector)"
    )
    _log_insights(races)


def _money(amount):
    return f"${amount:,.0f}"


def _log_insights(races):
    """Log the adversarial and coordinated races so a pipeline run surfaces where
    tracked PACs are spending against each other or pushing the same direction."""
    adversarial = [r for r in races if r["is_adversarial"]]
    coordinated = [r for r in races if r["is_coordinated"]]

    lines = ["", "=" * 72, "RACE INSIGHTS", "=" * 72]

    lines.append(f"\nAdversarial races (PACs spending against each other): {len(adversarial)}")
    for r in adversarial:
        reasons = ", ".join(r["adversarial_reasons"])
        lines.append(f"\n  {r['race_id']}  {_money(r['total'])}  [{reasons}]")
        for p in r["candidate_positions"]:
            sup = p["supporting_committees"]
            opp = p["opposing_committees"]
            if not (sup and opp):
                continue  # only the clashed candidates are interesting here
            flag = " (contested)" if p["contested"] else ""
            lines.append(f"      {p['candidate']}{flag}")
            for c in sup:
                lines.append(f"        + {c['name']} {_money(c['amount'])}")
            for c in opp:
                lines.append(f"        - {c['name']} {_money(c['amount'])}")
        # Rival backing: distinct candidates each drawing support.
        backed = [p for p in r["candidate_positions"] if p["supporting_committees"]]
        if len(backed) >= 2:
            lines.append("      backing rivals:")
            for p in backed:
                names = ", ".join(c["name"] for c in p["supporting_committees"])
                lines.append(f"        {p['candidate']} <- {names}")

    lines.append(f"\nCoordinated races (PACs spending the same direction): {len(coordinated)}")
    for r in coordinated:
        lines.append(f"\n  {r['race_id']}  {_money(r['total'])}")
        for p in r["candidate_positions"]:
            sup = p["supporting_committees"]
            opp = p["opposing_committees"]
            if len(sup) >= 2:
                names = ", ".join(c["name"] for c in sup)
                lines.append(f"      support {p['candidate']}: {names}")
            if len(opp) >= 2:
                names = ", ".join(c["name"] for c in opp)
                lines.append(f"      oppose  {p['candidate']}: {names}")

    lines.append("\n" + "=" * 72)
    logging.info("\n".join(lines))
