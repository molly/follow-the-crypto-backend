"""Compute race-level cross-sector and multi-PAC spending insights."""

import logging
from race_utils import get_all_races
from utils import get_sector_keys


def _build_committee_breakdown(spending, committee_ids, committees_meta):
    """Build a per-committee list with a candidate support/oppose breakdown.

    For each committee in committee_ids with spending in this race, aggregate
    support/oppose amounts per candidate across all subraces (flattened), so the
    result captures which candidates each PAC supported or opposed.

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
            "candidates": candidates_list,
        })

    committees_list.sort(key=lambda x: x["total"], reverse=True)
    return committees_list


def compute_race_insights(db):
    """
    Compute and store:
      raceInsights/crossSector  – races with both crypto and AI PAC spending
      raceInsights/multiPac     – races where >=2 tracked PACs are spending

    Both documents include, per qualifying race, a `committees` breakdown listing
    which candidates each tracked PAC supported or opposed.
    """
    all_race_data = get_all_races(db.client)
    tracked_committee_ids = set(db.committees.keys()) if db.committees else set()

    cross_sector = []
    multi_pac = []

    for state, state_data in all_race_data.items():
        for race_id, race_data in state_data.items():
            full_race_id = f"{state}-{race_id}"
            candidates = race_data.get("candidates", {})
            spending = race_data.get("spending", {})

            # Tracked committees with spending in this race
            active_tracked = [
                cid for cid in spending
                if cid in tracked_committee_ids and (spending[cid].get("total") or 0) > 0
            ]

            # Cross-sector: both crypto and AI spending > 0 in this race
            crypto_total = sum(
                (c.get("crypto_support_total") or 0) + (c.get("crypto_oppose_total") or 0)
                for c in candidates.values()
            )
            ai_total = sum(
                (c.get("ai_support_total") or 0) + (c.get("ai_oppose_total") or 0)
                for c in candidates.values()
            )
            if crypto_total > 0 and ai_total > 0:
                cross_sector.append({
                    "race_id": full_race_id,
                    "crypto_total": round(crypto_total, 2),
                    "ai_total": round(ai_total, 2),
                    "total": round(crypto_total + ai_total, 2),
                    "committees": _build_committee_breakdown(
                        spending, active_tracked, db.committees
                    ),
                })

            # Multi-PAC: >=2 tracked committees with spending in this race
            if len(active_tracked) >= 2:
                committees_list = _build_committee_breakdown(
                    spending, active_tracked, db.committees
                )
                race_total = sum(c["total"] for c in committees_list)
                multi_pac.append({
                    "race_id": full_race_id,
                    "committees": committees_list,
                    "pac_count": len(active_tracked),
                    "total": round(race_total, 2),
                })

    cross_sector.sort(key=lambda x: x["total"], reverse=True)
    multi_pac.sort(key=lambda x: x["total"], reverse=True)

    insights_col = db.client.collection("raceInsights")
    insights_col.document("crossSector").set({"races": cross_sector})
    insights_col.document("multiPac").set({"races": multi_pac})
    logging.info(
        f"Wrote {len(cross_sector)} cross-sector races and {len(multi_pac)} multi-PAC races"
    )
