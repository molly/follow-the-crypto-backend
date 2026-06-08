"""
Utilities for reading and writing the allRecipients/recipients data, which is
sharded across 10 Firestore documents to stay under the index entry limit.

Documents are named recipients_0 through recipients_9, sharded by the last
character of the committee ID (always a digit for FEC IDs).
"""

SHARD_PREFIX = "recipients_"
LEGACY_DOC = "recipients"

# A race is considered significantly supported if total contributions meet this
# threshold, or if any single contributor's total meets the per-contributor threshold.
DIRECT_SUPPORT_TOTAL_THRESHOLD = 25_000
DIRECT_SUPPORT_CONTRIBUTOR_THRESHOLD = 10_000


def has_significant_direct_support(recipient: dict) -> bool:
    """Return True if a recipient has significant direct industry support.

    Passes the bar if:
    - Total contributions from all tracked sources >= $25,000, OR
    - Any single contributor's total contributions >= $10,000
    """
    if recipient.get("total", 0) >= DIRECT_SUPPORT_TOTAL_THRESHOLD:
        return True
    return any(
        c["total"] >= DIRECT_SUPPORT_CONTRIBUTOR_THRESHOLD
        for group in recipient.get("contributions", [])
        for c in group.get("contributions", [])
    )


def compute_significant_direct_support(db, recipients=None) -> dict:
    """Single source of truth for the per-CANDIDATE direct-support scrape gate.

    A candidate clears the gate when their total direct (company) contributions
    reach DIRECT_SUPPORT_TOTAL_THRESHOLD, or any single contribution to them
    reaches DIRECT_SUPPORT_CONTRIBUTOR_THRESHOLD. Returns:
      - "candidates": canonical beneficiary ids that clear the gate.
      - "race_ids": the full race ids (e.g. "NJ-H-07", including special variants)
        those candidates' seats route to -- the races the scraper targets from
        direct contributions, keyed the same way by_race_companies is.

    update_race_details consumes "candidates" to choose which contributions to
    scrape; the healthcheck consumes "race_ids" to separate a real orphan (a race
    that should have been hydrated) from a sub-threshold company-only race that is
    excluded from scraping by design. Keeping both in one place prevents the two
    consumers from drifting to different thresholds.

    Only candidates actually running this cycle are credited. A principal campaign
    committee links every candidacy a person has ever filed (e.g. an old House run
    plus the Senate seat they now hold), and FEC returns all of those ids on the
    contribution group. Crediting the stale ones routed a senator's money to the
    House seat they vacated, flagging that seat as a spurious orphan. The
    running-this-cycle gate (isRunningThisCycle or on the seat roster) mirrors the
    primary gate in process_company_state_spending.compute_company_state_spending,
    so this function and by_race_companies agree on which candidacies count.
    """
    # Imported lazily to avoid a module-load cycle (process_company_state_spending
    # and race_utils both import this module).
    from process_company_state_spending import get_race_id, candidate_roster_status
    from states import canonical_race_keys
    from race_utils import get_all_races

    if recipients is None:
        recipients = get_all_recipients(db)

    all_races = get_all_races(db.client)

    totals: dict = {}
    max_contrib: dict = {}
    details_by_candidate: dict = {}
    for doc in db.client.collection("companies").stream():
        company = doc.to_dict()
        for group in company.get("contributions", []):
            committee_id = group.get("committee_id")
            if not committee_id or committee_id in (db.non_candidate_committees or set()):
                continue
            recipient_committee = recipients.get(committee_id)
            if not recipient_committee:
                continue
            candidate_ids = set(recipient_committee.get("candidate_ids", []) or [])
            candidate_details = recipient_committee.get("candidate_details", {})
            for cid in candidate_ids:
                cand_details = candidate_details.get(cid)
                if not cand_details or cand_details.get("office") not in {"H", "S"}:
                    continue
                # Skip candidacies that aren't this cycle's, so a person's stale
                # filings don't credit money to a seat they no longer run for.
                _, on_roster = candidate_roster_status(all_races, cand_details, cid)
                if not (cand_details.get("isRunningThisCycle", False) or on_roster):
                    continue
                # An aliased id is running under a different candidacy; its own
                # committee's race is stale (matches compute_company_state_spending).
                if cid in db.candidate_aliases:
                    continue
                canonical = db.candidate_aliases.get(cid, cid)
                totals[canonical] = totals.get(canonical, 0) + group["total"]
                for contrib in group.get("contributions", []):
                    amount = (
                        contrib.get("total_receipt_amount")
                        or contrib.get("contribution_receipt_amount", 0)
                    )
                    if amount > max_contrib.get(canonical, 0):
                        max_contrib[canonical] = amount
                if canonical not in details_by_candidate:
                    details_by_candidate[canonical] = cand_details

    candidates = {
        candidate
        for candidate, total in totals.items()
        if total >= DIRECT_SUPPORT_TOTAL_THRESHOLD
        or max_contrib.get(candidate, 0) >= DIRECT_SUPPORT_CONTRIBUTOR_THRESHOLD
    }

    race_ids: set = set()
    for candidate in candidates:
        cand_details = details_by_candidate.get(candidate)
        if not cand_details or cand_details.get("office") not in {"H", "S"}:
            continue
        base_seat = get_race_id(cand_details)
        if base_seat:
            race_ids.update(canonical_race_keys(base_seat))

    return {"candidates": candidates, "race_ids": race_ids}


def resolve_recipient_party(committee: dict) -> str:
    """Resolve a recipient committee's party for by-party summaries.

    Prefers the committee's own party (ignoring nonpartisan "N*" codes), then
    falls back to a unanimous party among its candidate_details. Returns "UNK"
    when neither yields a partisan value.
    """
    party = committee.get("party")
    if party is not None and not party.startswith("N"):
        return party
    parties = [
        c.get("party")
        for c in committee.get("candidate_details", {}).values()
        if c.get("party") is not None
    ]
    if len(set(parties)) == 1 and not parties[0].startswith("N"):
        return parties[0]
    return "UNK"


def _shard_key(committee_id: str) -> str:
    return f"{SHARD_PREFIX}{committee_id[-1]}"


def get_all_recipients(db) -> dict:
    """Read all recipients from sharded documents, falling back to the legacy
    single document for backwards compatibility during migration."""
    collection = db.client.collection("allRecipients")

    all_recipients = {}
    for digit in "0123456789":
        doc = collection.document(f"{SHARD_PREFIX}{digit}").get()
        if doc.exists:
            data = doc.to_dict()
            if data:
                all_recipients.update(data)

    if all_recipients:
        return all_recipients

    # Fall back to pre-sharding single document
    doc = collection.document(LEGACY_DOC).get()
    return doc.to_dict() if doc.exists else {}


def set_all_recipients(db, recipients: dict) -> None:
    """Write recipients to sharded documents."""
    shards: dict = {}
    for committee_id, data in recipients.items():
        key = _shard_key(committee_id)
        if key not in shards:
            shards[key] = {}
        shards[key][committee_id] = data

    collection = db.client.collection("allRecipients")
    for doc_id, shard_data in shards.items():
        collection.document(doc_id).set(shard_data)
