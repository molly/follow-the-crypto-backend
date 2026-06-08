"""
Utilities for reading and writing raceDetails data, which is sharded across
multiple Firestore documents to stay under the 1MB limit.

Documents are named {state}_{digit} (e.g. CA_0, CA_1), sharded by last digit
of district number for House races; Senate and all other races go to shard 0.
Legacy single-doc names (e.g. CA) are supported for backwards-compatible reads.
"""

import logging

from states import SPECIAL_ELECTIONS, is_current_special, canonical_race_keys

SHARD_COUNT = 10


def _shard_index(race_id: str) -> int:
    """Return 0-9 shard index for this race_id."""
    parts = race_id.split("-")
    if parts[0] == "H" and len(parts) > 1:
        try:
            return int(parts[1]) % SHARD_COUNT
        except ValueError:
            return 0
    return 0


def _shard_doc_name(state: str, race_id: str) -> str:
    return f"{state}_{_shard_index(race_id)}"


def get_races_for_state(db_client, state: str) -> dict:
    """Read all races for a state, merging sharded documents."""
    collection = db_client.collection("raceDetails")
    all_races = {}

    for digit in range(SHARD_COUNT):
        doc = collection.document(f"{state}_{digit}").get()
        if doc.exists:
            data = doc.to_dict()
            if data:
                all_races.update(data)

    return all_races


def get_all_races(db_client) -> dict:
    """Read all race details from all states, merging sharded documents.
    Returns {state: {race_id: race_data}}."""
    all_races = {}
    for doc in db_client.collection("raceDetails").stream():
        doc_id = doc.id
        # Support both sharded (e.g. "CA_0") and legacy (e.g. "CA") doc names
        parts = doc_id.rsplit("_", 1)
        if len(parts) == 2 and parts[1].isdigit():
            state = parts[0]
        else:
            state = doc_id
        data = doc.to_dict() or {}
        if state not in all_races:
            all_races[state] = {}
        all_races[state].update(data)
    return all_races


def get_most_recent_race_result(race_data: dict, candidate_name: str):
    """The candidate's recorded win/loss in the most recent subrace they were in.

    Returns True if they won it, False if they lost it, or None if that race has
    no recorded result yet (e.g. an uncalled co-winner in a multi-winner primary,
    or an upcoming race). Win/loss is derived from the per-race `won` flags rather
    than a stored summary field, which keeps it correct when races are edited
    between summarize runs.
    """
    involved = [
        race
        for race in race_data.get("races", [])
        if any(c.get("name") == candidate_name for c in race.get("candidates", []))
    ]
    if not involved:
        return None
    most_recent = max(involved, key=lambda r: r.get("date") or "")
    entry = next(
        (
            c
            for c in most_recent.get("candidates", [])
            if c.get("name") == candidate_name
        ),
        None,
    )
    return entry.get("won") if entry else None


def is_defeated(race_data: dict, candidate_name: str) -> bool:
    """Whether the candidate lost their most recent (finished, called) race."""
    return get_most_recent_race_result(race_data, candidate_name) is False


def save_races_for_state(db_client, state: str, race_data: dict) -> None:
    """Write race data for a state, splitting into sharded Firestore documents."""
    shards: dict = {}
    for race_id, data in race_data.items():
        doc_name = _shard_doc_name(state, race_id)
        if doc_name not in shards:
            shards[doc_name] = {}
        shards[doc_name][race_id] = data

    collection = db_client.collection("raceDetails")
    for doc_name, shard_data in shards.items():
        collection.document(doc_name).set(shard_data)


def update_race(db_client, state: str, race_id: str, updates: dict) -> None:
    """Update specific fields in the correct shard document for a race."""
    doc_name = _shard_doc_name(state, race_id)
    doc_ref = db_client.collection("raceDetails").document(doc_name)
    doc_ref.update(updates)


def prune_stale_race_details(db_client, valid_race_data: dict) -> None:
    """Delete raceDetails shard documents that contain no valid races.

    Should be called after save_scraped_races has written all valid shards.
    Any existing shard doc not referenced by valid_race_data has no surviving
    races and is deleted. This includes legacy single-state docs (e.g. "CA")
    left over from before sharding was introduced.

    valid_race_data: {state: {race_id: race_data}}
    """
    valid_shard_names = {
        _shard_doc_name(state, race_id)
        for state, state_races in valid_race_data.items()
        for race_id in state_races
    }

    collection = db_client.collection("raceDetails")
    for doc in collection.stream():
        if doc.id not in valid_shard_names:
            doc.reference.delete()
            logging.info(f"Deleted stale raceDetails document: {doc.id}")


def _shard_state(doc_id: str) -> str:
    """State for a raceDetails shard doc id ("AZ_6" -> "AZ", legacy "AZ" -> "AZ")."""
    parts = doc_id.rsplit("_", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0]
    return doc_id


def compute_tracked_race_ids(db) -> set:
    """Full race ids that should have a raceDetails entry — the exact inverse of
    the healthcheck's orphaned_spending check.

    A race is tracked when it has PAC spending (always hydrated) or clears the
    per-candidate direct-support gate (significant_company_races). Both consumers
    derive from the same spending buckets / gate so prune and the orphan check can
    never disagree. Keys are full ids (e.g. "AZ-H-06", including special variants),
    matching by_race / by_race_companies and compute_significant_direct_support.
    """
    # Lazy import: recipient_utils imports race_utils (via
    # process_company_state_spending), so importing it at module load would cycle.
    from recipient_utils import compute_significant_direct_support

    tracked = set(compute_significant_direct_support(db)["race_ids"])
    states_data = (
        db.client.collection("expenditures").document("states").get().to_dict() or {}
    )
    for state, data in states_data.items():
        if state in ("US", "None", None):
            continue
        # PAC spending (by_race) should always be hydrated, mirroring the orphan
        # check's PAC branch.
        tracked.update(data.get("by_race", {}))
    return tracked


def prune_untracked_races(db) -> None:
    """Remove races from raceDetails that have no tracked activity.

    A race is kept if it should have a raceDetails entry per
    compute_tracked_race_ids — i.e. it has PAC spending or clears the
    direct-support gate, the same rule the healthcheck's orphaned_spending check
    uses to flag missing races. Keying both off one source means a race the orphan
    check expects can never be pruned out from under it (which would otherwise
    oscillate it in and out every run). Races whose candidate data hasn't been
    populated yet are left alone.

    Should run after summarize_races so newly scraped races are present.
    """
    tracked_race_ids = compute_tracked_race_ids(db)
    collection = db.client.collection("raceDetails")
    for doc in collection.stream():
        state = _shard_state(doc.id)
        data = doc.to_dict() or {}
        kept = {}
        removed = []
        for race_id, race_data in data.items():
            if not race_data.get("candidates", {}):
                # summarize_races hasn't populated this race yet — leave it
                kept[race_id] = race_data
                continue
            if f"{state}-{race_id}" in tracked_race_ids:
                kept[race_id] = race_data
            else:
                removed.append(race_id)
        if removed:
            if kept:
                doc.reference.set(kept)
            else:
                doc.reference.delete()
            logging.info(f"Pruned untracked races from {doc.id}: {removed}")


def validate_special_elections(
    db, detail_ids=None, states_data=None, significant_company_races=None
) -> list:
    """Guardrail for the hand-maintained SPECIAL_ELECTIONS map.

    SPECIAL_ELECTIONS is domain knowledge that can't be derived from FEC data, so
    it drifts silently as new specials are called or seats are reclassified. This
    surfaces that drift by cross-checking the map against the spending buckets and
    scraped raceDetails. It catches:

      1. Misclassification: a current-cycle special-only seat (has_regular=False)
         that nonetheless accumulated spending on its bare regular key — meaning
         the "-special" routing isn't being applied.
      2. Missing special detail: a canonical race key with spending that should
         have been hydrated but has no raceDetails entry. "Should have been
         hydrated" mirrors the scraper's inclusion rule: PAC spending (by_race) is
         always scraped, while company-only spending (by_race_companies) is only
         scraped when the race clears the per-candidate direct-support gate. A
         company-only special whose money is all sub-threshold (e.g. a single
         small direct contribution split onto the "-special" key by
         canonical_race_keys) is excluded from scraping by design, so it is not
         drift — flagging it would contradict the scraper and the healthcheck's
         orphaned_spending check, which both apply the same gate.

    (General "spending but no detail" orphans are reported by the healthcheck's
    orphaned-spending check, which ranks them by dollar amount.)

    Logs every finding at WARNING level and returns them so a pipeline task can
    fail loudly. Read-only. Pass detail_ids/states_data/significant_company_races
    to reuse already-loaded data, otherwise they're fetched here.
    """

    def short_id(state, race_id):
        parts = race_id.split("-")
        return "-".join(parts[1:]) if parts[0] == state else race_id

    warnings = []

    def warn(message):
        warnings.append(message)
        logging.warning("SPECIAL_ELECTIONS drift — %s", message)

    if detail_ids is None:
        detail_ids = {
            state: set(races.keys())
            for state, races in get_all_races(db.client).items()
        }
    if states_data is None:
        states_data = (
            db.client.collection("expenditures").document("states").get().to_dict()
            or {}
        )
    if significant_company_races is None:
        # Imported lazily to avoid a module-load cycle (recipient_utils imports
        # race_utils via process_company_state_spending).
        from recipient_utils import compute_significant_direct_support

        significant_company_races = compute_significant_direct_support(db)["race_ids"]

    for seat in SPECIAL_ELECTIONS:
        if not is_current_special(seat):
            continue
        state = seat.split("-")[0]
        have = detail_ids.get(state, set())
        state_data = states_data.get(state, {})
        by_race = set(state_data.get("by_race", {}))
        by_companies = set(state_data.get("by_race_companies", {}))
        spending_keys = by_race | by_companies

        if not SPECIAL_ELECTIONS[seat]["has_regular"] and seat in spending_keys:
            warn(
                f"{seat}: classified special-only but has spending on the regular "
                f"key — expected it to route to {seat}-special"
            )

        for key in canonical_race_keys(seat):
            if short_id(state, key) in have:
                continue
            # PAC spending should always be hydrated; company-only spending only
            # when it clears the direct-support gate (same rule as the scraper).
            if key in by_race or (
                key in by_companies and key in significant_company_races
            ):
                warn(f"{key}: spending present but no raceDetails entry")

    if not warnings:
        logging.info("validate_special_elections: no drift detected")
    return warnings
