"""
Utilities for reading and writing raceDetails data, which is sharded across
multiple Firestore documents to stay under the 1MB limit.

Documents are named {state}_{digit} (e.g. CA_0, CA_1), sharded by last digit
of district number for House races; Senate and all other races go to shard 0.
Legacy single-doc names (e.g. CA) are supported for backwards-compatible reads.
"""

import logging

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


def prune_untracked_races(db_client) -> None:
    """Remove races from raceDetails that have no tracked activity.

    A race is kept if any candidate has PAC expenditures (support_total or
    oppose_total > 0) or significant direct industry support
    (has_non_pac_support). Races with no qualifying candidates are removed.
    Races whose candidate data hasn't been populated yet are left alone.

    Should run after summarize_races so candidate flags are up to date.
    """
    collection = db_client.collection("raceDetails")
    for doc in collection.stream():
        data = doc.to_dict() or {}
        kept = {}
        removed = []
        for race_id, race_data in data.items():
            candidates = race_data.get("candidates", {})
            if not candidates:
                # summarize_races hasn't populated this race yet — leave it
                kept[race_id] = race_data
                continue
            is_tracked = any(
                c.get("support_total", 0) > 0
                or c.get("oppose_total", 0) > 0
                or c.get("has_non_pac_support")
                for c in candidates.values()
            )
            if is_tracked:
                kept[race_id] = race_data
            else:
                removed.append(race_id)
        if removed:
            if kept:
                doc.reference.set(kept)
            else:
                doc.reference.delete()
            logging.info(f"Pruned untracked races from {doc.id}: {removed}")
