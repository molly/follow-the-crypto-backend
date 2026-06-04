"""
Race merging utility for preserving manual race entries.

This module handles merging scraped race data with manually-entered race data
when saving to Firestore.
"""

from typing import List, Dict, Any, Optional
import logging
import time
from race_utils import get_races_for_state, save_races_for_state


def merge_manual_into_scraped(
    scraped_races: List[Dict[str, Any]],
    manual_races: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Merge manually-entered race entries into scraped race data.

    For each manual race, finds a matching scraped race (same type and party).
    If a match is found, any manual candidates not already in the scraped race
    are appended, and existing candidates are updated with manually-set fields
    (e.g., declined=True).  If no matching scraped race exists, the manual race
    is appended as a new entry so that entirely hand-entered races are included.

    Args:
        scraped_races: Races list from Ballotpedia scrape.
        manual_races:  Races list from the admin UI (manualRaces field).

    Returns:
        Merged list of race dicts.
    """
    result = [
        dict(race, candidates=list(race.get("candidates", [])))
        for race in scraped_races
    ]

    for manual_race in manual_races:
        manual_type = manual_race.get("type")
        manual_party = manual_race.get("party")

        matching_idx = next(
            (
                i
                for i, r in enumerate(result)
                if r.get("type") == manual_type and r.get("party") == manual_party
            ),
            None,
        )

        if matching_idx is not None:
            existing_names = {c["name"] for c in result[matching_idx]["candidates"]}
            for manual_candidate in manual_race.get("candidates", []):
                cname = manual_candidate["name"]
                if cname not in existing_names:
                    result[matching_idx]["candidates"].append(manual_candidate)
                    existing_names.add(cname)
                else:
                    for i, c in enumerate(result[matching_idx]["candidates"]):
                        if c["name"] == cname:
                            result[matching_idx]["candidates"][i] = {
                                **c,
                                **manual_candidate,
                            }
                            break
        else:
            result.append(dict(manual_race))

    return result


def generate_race_key(race: Dict[str, Any]) -> str:
    """
    Generate a unique key for a race based on type, party, and date.

    Args:
        race: Race dictionary with 'type', 'party' (optional), and 'date'

    Returns:
        String key like "primary-R-2026-03-05"
    """
    race_type = race.get('type', '')
    party = race.get('party', 'none')
    date = race.get('date', '')
    return f"{race_type}-{party}-{date}"


def meaningful_races_signature(races: List[Dict[str, Any]]) -> frozenset:
    """
    Build an order-independent signature of the parts of a scraped races list
    that warrant human review.

    Two scrapes with the same signature describe the same set of subraces, the
    same candidates in each, and the same outcomes — only re-review-worthy
    changes (a new subrace, a new candidate, or a changed result) alter it.
    Volatile presentational fields like vote percentages are intentionally
    excluded so that re-scraping unchanged data does not re-flag a race.

    Args:
        races: A scrapedRaces-style list of race dicts.

    Returns:
        A frozenset usable for equality comparison between two scrapes.
    """
    signature = set()
    for race in races:
        candidates = frozenset(
            (candidate.get("name"), candidate.get("won"))
            for candidate in race.get("candidates", [])
        )
        signature.add(
            (
                generate_race_key(race),
                bool(race.get("canceled", False)),
                candidates,
            )
        )
    return frozenset(signature)


def save_scraped_races(db_client, state: str, race_data: Dict[str, Any]) -> None:
    """
    Save scraped race data to Firestore in the scrapedRaces field.

    This function preserves existing manualRaces and races fields.
    The races field is the reviewed/merged result produced by the review
    screen and must not be overwritten by the scraper.

    Args:
        db_client: Firestore client instance
        state: State abbreviation (e.g., 'AZ', 'CA')
        race_data: Dictionary of race groups for the state
                   Format: {race_id: {"races": [...], "withdrew": {...}, ...}}

    Example:
        save_scraped_races(
            db_client=db.client,
            state='AZ',
            race_data={
                'H-01': {
                    'races': [
                        {'type': 'primary', 'party': 'R', 'date': '2026-03-05', ...}
                    ],
                    'withdrew': {...}
                }
            }
        )
    """
    # Fetch existing data to preserve manualRaces and races fields
    existing_data = get_races_for_state(db_client, state)

    # Get current timestamp in milliseconds
    current_timestamp = int(time.time() * 1000)

    # Process each race group
    for race_id, race_group in race_data.items():
        scraped_races = race_group.get('races', [])

        # Get existing fields to preserve
        existing_group = existing_data.get(race_id, {})
        manual_races = existing_group.get('manualRaces', [])
        reviewed_races = existing_group.get('races', [])
        manual_races_updated = existing_group.get('manualRacesUpdated', 0)
        last_reviewed = existing_group.get('lastReviewed', 0)
        existing_scraped_races = existing_group.get('scrapedRaces', [])
        existing_scraped_updated = existing_group.get('scrapedRacesUpdated', 0)

        # Save scraped data to scrapedRaces field. Only advance the
        # scrapedRacesUpdated timestamp — which is what re-flags the race for
        # review — when the scrape's review-worthy content (subraces, candidates,
        # outcomes) actually changed. Re-scraping identical data preserves the
        # prior timestamp so an already-reviewed race does not reappear.
        race_group['scrapedRaces'] = scraped_races
        if (
            meaningful_races_signature(scraped_races)
            == meaningful_races_signature(existing_scraped_races)
        ):
            race_group['scrapedRacesUpdated'] = existing_scraped_updated
        else:
            race_group['scrapedRacesUpdated'] = current_timestamp

        # Preserve manualRaces field if it exists
        if manual_races:
            race_group['manualRaces'] = manual_races
            race_group['manualRacesUpdated'] = manual_races_updated
            logging.info(f"Preserved {len(manual_races)} manual races for {state}/{race_id}")

        # Preserve races field if it exists (this is the reviewed/merged data
        # written by the review screen — the scraper must not overwrite it)
        if reviewed_races:
            race_group['races'] = reviewed_races
            race_group['lastReviewed'] = last_reviewed
            logging.info(f"Preserved {len(reviewed_races)} reviewed races for {state}/{race_id}")
        else:
            # If no reviewed races exist yet, initialize with scraped data
            race_group['races'] = scraped_races

        # Preserve candidates and spending fields set by summarize_races
        existing_candidates = existing_group.get('candidates', {})
        existing_spending = existing_group.get('spending', {})
        if existing_candidates:
            race_group['candidates'] = existing_candidates
        if existing_spending:
            race_group['spending'] = existing_spending

        logging.info(
            f"{state}/{race_id}: Saved {len(scraped_races)} scraped races"
        )

    # Write back to Firestore (sharded)
    save_races_for_state(db_client, state, race_data)
    logging.info(f"Saved scraped race details for {state} with {len(race_data)} race groups")
