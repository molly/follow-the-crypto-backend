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
