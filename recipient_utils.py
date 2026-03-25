"""
Utilities for reading and writing the allRecipients/recipients data, which is
sharded across 10 Firestore documents to stay under the index entry limit.

Documents are named recipients_0 through recipients_9, sharded by the last
character of the committee ID (always a digit for FEC IDs).
"""

SHARD_PREFIX = "recipients_"
LEGACY_DOC = "recipients"


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
