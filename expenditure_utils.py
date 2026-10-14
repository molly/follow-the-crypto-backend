"""
Utilities for reading and writing the full set of independent expenditures,
which is sharded across Firestore documents to stay under the per-document
index entry limit (40,000) and size limit (1 MiB). Every leaf field of every
expenditure gets its own index entries, so a single document holding all of
them overflows as the cycle goes on.

Documents are expenditures/all_0 through expenditures/all_{NUM_SHARDS - 1},
each a map of uid -> expenditure. Rows are assigned by a stable hash of the uid
rather than by committee ID, since a handful of committees account for most of
the spending and would leave the shards badly lopsided.
"""

import zlib

COLLECTION = "expenditures"
SHARD_PREFIX = "all_"
NUM_SHARDS = 10


def _shard_id(uid: str) -> str:
    return f"{SHARD_PREFIX}{zlib.crc32(uid.encode()) % NUM_SHARDS}"


def get_all_expenditures(db) -> dict:
    """Read every expenditure, keyed by uid."""
    collection = db.client.collection(COLLECTION)
    all_expenditures = {}
    for i in range(NUM_SHARDS):
        doc = collection.document(f"{SHARD_PREFIX}{i}").get()
        if doc.exists:
            all_expenditures.update(doc.to_dict() or {})
    return all_expenditures


def set_all_expenditures(db, expenditures: dict) -> None:
    """Replace the full set of expenditures.

    Every shard is written, including empty ones, so a row that moves out of
    the set doesn't linger in a shard nobody rewrote. The writes go in one batch
    so readers never see a mix of old and new shards.
    """
    shards = {f"{SHARD_PREFIX}{i}": {} for i in range(NUM_SHARDS)}
    for uid, expenditure in expenditures.items():
        shards[_shard_id(uid)][uid] = expenditure

    collection = db.client.collection(COLLECTION)
    batch = db.client.batch()
    for doc_id, shard_data in shards.items():
        batch.set(collection.document(doc_id), shard_data)
    batch.commit()


def update_expenditure_field(db, uid: str, field: str, value) -> None:
    """Set a single field on one stored expenditure."""
    db.client.collection(COLLECTION).document(_shard_id(uid)).update(
        {db.client.field_path(uid, field): value}
    )
