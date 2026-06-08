"""
Workaround for a google-cloud-firestore bug (present through at least 2.16.1) that crashes
streamed queries when the gRPC channel goes stale.

When a `.stream()` query hits a transient gRPC error mid-stream — which happens when the
Firestore channel idles out during a long run (e.g. while the pipeline is busy throttling FEC
requests) — firestore tries to retry it via `Query._retry_query_after_exception`. That helper
reads `gapic_callable._retry`, but the streaming callable (`_UnaryStreamMultiCallable`) has no
`_retry` attribute, so it raises:

    AttributeError: '_UnaryStreamMultiCallable' object has no attribute '_retry'

That masks the real, retryable error and kills the task. `stream()` already knows how to
reconnect (it builds a fresh iterator with `start_after`) — it just needs the helper to return
True for transient errors instead of crashing. This patch makes the missing-`_retry` case fall
back to firestore's standard transient-error predicate.

Remove this once google-cloud-firestore is upgraded to a version that fixes the bug upstream.
"""

from google.api_core import gapic_v1
from google.api_core.retry import if_transient_error
from google.cloud.firestore_v1 import query as _query


def _safe_retry_query_after_exception(self, exc, retry, transaction):
    # no snapshot-based retry inside a transaction (matches upstream behavior)
    if transaction is not None:
        return False
    if retry is gapic_v1.method.DEFAULT:
        gapic_callable = self._client._firestore_api._transport.run_query
        default_retry = getattr(gapic_callable, "_retry", None)
        if default_retry is None:
            # The bug: streaming callable has no `_retry`. Use firestore's transient-error
            # predicate so a stale-channel error reconnects instead of crashing.
            return if_transient_error(exc)
        return default_retry._predicate(exc)
    return retry._predicate(exc)


def apply():
    """Idempotently install the patched stream-retry helper onto Query."""
    if getattr(_query.Query._retry_query_after_exception, "_ftc_patched", False):
        return
    _safe_retry_query_after_exception._ftc_patched = True
    _query.Query._retry_query_after_exception = _safe_retry_query_after_exception
