"""
SAPI backfill worker.

Purpose
- Run a cursor-based crawl for one SAPI scope (country + catalogs bundle + params fingerprint).
- Fetch pages from SAPI.
- Persist, atomically per page:
  1) raw page blob (append-only)
  2) extracted indices (upserts)
  3) ledger checkpoint (cursor_next + counters)

Hard invariants
- Never keep a DB transaction open during HTTP.
- Advance cursor_next only after raw + indices are persisted successfully
  (same DB transaction as those writes).

Operational behavior
- Supports resumable runs via run_id reuse. If run_id is provided and exists,
  the worker continues from the stored cursor_next.
- Supports chunked runs via max_pages. If max_pages is reached, the worker stops
  without marking the run completed (cursor_next remains persisted for resume).

Non-responsibilities
- No deletes/pruning.
- No business logic beyond “persist what provider returned”.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from src.persistence.engine import DatabaseManager
from src.pipeline.ledger import (
    SapiRunLedgerStore,
    SapiRunScope,
)
from src.persistence.stores.raw import (
    SapiRawPagesStore,
)
from src.persistence.stores.indices import (
    SapiIndicesStore,
)
from src.client.client import SapiClient
from src.client.extract import extract_show

logger = logging.getLogger(__name__)


def _utcnow_naive() -> datetime:
    """
    Return naive UTC datetime.

    Your ORM uses DateTime without timezone, so this keeps consistency.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _normalize_catalogs_param(catalogs: Any) -> str:
    """
    Normalize catalogs into the format SAPI expects.

    Accepts:
    - "netflix,prime,disney" (str)
    - ["netflix", "prime", "disney"] (iterable)
    """
    if catalogs is None:
        return ""
    if isinstance(catalogs, str):
        return catalogs
    if isinstance(catalogs, Iterable):
        return ",".join([str(x).strip() for x in catalogs if str(x).strip()])
    return str(catalogs)


@dataclass(frozen=True)
class BackfillOptions:
    """
    Run options for the backfill worker.

    max_pages:
        If set, stop after persisting this many pages even if hasMore is True.
        This is a “pause” feature (run is not marked completed).
    chunk_size:
        Passed to SapiIndicesStore for batched upserts.
    """

    max_pages: int | None = None
    chunk_size: int = 1000


class SapiBackfillWorker:
    """
    Cursor-crawl worker for SAPI /shows/search/filters.

    The worker is intentionally simple. It is not a scheduler. It does not manage concurrency.
    It executes one run for one scope.
    """

    def __init__(self, *, db_manager: DatabaseManager, sapi_client: SapiClient) -> None:
        """
        Args:
            db_manager: Provides SQLAlchemy Session context manager.
            sapi_client: Thin HTTP client for SAPI.
        """
        self._db = db_manager
        self._client = sapi_client

    def run_backfill(
        self,
        *,
        scope: SapiRunScope,
        base_query_params: dict[str, Any],
        options: BackfillOptions | None = None,
        run_id: str | None = None,
    ) -> str:
        """
        Execute a backfill run for a scope.

        Args:
            scope: Run scope (country + catalogs bundle + params fingerprint).
            base_query_params: Query params for /shows/search/filters excluding cursor.
                This function will ensure country and catalogs are set consistently.
            options: BackfillOptions controlling max_pages and chunk_size.
            run_id: Optional run id to resume. If omitted, a new run is created.

        Returns:
            The run_id used for this run (useful for resuming or diagnostics).
        """
        opt = options or BackfillOptions()
        run_id = run_id or str(uuid.uuid4())

        # Handshake: ensure ledger row exists and load resume cursor.
        with self._db.get_session() as session:
            with session.begin():
                ledger = SapiRunLedgerStore(session)
                ledger.ensure_started(run_id=run_id, scope=scope)

            # Resume cursor can be read outside a transaction.
            ledger = SapiRunLedgerStore(session)
            cursor_next = ledger.get_cursor_next(run_id)

        logger.info(
            "SAPI_BACKFILL_START run_id=%s country=%s catalogs=%s fingerprint=%s cursor_next=%s max_pages=%s",
            run_id,
            scope.country,
            scope.catalogs_bundle,
            scope.params_fingerprint,
            cursor_next,
            opt.max_pages,
        )

        pages_done = 0

        try:
            while True:
                if opt.max_pages is not None and pages_done >= opt.max_pages:
                    logger.info(
                        "SAPI_BACKFILL_STOP_MAX_PAGES run_id=%s pages_done=%d",
                        run_id,
                        pages_done,
                    )
                    break

                cursor_used = cursor_next  # cursor used for this request (may be None)

                # Build query params (HTTP outside DB tx).
                query_params = dict(base_query_params)
                query_params["country"] = scope.country

                # Prefer explicit catalogs_bundle if caller didn't provide.
                if "catalogs" not in query_params or query_params["catalogs"] in (
                    None,
                    "",
                ):
                    query_params["catalogs"] = scope.catalogs_bundle
                else:
                    query_params["catalogs"] = _normalize_catalogs_param(
                        query_params["catalogs"]
                    )

                if cursor_used is not None:
                    query_params["cursor"] = cursor_used
                else:
                    query_params.pop("cursor", None)

                fetch_t0 = time.perf_counter()
                fetched_at = _utcnow_naive()
                resp = self._client.fetch_data("/shows/search/filters", query_params)
                fetch_ms = (time.perf_counter() - fetch_t0) * 1000.0

                shows = resp.get("shows") or []
                has_more = resp.get("hasMore")
                next_cursor = resp.get("nextCursor")

                persist_t0 = time.perf_counter()

                # Persist raw + indices + checkpoint atomically (one DB transaction).
                with self._db.get_session() as session:
                    with session.begin():
                        raw_store = SapiRawPagesStore(session)
                        idx_store = SapiIndicesStore(session, chunk_size=opt.chunk_size)
                        ledger = SapiRunLedgerStore(session)

                        raw_store.append_page(
                            run_id=run_id,
                            cursor_used=cursor_used,
                            fetched_at=fetched_at,
                            response_json=resp,
                        )

                        # Extract per show, then bulk upsert per table (per page).
                        titles = []
                        offers = []
                        assets = []

                        for raw_show in shows:
                            title_rec, offer_recs, asset_recs = extract_show(
                                raw_show, fetched_at=fetched_at, run_id=run_id
                            )
                            titles.append(title_rec)
                            offers.extend(offer_recs)
                            assets.extend(asset_recs)

                        if titles:
                            idx_store.upsert_titles(titles)
                        if offers:
                            idx_store.upsert_offers(offers)
                        if assets:
                            idx_store.upsert_assets(assets)

                        ledger.checkpoint_after_page(
                            run_id=run_id,
                            next_cursor=next_cursor,
                            has_more=has_more,
                            items_count=len(shows),
                        )

                persist_ms = (time.perf_counter() - persist_t0) * 1000.0

                pages_done += 1
                cursor_next = next_cursor

                logger.info(
                    "SAPI_BACKFILL_PAGE_OK run_id=%s page=%d items=%d has_more=%s fetch_ms=%.2f persist_ms=%.2f cursor_used=%s next_cursor=%s",
                    run_id,
                    pages_done,
                    len(shows),
                    has_more,
                    fetch_ms,
                    persist_ms,
                    (repr(cursor_used)) if cursor_used else "START",
                    (repr(next_cursor)) if next_cursor else "NONE",
                )

                if has_more is False:
                    logger.info(
                        "SAPI_BACKFILL_DONE run_id=%s pages=%d",
                        run_id,
                        pages_done,
                    )
                    break

        except Exception as e:
            # Mark failed in a separate small transaction, then re-raise.
            err = f"{type(e).__name__}: {e}"
            with self._db.get_session() as session:
                with session.begin():
                    ledger = SapiRunLedgerStore(session)
                    ledger.mark_failed(run_id=run_id, error=err)
            logger.exception("SAPI_BACKFILL_FAILED run_id=%s error=%s", run_id, err)
            raise

        return run_id
