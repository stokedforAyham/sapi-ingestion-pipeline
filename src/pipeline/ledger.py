"""
SAPI run-ledger persistence (v0).

This module persists and updates a single evolving row per ingestion run
(`SapiRunLedger`). It is intentionally small and boring.

What it does:
- Create (or fetch) a run row identified by run_id.
- Store a resumable checkpoint cursor (`cursor_next`).
- Track coarse progress counters (`pages_processed`, `items_processed`).
- Mark lifecycle states: started -> running -> completed | failed.
- Query "latest completed run id" per scope to support currentness logic.

What it does not do:
- No locking or concurrency coordination (v0).

Important invariant:
- Update `cursor_next` only after raw page persistence + index upserts succeed,
  and do it in the same DB transaction (worker responsibility).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import logging
from sqlalchemy import Select, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.persistence.tables import (
    SapiRunLedger,
)

logger = logging.getLogger(__name__)


STATUS_STARTED = "started"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"


@dataclass(frozen=True)
class SapiRunScope:
    """
    The logical identity of a run "scope".

    Runs are scoped by:
    - country
    - catalogs_bundle (canonical sorted string, comma-separated)
    - params_fingerprint (stable hash of pagination/membership-affecting params)
    """

    country: str
    catalogs_bundle: str
    params_fingerprint: str


def canonical_catalogs_bundle(catalogs: Iterable[str]) -> str:
    """
    Canonicalize catalogs into a stable comma-separated string.

    Example:
        ["prime", "netflix"] -> "netflix,prime"
    """
    return ",".join(sorted({c.strip() for c in catalogs if c and c.strip()}))


def _utcnow_naive() -> datetime:
    """
    Return a naive UTC timestamp.

    Note:
        The models currently use `DateTime` without timezone info, so we keep
        naive UTC consistently.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


class SapiRunLedgerStore:
    """
    Repository for the SAPI run ledger.

    Intended use (inside worker loop):
        with Session.begin() as session:
            ledger = SapiRunLedgerStore(session)

            cursor_used = ledger.get_cursor_next(run_id)
            # fetch page using cursor_used ...

            # persist raw + indices first ...
            # then checkpoint:
            ledger.checkpoint_after_page(
                run_id=run_id,
                next_cursor=response_next_cursor,
                has_more=response_has_more,
                items_count=len(shows),
            )
    """

    def __init__(self, session: Session) -> None:
        """
        Args:
            session: SQLAlchemy Session bound to the source-store database.
        """
        self._session = session

    # ---------------------------------------------------------------------
    # Run creation / lookup
    # ---------------------------------------------------------------------

    def get(self, run_id: str) -> SapiRunLedger | None:
        """
        Fetch a run row by run_id.
        """
        return self._session.get(SapiRunLedger, run_id)

    def ensure_started(
        self,
        *,
        run_id: str,
        scope: SapiRunScope,
        started_at: datetime | None = None,
    ) -> SapiRunLedger:
        """
        Create the run row if missing; otherwise return the existing row.

        The row starts in STATUS_STARTED.
        """
        row = self._session.get(SapiRunLedger, run_id)
        if row is not None:
            return row

        row = SapiRunLedger(
            run_id=run_id,
            country=scope.country,
            catalogs_bundle=scope.catalogs_bundle,
            params_fingerprint=scope.params_fingerprint,
            status=STATUS_STARTED,
            started_at=started_at or _utcnow_naive(),
            ended_at=None,
            last_error=None,
            cursor_next=None,
            pages_processed=0,
            items_processed=0,
        )

        self._session.add(row)
        try:
            # Flush so the caller sees integrity errors early if run_id clashes.
            self._session.flush()
        except IntegrityError:
            # Another transaction inserted it first. Load and return.
            self._session.rollback()
            existing = self._session.get(SapiRunLedger, run_id)
            if existing is None:
                raise
            return existing

        logger.info(
            "sapi_ledger_run_started run_id=%s country=%s catalogs=%s",
            run_id,
            scope.country,
            scope.catalogs_bundle,
        )
        return row

    # ---------------------------------------------------------------------
    # Cursor checkpointing
    # ---------------------------------------------------------------------

    def get_cursor_next(self, run_id: str) -> str | None:
        """
        Return the cursor to use for the next request.

        None means "first page" (omit cursor param in the API call).
        """
        row = self._session.get(SapiRunLedger, run_id)
        if row is None:
            return None
        return row.cursor_next

    def set_running(self, run_id: str) -> None:
        """
        Mark the run as running (idempotent).

        Safe to call repeatedly.
        """
        stmt = (
            update(SapiRunLedger)
            .where(SapiRunLedger.run_id == run_id)
            .values(status=STATUS_RUNNING, last_error=None)
        )
        self._session.execute(stmt)

    def checkpoint_after_page(
        self,
        *,
        run_id: str,
        next_cursor: str | None,
        has_more: bool | None,
        items_count: int,
    ) -> None:
        """
        Advance the run checkpoint after a page was durably persisted.

        This must be called only after:
        - raw page blob append succeeded
        - index upserts succeeded

        Args:
            run_id: Current run id.
            next_cursor: Value from provider response (response.nextCursor).
            has_more: Value from provider response (response.hasMore). If False,
                run is marked completed with ended_at set.
            items_count: Number of show items in this page (used for counters).
        """
        ended_at: datetime | None = None
        status: str = STATUS_RUNNING

        if has_more is False:
            status = STATUS_COMPLETED
            ended_at = _utcnow_naive()

        stmt = (
            update(SapiRunLedger)
            .where(SapiRunLedger.run_id == run_id)
            .values(
                status=status,
                ended_at=ended_at,
                last_error=None,
                cursor_next=next_cursor,
                pages_processed=SapiRunLedger.pages_processed + 1,
                items_processed=SapiRunLedger.items_processed + int(items_count),
            )
        )
        self._session.execute(stmt)

        logger.info(
            "sapi_ledger_checkpoint run_id=%s pages+=1 items+=%d status=%s has_more=%s",
            run_id,
            int(items_count),
            status,
            has_more,
        )

    # ---------------------------------------------------------------------
    # Failure / completion
    # ---------------------------------------------------------------------

    def mark_failed(
        self, *, run_id: str, error: str, ended_at: datetime | None = None
    ) -> None:
        """
        Mark the run as failed and store a human-readable error summary.
        """
        stmt = (
            update(SapiRunLedger)
            .where(SapiRunLedger.run_id == run_id)
            .values(
                status=STATUS_FAILED,
                ended_at=ended_at or _utcnow_naive(),
                last_error=error,
            )
        )
        self._session.execute(stmt)

        logger.warning("sapi_ledger_failed run_id=%s error=%s", run_id, error)

    def mark_completed(self, *, run_id: str, ended_at: datetime | None = None) -> None:
        """
        Mark the run as completed (independent of checkpointing).
        """
        stmt = (
            update(SapiRunLedger)
            .where(SapiRunLedger.run_id == run_id)
            .values(
                status=STATUS_COMPLETED,
                ended_at=ended_at or _utcnow_naive(),
                last_error=None,
            )
        )
        self._session.execute(stmt)
        logger.info("sapi_ledger_completed run_id=%s", run_id)

    # ---------------------------------------------------------------------
    # Scope queries
    # ---------------------------------------------------------------------

    def latest_completed_run_id(self, scope: SapiRunScope) -> str | None:
        """
        Return the most recent completed run_id for a given scope.

        This supports the "currentness" rule:
            record is current iff last_seen_run_id == latest_completed_run_id(scope)
        """
        stmt: Select = (
            select(SapiRunLedger.run_id)
            .where(SapiRunLedger.country == scope.country)
            .where(SapiRunLedger.catalogs_bundle == scope.catalogs_bundle)
            .where(SapiRunLedger.params_fingerprint == scope.params_fingerprint)
            .where(SapiRunLedger.status == STATUS_COMPLETED)
            .order_by(SapiRunLedger.ended_at.desc(), SapiRunLedger.started_at.desc())
            .limit(1)
        )
        return self._session.execute(stmt).scalar_one_or_none()
