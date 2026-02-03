"""
Append-only raw-page store for SAPI.

Monitoring:
- SAPI_DB_RAW_APPEND_START
- SAPI_DB_RAW_APPEND_SUCCESS
- SAPI_DB_RAW_APPEND_FAILED

Logs include run_id, cursor_used, items_count, has_more, next_cursor, payload_bytes,
hash_ms, db_ms, and dialect.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from src.persistence.tables import (
    SapiRawPage,
)

logger = logging.getLogger(__name__)


def _normalize_cursor_used(cursor_used: str | None) -> str:
    """
    Normalize cursor values to avoid NULL in unique keys.

    Note:
        In PostgreSQL, UNIQUE does not consider NULL equal to NULL, so multiple rows
        could exist for (run_id, NULL). Normalizing the first page cursor to "" avoids that.
    """
    return cursor_used or ""


def _stable_json_bytes(payload: dict[str, Any]) -> bytes:
    """
    Serialize JSON deterministically for hashing and monitoring.

    sort_keys=True makes the representation stable across runs.
    separators minimize size.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _hash_bytes(blob: bytes) -> str:
    """
    SHA-256 hash of a bytes blob.
    """
    return hashlib.sha256(blob).hexdigest()


class SapiRawPagesStore:
    """
    Append-only store for raw SAPI page responses.

    Stores the entire provider response as a JSON blob.
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    def append_page(
        self,
        *,
        run_id: str,
        cursor_used: str | None,
        fetched_at: datetime,
        response_json: dict[str, Any],
    ) -> bool:
        """
        Append a raw page blob.

        Idempotent by (run_id, cursor_used_norm).
        If the page already exists, the insert is ignored.

        Returns:
            True if inserted, False if skipped due to conflict.
        """
        cursor_norm = _normalize_cursor_used(cursor_used)
        dialect = self._session.get_bind().dialect.name

        items_count = len(response_json.get("shows") or [])
        has_more = response_json.get("hasMore")
        next_cursor = response_json.get("nextCursor")

        logger.debug(
            "SAPI_DB_RAW_APPEND_START run_id=%s cursor_used=%s items_count=%d has_more=%s next_cursor=%s dialect=%s",
            run_id,
            cursor_norm,
            items_count,
            has_more,
            next_cursor,
            dialect,
        )

        # Hash + payload size timing (do once, reuse)
        t0 = time.perf_counter()
        payload_bytes = _stable_json_bytes(response_json)
        response_hash = _hash_bytes(payload_bytes)
        hash_ms = (time.perf_counter() - t0) * 1000.0

        row = {
            "run_id": run_id,
            "cursor_used": cursor_norm,
            "fetched_at": fetched_at,
            "has_more": has_more,
            "next_cursor": next_cursor,
            "items_count": items_count,
            "response_json": response_json,
            "response_hash": response_hash,
        }

        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        elif dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert
        else:
            raise NotImplementedError(f"raw append not implemented for {dialect}")

        stmt = (
            insert(SapiRawPage.__table__)
            .values(row)
            .on_conflict_do_nothing(index_elements=["run_id", "cursor_used"])
        )

        t1 = time.perf_counter()
        try:
            result = self._session.execute(stmt)
            db_ms = (time.perf_counter() - t1) * 1000.0

            # rowcount is 1 if inserted, 0 if conflict-do-nothing
            inserted = bool(getattr(result, "rowcount", 0))

            logger.info(
                "SAPI_DB_RAW_APPEND_SUCCESS run_id=%s cursor_used=%s inserted=%s items_count=%d has_more=%s next_cursor=%s "
                "payload_bytes=%d hash_ms=%.2f db_ms=%.2f dialect=%s",
                run_id,
                cursor_norm,
                inserted,
                items_count,
                has_more,
                next_cursor,
                len(payload_bytes),
                hash_ms,
                db_ms,
                dialect,
            )
            return inserted

        except Exception:
            db_ms = (time.perf_counter() - t1) * 1000.0
            logger.exception(
                "SAPI_DB_RAW_APPEND_FAILED run_id=%s cursor_used=%s items_count=%d has_more=%s next_cursor=%s "
                "payload_bytes=%d hash_ms=%.2f db_ms=%.2f dialect=%s",
                run_id,
                cursor_norm,
                items_count,
                has_more,
                next_cursor,
                len(payload_bytes),
                hash_ms,
                db_ms,
                dialect,
            )
            raise
