"""
SAPI indices store (persistence only).

This module persists already-extracted SAPI index records into the three index tables:
- titles_index: one row per sapi_id
- offers_index: one row per (sapi_id, country, service_id, offer_type)
- assets_index: one row per (sapi_id, asset_kind)

Design constraints (per Data Layer DR):
- No deletes mid-run.
- Upsert by stable idempotency keys.
- Update last_seen_run_id on every touched row.

Non-responsibilities:
- No HTTP calls.
- No raw JSON extraction. That lives in extract.py.
- No ledger logic. That belongs in the run ledger module.

Monitoring:
- Emits structured logs for DB upserts:
  - SAPI_DB_UPSERT_START
  - SAPI_DB_UPSERT_SUCCESS
  - SAPI_DB_UPSERT_FAILED
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Sequence, Type

from sqlalchemy.orm import Session

from src.persistence.tables import (
    AssetKind as OrmAssetKind,
    Base,
    SapiAssetIndex,
    SapiOfferIndex,
    SapiShowTypeEnum,
    SapiTitleIndex,
)
from src.persistence.models import (
    SapiAssetIndexRecord,
    SapiOfferIndexRecord,
    SapiTitleIndexRecord,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UpsertCounts:
    """
    Counts of rows attempted to be upserted.

    Note:
        These are the number of input rows processed, not DB-reported affected rows.
        (DB affected-row reporting varies by dialect and driver.)
    """

    titles: int
    offers: int
    assets: int


class SapiIndicesStore:
    """
    Repository for persisting extracted SAPI index records.

    Intended use:
        with Session.begin() as session:
            store = SapiIndicesStore(session)
            counts = store.upsert_all(title, offers, assets)
    """

    def __init__(self, session: Session, *, chunk_size: int = 1000) -> None:
        """
        Args:
            session: SQLAlchemy Session bound to the source-store database.
            chunk_size: Max rows per upsert statement to avoid parameter limits.
        """
        self._session = session
        self._chunk_size = chunk_size

    def upsert_all(
        self,
        title: SapiTitleIndexRecord,
        offers: Sequence[SapiOfferIndexRecord],
        assets: Sequence[SapiAssetIndexRecord],
    ) -> UpsertCounts:
        """
        Upsert a title plus its offers and assets.

        Titles are upserted first to satisfy FK constraints.
        """
        titles_n = self.upsert_titles([title])
        offers_n = self.upsert_offers(list(offers))
        assets_n = self.upsert_assets(list(assets))
        return UpsertCounts(titles=titles_n, offers=offers_n, assets=assets_n)

    def upsert_titles(self, records: Sequence[SapiTitleIndexRecord]) -> int:
        """
        Upsert title index records by (sapi_id).
        """
        rows = [self._row_title(r) for r in records]
        return self._bulk_upsert(
            model=SapiTitleIndex,
            rows=rows,
            conflict_cols=("sapi_id",),
        )

    def upsert_offers(self, records: Sequence[SapiOfferIndexRecord]) -> int:
        """
        Upsert offer index records by (sapi_id, country, service_id, offer_type).
        """
        rows = [self._row_offer(r) for r in records]

        def _quality_rank(q: str | None) -> int:
            return {"uhd": 3, "hd": 2, "sd": 1}.get((q or "").lower(), 0)

        def _dedupe_offers(records: list[SapiOfferIndexRecord]) -> list[SapiOfferIndexRecord]:
            best: dict[tuple[str, str, str, str], SapiOfferIndexRecord] = {}

            for r in records:
                key = (r.sapi_id, r.country, r.service_id, r.offer_type)
                cur = best.get(key)
                if cur is None:
                    best[key] = r
                    continue

                # prefer higher quality
                if _quality_rank(r.quality) > _quality_rank(cur.quality):
                    best[key] = r
                    continue

                # if quality tie, prefer one with watch_link
                if (r.watch_link is not None) and (cur.watch_link is None):
                    best[key] = r
                    continue

                # if still tie, prefer later available_since (if present)
                if (r.available_since or 0) > (cur.available_since or 0):
                    best[key] = r

            return list(best.values())

        records = _dedupe_offers(records)
        rows = [self._row_offer(r) for r in records]

        return self._bulk_upsert(
            model=SapiOfferIndex,
            rows=rows,
            conflict_cols=("sapi_id", "country", "service_id", "offer_type"),
        )

    def upsert_assets(self, records: Sequence[SapiAssetIndexRecord]) -> int:
        """
        Upsert asset index records by (sapi_id, asset_kind).
        """
        rows = [self._row_asset(r) for r in records]
        return self._bulk_upsert(
            model=SapiAssetIndex,
            rows=rows,
            conflict_cols=("sapi_id", "asset_kind"),
        )

    # -------------------------------------------------------------------------
    # Normalization: Pydantic record -> ORM insert/update row
    # -------------------------------------------------------------------------

    @staticmethod
    def _row_title(r: SapiTitleIndexRecord) -> dict[str, Any]:
        """
        Convert a title index record into a DB row dict.

        Uses model_dump() to keep native Python objects (notably datetime).
        Converts show_type string literal into ORM enum type.
        """
        d = r.model_dump()
        d["show_type"] = SapiShowTypeEnum(d["show_type"])
        return d

    @staticmethod
    def _row_offer(r: SapiOfferIndexRecord) -> dict[str, Any]:
        """
        Convert an offer index record into a DB row dict.

        Uses model_dump() to keep native Python objects (notably datetime).
        Converts country string literal into ORM enum type.
        """
        d = r.model_dump()
        return d

    @staticmethod
    def _row_asset(r: SapiAssetIndexRecord) -> dict[str, Any]:
        """
        Convert an asset index record into a DB row dict.

        Uses model_dump() to keep native Python objects (notably datetime).

        Note:
            image_urls values are AnyUrl in Pydantic, but the DB JSON column is
            dict[str, str], so URL values are coerced to strings.
        """
        d = r.model_dump()

        ak = d["asset_kind"]
        if isinstance(ak, Enum):
            ak = ak.value
        d["asset_kind"] = OrmAssetKind(ak)

        # AnyUrl -> str for JSON persistence
        d["image_urls"] = {k: str(v) for k, v in (d.get("image_urls") or {}).items()}
        return d

    # -------------------------------------------------------------------------
    # Upsert implementation (with monitoring logs)
    # -------------------------------------------------------------------------

    def _insert_fn(self):
        """
        Return a dialect-specific insert() that supports on_conflict_do_update().
        """
        dialect = self._session.get_bind().dialect.name
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as ins

            return ins
        if dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as ins

            return ins
        raise NotImplementedError(
            f"Upsert is only implemented for PostgreSQL and SQLite. dialect={dialect}"
        )

    def _bulk_upsert(
        self,
        *,
        model: Type[Base],
        rows: list[dict[str, Any]],
        conflict_cols: tuple[str, ...],
    ) -> int:
        """
        Bulk upsert rows into the table mapped by `model`.

        Monitoring:
            Emits structured logs with table name, row count, chunk count, dialect,
            and total DB latency.
        """
        if not rows:
            return 0

        insert = self._insert_fn()
        table = model.__table__
        table_name = getattr(model, "__tablename__", table.name)

        dialect = self._session.get_bind().dialect.name
        run_id = (
            rows[0].get("last_seen_run_id")
            if rows and isinstance(rows[0], dict)
            else None
        )

        chunks = list(_chunks(rows, self._chunk_size))
        chunk_count = len(chunks)

        logger.debug(
            "SAPI_DB_UPSERT_START table=%s rows=%d chunks=%d chunk_size=%d dialect=%s run_id=%s",
            table_name,
            len(rows),
            chunk_count,
            self._chunk_size,
            dialect,
            run_id,
        )

        start = time.perf_counter()
        try:
            total = 0
            for chunk in chunks:
                stmt = insert(table).values(chunk)

                excluded = stmt.excluded
                conflict_elements = [table.c[c] for c in conflict_cols]

                # Update all non-conflict columns to the incoming values.
                set_clause = {
                    c.name: getattr(excluded, c.name)
                    for c in table.c
                    if c.name not in conflict_cols
                }

                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_elements,
                    set_=set_clause,
                )

                self._session.execute(stmt)
                total += len(chunk)

            latency_ms = (time.perf_counter() - start) * 1000.0
            logger.info(
                "SAPI_DB_UPSERT_SUCCESS table=%s rows=%d chunks=%d latency_ms=%.2f dialect=%s run_id=%s",
                table_name,
                total,
                chunk_count,
                latency_ms,
                dialect,
                run_id,
            )
            return total

        except Exception:
            latency_ms = (time.perf_counter() - start) * 1000.0
            logger.exception(
                "SAPI_DB_UPSERT_FAILED table=%s rows=%d chunks=%d latency_ms=%.2f dialect=%s run_id=%s conflict_cols=%s",
                table_name,
                len(rows),
                chunk_count,
                latency_ms,
                dialect,
                run_id,
                conflict_cols,
            )
            raise


def _chunks(items: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    """
    Yield list chunks of at most `size` items.
    """
    for i in range(0, len(items), size):
        yield items[i : i + size]
