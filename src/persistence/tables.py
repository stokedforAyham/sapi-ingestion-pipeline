"""
SQLAlchemy ORM models for Streaming Availability Provider (SAPI) indexing.

This module provides SQLAlchemy 2.0 typed declarative models that mirror the
corresponding Pydantic models in `models.py`.

Storage notes:
- `audios` and `subtitles` are stored as JSON arrays of objects.
- `image_urls` is stored as a JSON object mapping size or variant keys to URL strings.
- Upsert keys are represented as primary keys (single or composite).
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from sqlalchemy import (
    UniqueConstraint,
    Boolean,
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """
    Declarative base class for all ORM models in this module.

    Subclassing `DeclarativeBase` enables SQLAlchemy 2.0 typed mappings via
    `Mapped[...]` and `mapped_column(...)`.
    """


class SapiShowTypeEnum(str, Enum):
    """
    High-level content type returned by the provider.

    Typically distinguishes between movies and series.
    """

    MOVIE = "movie"
    SERIES = "series"


class AssetKind(str, Enum):
    """
    Asset categories supported by the indexing layer.

    Used to partition image URLs by layout and usage context.
    """

    VERTICAL_POSTER = "verticalPoster"
    VERTICAL_BACKDROP = "verticalBackdrop"
    HORIZONTAL_POSTER = "horizontalPoster"
    HORIZONTAL_BACKDROP = "horizontalBackdrop"


class SapiRawPage(Base):
    """
    Append-only storage for raw SAPI page responses.

    Idempotency:
        One raw page per (run_id, cursor_used_norm).
        cursor_used_norm is "" for the first page (no cursor param sent).
    """

    __tablename__ = "sapi_raw_pages"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    run_id: Mapped[str] = mapped_column(String, nullable=False)
    cursor_used: Mapped[str] = mapped_column(
        String, nullable=False
    )  # normalized, never NULL

    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    has_more: Mapped[bool | None] = mapped_column(nullable=True)
    next_cursor: Mapped[str | None] = mapped_column(String, nullable=True)
    items_count: Mapped[int | None] = mapped_column(nullable=True)

    response_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    response_hash: Mapped[str | None] = mapped_column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint("run_id", "cursor_used", name="uq_sapi_raw_pages_run_cursor"),
        Index("ix_sapi_raw_pages_run_fetched_at", "run_id", "fetched_at"),
    )


class SapiTitleIndex(Base):
    """
    Title-level index record.

    Purpose:
    - Stores the stable provider identifier (`sapi_id`) and basic title metadata.
    - Holds external IDs when present (IMDb, TMDB).
    - Tracks ingestion recency (`fetched_at`) and existence in the latest run
      (`last_seen_run_id`) to support pruning and drift detection.

    Primary key:
    - `sapi_id`
    """

    __tablename__ = "sapi_titles_index"

    sapi_id: Mapped[str] = mapped_column(String, primary_key=True)

    imdb_id: Mapped[str | None] = mapped_column(String, nullable=True)
    tmdb_id: Mapped[str | None] = mapped_column(String, nullable=True)

    title: Mapped[str] = mapped_column(String, nullable=False)
    original_title: Mapped[str | None] = mapped_column(String, nullable=True)

    show_type: Mapped[SapiShowTypeEnum] = mapped_column(
        SAEnum(SapiShowTypeEnum, name="sapi_show_type"),
        nullable=False,
    )

    item_type: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        server_default="show",
    )

    release_year: Mapped[str | None] = mapped_column(String, nullable=True)

    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_seen_run_id: Mapped[str] = mapped_column(String, nullable=False)

    __table_args__ = (
        Index("ix_sapi_titles_last_seen_run_id", "last_seen_run_id"),
        Index("ix_sapi_titles_fetched_at", "fetched_at"),
        Index("ix_sapi_titles_imdb_id", "imdb_id"),
        Index("ix_sapi_titles_tmdb_id", "tmdb_id"),
    )


class SapiOfferIndex(Base):
    """
    Offer-level index record.

    Purpose:
    - Stores availability offers for a title scoped by country and service.
    - Captures the offer type plus watch links, quality, and language tracks.
    - Tracks ingestion recency (`fetched_at`) and last observation (`last_seen_run_id`).

    Composite primary key (upsert key):
    - (`sapi_id`, `country`, `service_id`, `offer_type`)

    Data encoding:
    - `audios`: JSON array of locale objects.
    - `subtitles`: JSON array of subtitle objects.
    - `available_since` and `expires_on`: epoch seconds as integers.
    """

    __tablename__ = "sapi_offers_index"

    sapi_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("sapi_titles_index.sapi_id", ondelete="CASCADE"),
        primary_key=True,
    )
    country: Mapped[str] = mapped_column(
        String,
        primary_key=True,
    )
    service_id: Mapped[str] = mapped_column(String, primary_key=True)
    offer_type: Mapped[str] = mapped_column(String, primary_key=True)

    service_name: Mapped[str | None] = mapped_column(String, nullable=True)

    title_page_link: Mapped[str] = mapped_column(String, nullable=False)
    watch_link: Mapped[str | None] = mapped_column(String, nullable=True)

    quality: Mapped[str | None] = mapped_column(String, nullable=True)

    audios: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON,
        nullable=False,
        default=list,
    )
    subtitles: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON,
        nullable=False,
        default=list,
    )

    available_since: Mapped[int] = mapped_column(Integer, nullable=False)
    expires_soon: Mapped[bool] = mapped_column(Boolean, nullable=False)
    expires_on: Mapped[int | None] = mapped_column(Integer, nullable=True)

    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_seen_run_id: Mapped[str] = mapped_column(String, nullable=False)

    __table_args__ = (
        Index("ix_sapi_offers_last_seen_run_id", "last_seen_run_id"),
        Index("ix_sapi_offers_fetched_at", "fetched_at"),
        Index("ix_sapi_offers_country_service", "country", "service_id"),
    )


class SapiAssetIndex(Base):
    """
    Asset-level index record.

    Purpose:
    - Stores image URLs for a title grouped by asset kind (poster/backdrop variants).
    - Tracks ingestion recency (`fetched_at`) and last observation (`last_seen_run_id`).

    Composite primary key (upsert key):
    - (`sapi_id`, `asset_kind`)

    Data encoding:
    - `image_urls`: JSON object mapping variants or sizes to URL strings.
    """

    __tablename__ = "sapi_assets_index"

    sapi_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("sapi_titles_index.sapi_id", ondelete="CASCADE"),
        primary_key=True,
    )
    asset_kind: Mapped[AssetKind] = mapped_column(
        SAEnum(AssetKind, name="sapi_asset_kind"),
        primary_key=True,
    )

    image_urls: Mapped[dict[str, str]] = mapped_column(
        JSON,
        nullable=False,
        default=dict,
    )

    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_seen_run_id: Mapped[str] = mapped_column(String, nullable=False)

    __table_args__ = (
        Index("ix_sapi_assets_last_seen_run_id", "last_seen_run_id"),
        Index("ix_sapi_assets_fetched_at", "fetched_at"),
    )


class SapiRunLedger(Base):
    """
    Minimal run ledger for SAPI ingestion (v0).

    Purpose
    - Persist a single evolving row per ingestion run.
    - Provide a resumable checkpoint via `cursor_next`.
    - Track basic run lifecycle and coarse progress counters.

    Core invariant (v0)
    - `cursor_next` is advanced only after raw page persistence and index upserts
      have succeeded, within the same database transaction in the worker loop.

    Scope identity
    - `country`, `catalogs_bundle`, and `params_fingerprint` identify the logical
      ingest scope so you can later query the latest completed run for a scope.

    Notes
    - This v0 ledger intentionally avoids concurrency locking, retry metrics,
      and enum-hardening. Those can be added after v0 correctness is proven.
    """

    __tablename__ = "sapi_run_ledger"

    run_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        doc="Unique run identifier (primary key).",
    )

    country: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="ISO 3166-1 alpha-2 country code for this run scope (e.g., 'de').",
    )
    catalogs_bundle: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Canonical, sorted comma-separated list of catalogs for this run scope.",
    )
    params_fingerprint: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Stable hash of request parameters that define membership/order for this run.",
    )

    status: Mapped[str] = mapped_column(
        String,
        nullable=False,
        default="started",
        doc="Run status string: started | running | completed | failed.",
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        doc="UTC timestamp when the run row was created.",
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
        doc="UTC timestamp when the run completed or failed (NULL while active).",
    )
    last_error: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="Last error message captured for the run (NULL if none).",
    )

    cursor_next: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="Cursor to request next; NULL means start from the first page.",
    )
    pages_processed: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        doc="Number of pages successfully persisted (raw + indices) for this run.",
    )
    items_processed: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        doc="Number of show items processed across persisted pages for this run.",
    )

    __table_args__ = (
        Index(
            "ix_sapi_run_ledger_scope_status_ended",
            "country",
            "catalogs_bundle",
            "params_fingerprint",
            "status",
            "ended_at",
        ),
        Index("ix_sapi_run_ledger_status_started", "status", "started_at"),
    )
