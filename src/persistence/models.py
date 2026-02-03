"""
SAPI source-store persistence schemas (extracted indices).

These models represent the extracted/normalized records derived from SAPI raw responses.

Design notes:
- Raw SAPI responses are stored append-only elsewhere.
- Meta (overview, genres, cast, etc.) stays in raw for v0.
- Indices are upserted by stable keys and carry `last_seen_run_id` for "currentness".
- Store *all* services returned in `streamingOptions`, not just the crawl-scope services.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, ConfigDict, AnyUrl


SapiCountry = str
SapiShowType = Literal["movie", "series"]


class SapiTitleIndexRecord(BaseModel):
    """
    Upsert key: sapi_id
    """

    model_config = ConfigDict(extra="forbid",
                              populate_by_name=True)

    sapi_id: str = Field(alias="id")

    # Cross-source anchors (optional, not guaranteed to exist)
    imdb_id: str | None = Field(default=None, alias="imdbId")
    tmdb_id: str | None = Field(default=None, alias="tmdbId")

    # Identity-ish fields (still sourced from SAPI; canonicalization comes later)
    title: str
    original_title: str | None = Field(default=None, alias="originalTitle")
    show_type: SapiShowType = Field(alias="showType")

    # SAPI often uses "show" as a generic item label; keep flexible.
    item_type: str | None = Field(default="show", alias="itemType")

    release_year: str | None = Field(default=None, alias="releaseYear")

    fetched_at: datetime
    last_seen_run_id: str


class Locale(BaseModel):
    """
    SAPI locales are typically language + optional region.
    Keep validation permissive to avoid ingestion brittleness.
    """
    model_config = ConfigDict(extra="forbid")

    language: str = Field(..., min_length=2, max_length=3)
    region: str | None = Field(None, min_length=2, max_length=3)


class Subtitle(BaseModel):
    model_config = ConfigDict(extra="forbid",
                              populate_by_name=True)

    closed_captions: bool = Field(alias="closedCaptions")
    locale: Locale


class SapiOfferIndexRecord(BaseModel):
    """
    Upsert key: (sapi_id, country, service_id, offer_type)

    Notes:
    - service_id is NOT restricted to Netflix/Prime/Disney; store all returned services.
    - available_since/expires_on are ints per SAPI docs (epoch seconds).
    """

    model_config = ConfigDict(extra="forbid",
                              populate_by_name=True)

    sapi_id: str = Field(alias="id")
    country: SapiCountry

    # External enumeration: keep open (string) to avoid breaking when SAPI adds services.
    service_id: str
    service_name: str | None = Field(
        default=None, alias="name"
    )  # optional display value if provided

    # External enumeration: keep open to avoid brittle ingestion.
    offer_type: str = Field(alias="type")  # expected: free/subscription/rent/buy/addon

    # Deep links (naming reflects intended meaning, regardless of SAPI field name)
    title_page_link: str = Field(alias="link")
    watch_link: str | None = Field(default=None, alias="videoLink")

    # External enumeration; keep open.
    quality: str | None = None  # expected: sd/hd/qhd/uhd

    audios: list[Locale] = Field(default_factory=list)
    subtitles: list[Subtitle] = Field(default_factory=list)

    available_since: int = Field(alias="availableSince")
    expires_soon: bool = Field(alias="expiresSoon")
    expires_on: int | None = Field(default=None, alias="expiresOn")

    fetched_at: datetime
    last_seen_run_id: str


class AssetKind(str, Enum):
    """
    Asset kind keys should match the extracted SAPI image set keys we upsert by.
    """

    VERTICAL_POSTER = "verticalPoster"
    VERTICAL_BACKDROP = "verticalBackdrop"
    HORIZONTAL_POSTER = "horizontalPoster"
    HORIZONTAL_BACKDROP = "horizontalBackdrop"


class SapiAssetIndexRecord(BaseModel):
    """
    Upsert key: (sapi_id, asset_kind)

    Store image pointers only (no downloading here). Keep the width map open-ended.
    """

    model_config = ConfigDict(extra="forbid",
                              populate_by_name=True)

    sapi_id: str = Field(alias="id")
    asset_kind: AssetKind

    # Example keys: w240, w360, w480, w720, w1080, w1440, ...
    image_urls: dict[str, AnyUrl] = Field(default_factory=dict)

    fetched_at: datetime
    last_seen_run_id: str
