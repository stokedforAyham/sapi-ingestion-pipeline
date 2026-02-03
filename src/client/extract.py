# Deterministic extraction from a raw SAPI response into index records:
# TitleIndexRecord
# OfferIndexRecord
# AssetIndexRecord
# No persistence. No business logic. Pure mapping.

import datetime
from src.persistence.models import (
    SapiTitleIndexRecord,
    SapiOfferIndexRecord,
    SapiAssetIndexRecord,
)


def _map_title(
    raw_json: dict, fetched_at: datetime, run_id: str
) -> SapiTitleIndexRecord:
    """
    Extracts core identity and metadata into a Title index record.

    Handles SAPI's inconsistent year field naming by falling back from
    'firstAirYear' (series) to 'releaseYear' (movies).
    """
    year = raw_json.get("releaseYear") or raw_json.get("firstAirYear")

    return SapiTitleIndexRecord(
        sapi_id=raw_json["id"],
        imdb_id=raw_json.get("imdbId"),
        tmdb_id=raw_json.get("tmdbId"),
        title=raw_json["title"],
        original_title=raw_json.get("originalTitle"),
        show_type=raw_json["showType"],
        release_year=str(year) if year else None,
        fetched_at=fetched_at,
        last_seen_run_id=run_id,
    )


def _map_offers(
    raw_json: dict, fetched_at: datetime, run_id: str
) -> list[SapiOfferIndexRecord]:
    """
    Flattens country-nested streaming options into individual Offer records.

    Maps nested service and price information while capturing the country
    code from the SAPI dictionary keys into the record's 'country' field.
    """
    records = []
    sapi_id = raw_json["id"]
    streaming_options = raw_json.get("streamingOptions", {})

    for country_code, offers_list in streaming_options.items():
        for offer in offers_list:
            # Reaching into nested 'service' and 'price' (if exists)
            service_info = offer.get("service", {})

            records.append(
                SapiOfferIndexRecord(
                    sapi_id=sapi_id,
                    country=country_code,  # From the dict key (e.g., "de")
                    service_id=service_info.get("id"),
                    service_name=service_info.get("name"),
                    offer_type=offer.get("type"),
                    title_page_link=offer["link"],
                    watch_link=offer.get("videoLink"),
                    quality=offer.get("quality"),
                    audios=offer.get("audios", []),  # Maps to list[Locale]
                    subtitles=offer.get("subtitles", []),  # Maps to list[Subtitle]
                    available_since=offer["availableSince"],
                    expires_soon=offer["expiresSoon"],
                    expires_on=offer.get("expiresOn"),
                    fetched_at=fetched_at,
                    last_seen_run_id=run_id,
                )
            )
    return records


def _map_assets(
    raw_json: dict, fetched_at: datetime, run_id: str
) -> list[SapiAssetIndexRecord]:
    """
    Flattens the SAPI 'imageSet' into a list of Asset records.

    Iterates through image kinds (e.g., verticalPoster) and captures the
    dictionary of resolution-specific URLs.
    """
    records = []
    image_set = raw_json.get("imageSet", {})
    sapi_id = raw_json["id"]

    for kind, urls in image_set.items():
        if not urls:
            continue

        records.append(
            SapiAssetIndexRecord(
                sapi_id=sapi_id,
                asset_kind=kind,  # e.g., "verticalPoster", "horizontalBackdrop"
                image_urls=urls,  # The dict of { "w240": "url...", "w360": "url..." }
                fetched_at=fetched_at,
                last_seen_run_id=run_id,
            )
        )
    return records


def extract_show(
    raw_json: dict, fetched_at: datetime, run_id: str
) -> tuple[
    SapiTitleIndexRecord, list[SapiOfferIndexRecord], list[SapiAssetIndexRecord]
]:
    """
    Primary entry point for transforming a raw SAPI show response into
    normalized index records.

    Args:
        raw_json: A single 'item' dictionary from the SAPI result list.
        fetched_at: Timestamp of when the SAPI response was received.
        run_id: Unique identifier for the current extraction pipeline run.

    Returns:
        A tuple containing (TitleRecord, AssetRecords, OfferRecords).
    """
    return (
        _map_title(raw_json, fetched_at, run_id),
        _map_offers(raw_json, fetched_at, run_id),
        _map_assets(raw_json, fetched_at, run_id),
    )
