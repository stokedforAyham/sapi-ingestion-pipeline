# SAPI Ingestion Pipeline

**An artifact of a larger data platform.** This is a production-grade ingestion system for the Streaming Availability API (SAPI), demonstrating thoughtful engineering decisions around data reliability, resumability, and schema design.

---

## What is SAPI?

The **Streaming Availability API** (via RapidAPI) provides **near-real-time data about movie and TV show availability across streaming services** in different countries. It answers questions like:

- *What services stream this title in Germany?*
- *What's the watch link for Netflix in the US?*
- *Is this title available with subtitles or audio in Spanish?*
- *When will this title expire from Prime Video?*

SAPI returns structured data for titles (movies, series) including:
- **Title metadata:** ID, title, IMDb ID, TMDB ID, release year, show type
- **Streaming offers:** Service (Netflix, Prime, Disney+, etc.), country, offer type (subscription, rent, buy, free), quality, expiry date
- **Media assets:** Posters and backdrops at multiple resolutions (w240, w360, w720, w1080, w1440, etc.)
- **Localization:** Audio languages and subtitle locales for each offer

**Catalog churn is constant:** Services add/remove titles daily, expiry dates shift, new services appear. This pipeline is built to handle that volatility safely.

---

## What This Does

Fetches paginated SAPI data by scope (country + catalog bundle + optional genre/filters), persists raw responses durably, and extracts queryable indices—all while surviving partial runs and catalog churn.

**The core invariant:** Cursor position advances only after raw data + indices are written. This means runs are resumable without data loss or duplication.

**Data flow:**
```
SAPI API (paginated responses)
    ↓
[Raw store] — append-only JSON blobs (audit trail)
    ↓
[Extraction] — deterministic mapping to structured records
    ↓
[Indices] — 3 upsertable tables:
  • Title records (movies & series metadata)
  • Offer records (streaming availability per country)
  • Asset records (image URLs at multiple resolutions)
    ↓
[Ledger] — cursor checkpoint for resumable runs
```

---

## Data Schema & Extraction

### Raw Store (Append-Only Archive)
```
table: sapi_raw_pages
  run_id          (foreign key to ledger)
  page_number     (sequence within run)
  response_blob   (raw JSON from SAPI, unchanged)
  fetched_at      (timestamp)
```

Every API response is stored exactly as returned. This enables:
- Debugging and auditing ("What did SAPI say on 2025-02-03?")
- Replaying extraction logic if business rules change
- Compliance and data lineage tracking

### Extracted Indices (Upsertable Tables)

#### `sapi_titles`
Canonical title records (movies & series). Upsert key: `(sapi_id)`.

```
sapi_id              (SAPI's unique identifier)
imdb_id, tmdb_id     (cross-source anchors for canonicalization)
title, original_title
show_type            (movie | series)
release_year
fetched_at           (when this record was last refreshed)
last_seen_run_id     (run ID that last updated this title — "currentness")
```

#### `sapi_offers`
Streaming availability per title, country, and service. Upsert key: `(sapi_id, country, service_id, offer_type)`.

```
sapi_id              (title reference)
country              (e.g., "US", "DE", "GB")
service_id           (e.g., "nflx", "amz", "disneyplus")
service_name         (display name from SAPI)
offer_type           (subscription | rent | buy | free | addon)
quality              (sd | hd | qhd | uhd)
title_page_link      (link to title page on service)
watch_link           (deep link to watch directly)
audios               (list of available language locales)
subtitles            (list of subtitle locales + closed captions flag)
available_since      (epoch seconds: when offer became available)
expires_soon         (boolean flag: provider says expiry is imminent)
expires_on           (epoch seconds: exact expiry, if known)
fetched_at           (when this offer data was last seen)
last_seen_run_id     (run ID that most recently saw this offer — detect stale data)
```

#### `sapi_assets`
Poster & backdrop images at multiple resolutions. Upsert key: `(sapi_id, asset_kind)`.

```
sapi_id              (title reference)
asset_kind           (verticalPoster | verticalBackdrop | horizontalPoster | horizontalBackdrop)
image_urls           (dict: width → URL, e.g., {"w240": "...", "w720": "...", "w1080": "..."})
fetched_at
last_seen_run_id
```

### Why This Schema?

**Append-only raw + upsert indices decouples concerns:**
- Raw is a durable, immutable journal → audit trail + replay capability
- Indices are normalized, queryable tables → analytics and downstream services
- `last_seen_run_id` tracks recency without deletes → safe handling of catalog churn

**Example:** If Netflix removes a title tomorrow:
- The offer row won't appear in the next crawl
- You compare `last_seen_run_id` to today's run ID to detect removal
- No data loss; you can still query what was available as of yesterday

---

## Engineering Highlights

### 1. **Source-Isolated Pipeline**  
Adapter pattern separates SAPI client concerns (auth, retries, telemetry) from business logic. Enables clean multi-provider scaling without coupling.

```
[SapiClient] → [Extractor] → [Raw Store] + [Indices Store]
              ↓
         [Ledger Checkpoint]
```

### 2. **Append-Only Raw + Upsert Indices**  
Raw responses stored immutably for auditability; extracted indices (titles, offers) upserted by stable keys (`sapi_id`, country, service).

- **Why:** Separates "what the API said" from "what we understand."
- **Why:** Supports safe reprocessing and schema evolution.

### 3. **Resumable Cursor Checkpointing**  
Ledger tracks `cursor_next` per run. If the process crashes mid-page:
- Restart with the same `run_id` → resumes from checkpoint.
- No duplicate writes (upserts are idempotent).
- Partial runs don't advance the cursor.

```python
# Hard invariant in worker.py
with session.begin():
    ledger.persist_page_and_advance_cursor(
        run_id=run_id,
        raw_page=response,
        indices=extracted,
        cursor_next=next_cursor,
    )
# cursor_next updates only after raw + indices commit successfully
```

### 4. **Soft Deletes via `last_seen_run_id`**  
Instead of purging records, track recency. Handles catalog churn without losing history or requiring full replays.

---

## Project Structure

```
src/
├── client/
│   ├── client.py         # SapiClient: HTTP adapter with retry policy
│   └── extract.py        # Business logic to map raw SAPI → index records
├── config/
│   ├── config.py         # Settings loader
│   ├── postgres_settings.py
│   └── sapi_settings.py
├── persistence/
│   ├── engine.py         # DatabaseManager: session context manager
│   ├── models.py         # Pydantic schemas for index records
│   ├── tables.py         # SQLAlchemy table definitions
│   └── stores/
│       ├── raw.py        # SapiRawPagesStore: append-only raw JSON
│       └── indices.py    # SapiIndicesStore: upsert operations
└── pipeline/
    ├── ledger.py         # SapiRunLedgerStore: cursor checkpointing
    └── worker.py         # SapiBackfillWorker: orchestration logic
```

---

## How to Use

### Setup

```bash
# Install in editable mode (or use the venv)
python -m pip install -e .

# Set environment variables for SAPI credentials + Postgres
export SAPI_API_KEY=...
export SAPI_API_HOST=...
export DATABASE_URL=postgresql://...
```

### Running a Backfill

The worker is a library, not a CLI. You orchestrate it from your own code or scheduler:

```python
from src.persistence.engine import DatabaseManager
from src.client.client import SapiClient
from src.pipeline.worker import SapiBackfillWorker, SapiRunScope, BackfillOptions
from src.config import SAPI_SETTINGS, POSTGRES_SETTINGS

# Initialize
db_manager = DatabaseManager(POSTGRES_SETTINGS)
client = SapiClient(
    api_key=SAPI_SETTINGS.api_key,
    api_host=SAPI_SETTINGS.api_host,
    base_url=SAPI_SETTINGS.base_url,
)
worker = SapiBackfillWorker(db_manager=db_manager, sapi_client=client)

# Run a backfill for US drama titles available on streaming services
scope = SapiRunScope(
    country="US",
    catalogs_bundle="netflix,prime,disney",
    params_fingerprint="v1:genre=18",  # genre 18 = drama
)
options = BackfillOptions(max_pages=100)

run_id = worker.run_backfill(
    scope=scope,
    base_query_params={"genre": "18"},
    options=options,
)
print(f"Backfill {run_id} started")

# Later: resume the same run (picks up from checkpoint)
worker.run_backfill(scope=scope, base_query_params={"genre": "18"}, run_id=run_id)
```

After the backfill completes, you can query the indices:

```sql
-- Find all Breaking Bad offers in the US
SELECT 
  t.title, 
  o.service_id, 
  o.offer_type, 
  o.watch_link,
  o.quality,
  o.expires_on
FROM sapi_titles t
JOIN sapi_offers o ON t.sapi_id = o.sapi_id
WHERE t.title = 'Breaking Bad'
  AND o.country = 'US'
  AND o.last_seen_run_id = 'run-abc-123'  -- latest run
ORDER BY o.service_id;

-- Find all available assets (posters) for a title
SELECT 
  asset_kind,
  image_urls
FROM sapi_assets
WHERE sapi_id = 'tt0903747'
  AND last_seen_run_id = 'run-abc-123';
```

### Running Tests

```bash
python -m pytest tests/
```

---

## Key Design Decisions

### Why No HTTP During Transactions?

Keeping DB transactions open during API calls risks deadlocks and blocks other writers. Instead:

1. Fetch page from API (no DB connection).
2. Parse + extract indices in memory.
3. Enter transaction: write raw + indices + advance cursor (all in one atomic operation).

### Why Ledger Checkpointing?

- **Resumability:** Operators can safely interrupt and restart.
- **Partial crawls:** Run with `max_pages=100` to chunk large catalogs; ledger persists your stopping point.
- **Observability:** Log entry tracks run state without polling the DB.

### Why Upserts for Indices?

SAPI data changes frequently (new services, expiry dates, catalog churn). Upserts let you:
- Reprocess the same pages without manual cleanup.
- Handle schema evolution by re-extracting.
- Detect stale records via `last_seen_run_id`.

---

## Testing Strategy

Tests live in `tests/test_client.py` and use `responses` to mock HTTP:

```python
import responses
from src.client.client import SapiClient

@responses.activate
def test_fetch_data_success():
    responses.add(
        responses.GET,
        "https://api.example.com/shows",
        json={"items": [{"id": "1"}], "next_cursor": "abc"},
        status=200,
    )
    client = SapiClient(api_key="test", api_host="test", base_url="https://api.example.com")
    result = client.fetch_data("shows", {})
    assert result["next_cursor"] == "abc"
```

---

## What's Not Included

- **Scheduler:** Bring your own Airflow, Temporal, or cron.
- **Deletion logic:** This is append-only. If you need to purge old data, write a separate cleanup job.
- **Multi-provider:** The design supports it (see source isolation), but only SAPI is implemented.
- **Real-time streaming:** This is batch/scheduled crawling.

---

## Future Directions

- Implement a schema registry for index record evolution.
- Add observability hooks (metrics, structured logging).
- Build a query service layer on top of indices.

---
