# Order Shipping Status — Overview & Replay Guide

This document summarizes what the system does today, how replay works (with or without an input file), and how the pipeline is designed and implemented — updated to reflect the latest working code and tests.


## What the System Does



## Supported Modes

### Normal mode (with input file)
  - `Tracking Number`
  - `Carrier Code` (e.g., `FDX`)
  ```markdown
  # Order Shipping Status — Overview & Replay Guide

  This document summarizes the system behaviour, how to run it (development and CI-friendly replay), and the important implementation details you should know when contributing.


  ## What the system does

  Given an input workbook the pipeline enriches each row with carrier-derived status, computes a small set of indicators (Pre-Transit / Delivered / Exception / RTS / Stalled), derives a human-friendly calculated status and reasons, and writes a processed workbook with stable columns for downstream consumption.


  ## Supported modes

  ### Normal mode (with input file)
  - Input workbook rows are read (first sheet). Typical columns: `Tracking Number`, `Carrier Code`, optionally `Promised Delivery Date`, `Delivery Tracking Status`, etc.

  ### Replay mode (deterministic, recommended for CI)
  - The CLI accepts a `--replay-dir` containing one JSON file per tracking number named `<TrackingNumber>.json`. The `ReplayClient` will load those files to provide carrier payloads to the normalizer.
  - Note: the CLI can also write a single combined API bodies file via `--dump-api-bodies` (see below). That combined dump is a JSON array written by `FedExWriter` but is not directly consumed by `ReplayClient` — if you want to replay from a combined dump you must split it into per-TN files or adapt the replay client.


  ## High-level pipeline

  ```
  Input Workbook (xlsx)
    │
    ▼
  Preprocessor
    - Drops the leading extraneous column (common in Excel exports)
    - Optionally filters rows to the prior week (Sunday..Saturday) relative to `--reference-date`
    - Optionally excludes delivered rows
    │
    ▼
  ColumnContract
    - Ensures the output contains the expected suffix of columns (status fields, indicators, metrics)
    │
    ▼
  Enricher
    - For each row: fetch payload (replay directory or live API) and call the normalizer
    - Merge normalized columns and backfill core fields when possible
    - Optionally write per-TN debug sidecars
    │
    ▼
  Metrics
    - Compute `LatestEventTimestampUtc` and `DaysSinceLatestEvent`
    │
    ▼
  Rules & Classifier
    - Produce indicator columns and compose `CalculatedStatus`/`CalculatedReasons`
    │
    ▼
  Processed Workbook (xlsx)
  ```


  ## Preprocessor details

  - The Preprocessor drops the first column of the input (this removes common placeholder/index columns from exported Excel files).
  - By default the preprocessor applies a prior-week filter: when a `--reference-date YYYY-MM-DD` is provided the pipeline keeps only rows whose `Promised Delivery Date` lies in the prior calendar week (Sunday..Saturday) relative to `reference_date`.
  - Use `--skip-date-filter` to disable this behaviour (useful for replay runs and full reprocessing).


  ## Normalization (FedEx)

  Function: `normalize_fedex(payload, *, tracking_number, carrier_code, source)` — returns a model or mapping with the core fields the rules expect.

  - The normalizer extracts the most relevant fields from either a minimal flat payload or a full FedEx response tree (e.g. `output.completeTrackResults[*].trackResults[*]`).
  - Key outputs: `code`, `derivedCode`, `statusByLocale`, `description` and any timestamps/scan events used for metrics.
  - Timestamps are normalized to UTC ISO-8601 where possible.
  - The normalizer has best-effort fallbacks so tests can feed either shallow or deep payloads.


  ## Enrichment and sidecars

  - `ReplayClient(replay_dir)` expects per-TN JSON files named exactly `<trackingNumber>.json`.
  - When live API mode is used (`--use-api`) and `--dump-api-bodies PATH` is provided, the CLI will persist raw API responses into a single JSON array file at `PATH` (the `FedExWriter` writes a JSON array and exposes `read_all()` to read that array).
  - For diagnostics you can enable `--debug-sidecar PATH` which writes per-row normalized JSON sidecars named `<Carrier>_<TrackingNumber>.json` into `PATH`.


  ## Metrics

  - `LatestEventTimestampUtc` is populated from the normalized payload (or backfilled from scan events) as an ISO-8601 UTC string.
  - `DaysSinceLatestEvent` is calculated as integer days relative to an internal `now` (which can be injected via `WorkbookProcessor(reference_now=...)` for deterministic tests).


  ## Indicators & classification

  Indicators are integer (0/1) columns added to the output. Current code produces: `IsPreTransit`, `IsDelivered`, `HasException`, `IsRTS`, `IsStalled`.

  Classifier / precedence notes (current behavior):

  - Terminal states: `IsDelivered == 1` or `IsRTS == 1` are treated as terminal for some rules.
  - Precedence for `CalculatedStatus` (applied in the rules mapper):
    1. If `IsRTS` then `Returned to Sender`
    2. Else if `IsDelivered` then `Delivered`
    3. Else if `HasException` then `Exception`
    4. Else if `IsPreTransit` then `Pre-Transit`
    5. Otherwise empty string

  - `CalculatedReasons` is the concatenation of active indicator labels in a fixed order (used by tests). Indicators are independent (0/1 integers) with documented precedence for status composition.


  ## CLI

  The CLI entrypoint is `order_shipping_status.cli` and exposes the following (most relevant) options:

  - `input` (positional): path to input `.xlsx` (required)
  - `--replay-dir PATH`: load per-TN JSON payloads from `PATH` (deterministic replay)
  - `--use-api`: call the live FedEx API (requires credentials in environment)
  - `--dump-api-bodies PATH`: when using live API, persist raw API responses into a single JSON array file at `PATH` (FedExWriter writes a JSON array)
  - `--reference-date YYYY-MM-DD`: anchor date for prior-week filtering (Sunday..Saturday). Example: `2025-10-22`.
  - `--skip-date-filter`: disable prior-week date filtering
  - `--debug-sidecar PATH`: write per-row normalized sidecars `<Carrier>_<TN>.json` into the supplied directory
  - `--stalled-threshold-days N`: threshold to mark `IsStalled` based on days since latest event (default `4`)
  - `--no-console` / `--log-level LEVEL` / `--strict-env` as in the code

  Exit codes: `0` success, `1` internal error, `2` user/env error (missing input, invalid args, strict-env failures).


  ## Replay vs combined API dumps

  - `--replay-dir` (per-TN files) is the recommended mode for deterministic CI and regression tests.
  - `--dump-api-bodies PATH` writes a single JSON array file containing the raw response bodies. That file is useful as an archival artifact, but note that the current `ReplayClient` does not consume this combined dump directly — you must split it into per-TN files to replay the exact responses.


  ## Tests & integration

  - Unit tests cover the normalizer helpers, enricher/backfill logic, classifier precedence, and column contract.
  - Integration tests use replay fixtures under `tests/data/` and exercise the CLI end-to-end in replay mode. Live FedEx integration tests are gated by environment variables and skipped unless `SHIPPING_CLIENT_ID`/`SHIPPING_CLIENT_SECRET` are set.


  ## Column contract (key outputs)

  - FedEx status columns: `code`, `derivedCode`, `statusByLocale`, `description`
  - Metrics: `LatestEventTimestampUtc`, `DaysSinceLatestEvent`
  - Indicators: `IsPreTransit`, `IsDelivered`, `HasException`, `IsRTS`, `IsStalled`
  - Aggregates: `CalculatedStatus`, `CalculatedReasons`

  The contract ensures original input columns are kept first, and the suffix above is appended in a stable order.

  ---

  This README reflects the current implementation in `src/order_shipping_status` and the behavior exercised by the test-suite. If you want a PR patch that tightens wording or adds examples (split dump → per-TN splitter script), tell me which section to expand and I'll prepare it.

  ```
- `input` (positional): path to input `.xlsx` workbook (required).
