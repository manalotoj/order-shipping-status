# Order Shipping Status — Overview & Replay Guide

This document summarizes what the system does today, how replay works (with or without an input file), and how the pipeline is designed and implemented — updated to reflect the latest working code and tests.


## What the System Does

This project processes an input Excel workbook of shipment rows and enriches each row with carrier-derived status and metrics. For each Tracking Number the pipeline either replays a recorded carrier response (from a combined JSON dump) or calls the live FedEx API, normalizes the response, and merges canonical fields into the row. The pipeline computes timing metrics (LatestEventTimestampUtc, DaysSinceLatestEvent), derives indicator flags (IsPreTransit, IsDelivered, HasException, IsRTS, IsStalled), and composes a human-friendly CalculatedStatus and CalculatedReasons. The final artifact is a stable, processed workbook (xlsx) suitable for downstream reporting and deterministic CI/regression testing.

   # Order Shipping Status — Overview & Replay Guide

  This document summarizes what the system does today, how replay works (with or without an input file), and how the pipeline is designed and implemented — updated to reflect the latest working code and tests.

  ## Supported Modes

  ### Normal mode (with input file)
    - `Tracking Number`
    - `Carrier Code` (e.g., `FDX`)
    - Optionally `Promised Delivery Date`, `Delivery Tracking Status`, etc.

  ### Replay mode (with or without input file)
    - A **flat** minimal body: `{"code": "...","statusByLocale": "...","description": "..."}`; or
    - A **deep** FedEx response under `output.completeTrackResults[*].trackResults[*]`, including `latestStatusDetail`, `scanEvents[]`, `dateAndTimes[]`.


  ## High-Level Design

  ```
  Input Workbook (xlsx)
    │
    ▼
  Preprocessor
    - Drops disposable lead column
    - Parses/filters by reference date (optional)
    - Excludes delivered rows from input if configured
    │
    ▼
  ColumnContract
    - Ensures stable suffix of output columns (schema)
    │
    ▼
  Enricher
    - For each row: fetch payload (replay or live) + normalize (FedEx)
    - Merge normalized columns
    - Backfill LatestEventTimestampUtc if missing
    │
    ▼
  Metrics
    - LatestEventTimestampUtc (UTC ISO-8601)
    - DaysSinceLatestEvent (int)
    │
    ▼
  Rules (Indicators)
    - IsPreTransit
    - IsDelivered 
    - HasException 
    - IsRTS
    - IsStalled 
    - UnableToDeliver
    │
    ▼
  Status Mapper
    - CalculatedStatus + CalculatedReasons
    │
    ▼
  Processed Workbook (xlsx)
  ```

  ## Preprocessing

  > Preprocessing never reaches out to carrier APIs; it’s purely input sanitation and scoping.

  ## Normalization (FedEx)
  `normalize_fedex(payload, *, tracking_number, carrier_code, source)` extracts the small, canonical set of fields the rules and downstream code expect. The normalizer supports two common input shapes:

  - Flat/minimal: a single-level mapping (e.g. `{"code":"DLV","statusByLocale":"Delivered","description":"Left at front door"}`).
  - Deep/full FedEx response: the carrier JSON where per-package data typically lives under `output.completeTrackResults[*].trackResults[*]` (or inside wrapper keys like `body`/`response`).

  Key normalized outputs (when derivable):

  - `code` (short status code, e.g. `DLV`, `OC`)
  - `derivedCode` (normalizer-derived canonical code when `code` is missing or a variant)
  - `statusByLocale` (human-friendly text such as `Delivered`)
  - `description` (free-form description)
  - `LatestEventTimestampUtc` (ISO-8601 UTC timestamp of the most relevant event)
  - `ScanEventsCount` and `scanEvents` (when available)

  Behavior and fallbacks:

  - Timestamps are normalized to UTC ISO-8601; the normalizer prefers explicit `latestStatusDetail` or `dateAndTimes` fields, falling back to scan event timestamps.
  - The normalizer will attempt to extract a tracking number from common locations; callers may pass an explicit `tracking_number` to disambiguate.
  - The function is defensive: it returns a mapping with empty/default fields when data is missing so downstream logic can run deterministically without crashing.


  ## Enrichment

  - Data flow: the `Enricher` iterates rows from the preprocessor and, for each row, obtains a carrier payload either by looking up a recorded response (replay file) or by calling the live API.

  - Replay semantics: `ReplayClient(replay_dir)` expects a single combined JSON file and builds an in-memory index keyed by tracking number at initialization for fast lookups. If a payload is not found the enricher proceeds with empty/default values and logs a diagnostic.

  - Live semantics: when `--use-api` is enabled the enricher will call the shipping client and (when `--dump-api-bodies` is set) append raw responses to the combined dump file for later replay.

  - Merge rules:
    - The normalizer provides canonical columns; the enricher merges these into the input row, preferring non-empty normalized values over input values.
    - When `LatestEventTimestampUtc` is present it is used to compute `DaysSinceLatestEvent`; otherwise the enricher attempts to derive a suitable timestamp from scan events.
    - Merged values are coerced to predictable types (strings, ints) to avoid Pandas NaN/NaT churn; indicators are integers (0/1).

  - Diagnostics & sidecars:
    - When `--debug-sidecar PATH` is supplied the enricher writes per-row normalized JSON sidecars named `<Carrier>_<TrackingNumber>.json` to the supplied directory for debugging.

  - Performance: the ReplayClient index gives O(1) lookups per row. For very large dumps consider streaming or pre-filtering before running enrichment in memory-constrained environments.


  ## Metrics


  > The “now” used for metrics can be injected via `WorkbookProcessor(reference_now=...)` to make tests deterministic.


  ## Indicators & classification

  The indicator and classification rules live in the `rules` package and operate on normalized/enriched fields. Key behaviors and implementation details:

  - IsPreTransit (0/1): true when `code` or `derivedCode` maps to a pre-transit category (e.g., `OC`, `LP` variants) or when `statusByLocale` text indicates a label/label-created state. The normalizer canonicalizes known code variants before this check.

  - IsDelivered (0/1): true when `statusByLocale` or `code` indicates delivery (e.g., `Delivered`, `DLV`), or when `latestStatusDetail` explicitly marks delivery.

  - HasException (0/1): true when `code` is an exception code or `description`/`statusByLocale` contains exception-like text. Text and code-based heuristics are applied to cover carrier variations.

  - IsRTS (0/1): detected from RTS-like text (e.g., `returned to sender`, `return to shipper`) or known RTS codes. RTS is treated as a terminal state and takes precedence in `CalculatedStatus`.

  - IsStalled (0/1): true when `DaysSinceLatestEvent` >= `stalled_threshold_days` (configurable) OR when there are zero scan events and the threshold is exceeded. Stalled may be set together with `HasException`, but it is suppressed when a terminal state (`IsDelivered` or `IsRTS`) is present.

  All indicators are integer columns (0/1). `CalculatedStatus` is derived from indicators in precedence order:

  1. If `IsRTS` then `Returned to Sender`
  2. Else if `IsDelivered` then `Delivered`
  3. Else if `HasException` then `Exception`
  4. Else if `IsPreTransit` then `Pre-Transit`
  5. Else empty string

  `CalculatedReasons` concatenates active indicator labels in a stable order (for example: `PreTransit;Delivered;Exception;ReturnedToSender;Stalled`) so tests can assert exact strings.

  The rules are deterministic and conservative; unit and integration tests rely on exact strings and integer indicator values.


  ## WorkbookProcessor Parameters (most relevant)



  ## CLI & Public API

    - `WorkbookProcessor`
    - `process_workbook` (shim)

  ## CLI Usage

  This project exposes a small CLI entrypoint `order-shipping-status` (the module defines an argparse-based parser). Below are examples showing how to run the CLI during development (without installing) and after installation.

  Development (run from repo root):

  ```bash
  # Use the in-repo package on PYTHONPATH
  PYTHONPATH=src python -m order_shipping_status.cli /path/to/input.xlsx \
    --replay-dir tests/data/RAW_TransitIssues_10-20-2025-json-bodies.json --reference-date 2025-10-07
  ```

  Installed (if you install the package):

  ```bash
  # after `pip install -e .` or similar
  order-shipping-status /path/to/input.xlsx --replay-dir /tmp/replay
  ```

  Key CLI options (matching `src/order_shipping_status/cli.py`):

  - `input` (positional): path to input `.xlsx` workbook (required).
  - `--replay-dir PATH`: path to a single JSON file (combined dump) containing one or more API bodies to use for deterministic replay. Historically a directory of per‑TN files was used, but current usage expects a single combined JSON file.
  - `--use-api`: call the live FedEx API (requires credentials in env). Ignored when `--replay-dir` is set.
  - `--reference-date YYYY-MM-DD`: anchor date for the preprocessor prior-week filter (Sunday..Saturday). Example: `2025-10-07`.
  - `--skip-date-filter`: disable the prior-week date filtering (useful for replay runs or full reprocesses).
  - `--stalled-threshold-days N`: integer threshold for DaysSinceLatestEvent to mark `IsStalled` (default `4`).
  - `--debug-sidecar PATH`: write normalized sidecar JSON files per tracking number into PATH for diagnostics.
  - `--no-console`: disable console logging (file logging still occurs).
  - `--log-level LEVEL`: logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.
  - `--strict-env`: fail early if required shipping credentials are not present in environment (exit code 2).

  Exit codes:

  - `0`: success
  - `1`: internal error during processing
  - `2`: user/environment error (missing input file, invalid --reference-date, missing required env when `--strict-env`)

  Notes:

  - When using `--replay-dir`, the CLI uses `ReplayClient` and `normalize_fedex` to load carrier responses from disk — this mode is deterministic and recommended for CI and regression testing.
  - If `--use-api` is set, the CLI will attempt to construct a `FedExClient` using environment variables (see `src/order_shipping_status/config/env.py`), and will make real HTTP calls via `RequestsTransport`.

  ## Example: generate a combined JSON dump and replay it

  A common developer workflow is to (A) call the live API once to collect raw responses into a combined JSON dump, then (B) run deterministic replay runs from that dump.

  1) Generate the combined dump (writes `<input-stem>-json-bodies.json` next to the workbook). This requires valid FedEx credentials in your environment or a `.env` you load into the shell:

  ```bash
  PYTHONPATH=src python -m order_shipping_status.cli \
    tests/data/RAW_TransitIssues_10-20-2025.xlsx \
    --use-api --dump-api-bodies --reference-date 2025-10-22 --no-console
  ```

  After this run you should see a file named `tests/data/RAW_TransitIssues_10-20-2025-json-bodies.json` containing an array of raw API bodies.

  2) Replay deterministically using the generated dump (no network calls):

  ```bash
  PYTHONPATH=src python -m order_shipping_status.cli \
    tests/data/RAW_TransitIssues_10-20-2025.xlsx \
    --replay-dir tests/data/RAW_TransitIssues_10-20-2025-json-bodies.json \
    --reference-date 2025-10-22 --no-console
  ```

  Notes:
  - The `--dump-api-bodies` flag appends raw responses to the computed `<input-stem>-json-bodies.json` path; multiple runs will append to the same file (use a fresh copy if you want a reproducible snapshot).
  - Replay runs do not require credentials and are safe to run in CI.


  ## Column Contract (key outputs)

  - FedEx status columns: `code`, `derivedCode`, `statusByLocale`, `description`
  - Metrics: `LatestEventTimestampUtc`, `DaysSinceLatestEvent`
  - Indicators: `IsPreTransit`, `IsDelivered`, `HasException`, `IsRTS`, `IsStalled`
  - Aggregates: `CalculatedStatus`, `CalculatedReasons`

  The contract keeps column order stable: original columns first (in original order), then the known suffix list above.

  ---

  *This document tracks the current, working behavior validated by the latest passing tests and your clarified rules (Delivered = `DL`, Pre-Transit = `OC`).* (See <attachments> above for file contents. You may not need to search or read the file again.)
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
