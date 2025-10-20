# Order Shipping Status — Overview & Replay Guide

This document summarizes what the system does today, how replay works (with or without an input file), and how the pipeline is designed and implemented — updated to reflect the latest working code and tests.


## What the System Does



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
  - IsPreTransit, IsDelivered, HasException, IsRTS, IsStalled
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

`normalize_fedex(payload, *, tracking_number, carrier_code, source)` extracts the **core** fields expected across the codebase and tests:

  - `scanEvents[].date` and
  - `dateAndTimes[].dateTime`
  normalized to **UTC ISO-8601**

Robust fallbacks are used so unit tests pass with either **flat** or **deep** payloads:

> The normalizer deliberately **does not** emit extra columns not used by tests/contracts.


## Enrichment

  - **Replay**: `ReplayClient(replay_dir)` loads `<tracking>.json`.
  - **Live**: fetch from real API (out of scope for tests).

All merged columns are string-coerced as needed to avoid NaN/NaT churn downstream.


## Metrics


> The “now” used for metrics can be injected via `WorkbookProcessor(reference_now=...)` to make tests deterministic.


## Indicators & Classification

### Indicators (ground truth aligned with your code)

  _Purely from the latest FedEx status code._

  _Purely from the latest FedEx status code. “OC” / “LP” style variants are treated according to current normalizer rules; tests target `OC`._

  Current logic checks:
  - status **code** in the known exception code set; and/or
  - status **text** contains “exception”.

  - Regex/text: “return to shipper”, “returning to shipper”, etc.
  - Code variants (if present in data) are treated per the indicators module.

  - **Terminal** = `IsDelivered == 1` **OR** `IsRTS == 1`  
  - **Exception does not block stalled** (by design).
  - The threshold is configurable per processor (`stalled_threshold_days`, default typically `4`; tests may pass `1`).

All indicators are added as integer columns (`0/1`) and are independent unless stated above (e.g., Stalled is suppressed by terminal states).

### CalculatedStatus & CalculatedReasons

  1. `IsRTS` → **“Returned to Sender”**  
  2. `IsDelivered` → **“Delivered”**  
  3. `HasException` → **“Exception”**  
  4. `IsPreTransit` → **“Pre-Transit”**  
  5. Otherwise empty (`""`)

  `CalculatedReasons` is the **join** of active indicators in this fixed order:  
  **`PreTransit;Delivered;Exception;ReturnedToSender`**  
  If none are active, it’s an empty string.

These rules match the unit/integration tests (e.g., delivered-with-exception precedence, RTS overrides everything, pre-transit when only pre-transit is set, and stalled independent of exception).


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
  --replay-dir tests/data/replay --reference-date 2025-10-07
```

Installed (if you install the package):

```bash
# after `pip install -e .` or similar
order-shipping-status /path/to/input.xlsx --replay-dir /tmp/replay
```

Key CLI options (matching `src/order_shipping_status/cli.py`):

- `input` (positional): path to input `.xlsx` workbook (required).
- `--replay-dir PATH`: use replay JSONs from this directory instead of calling the live FedEx API. Each file should be named `<TrackingNumber>.json`.
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



## Column Contract (key outputs)

- FedEx status columns: `code`, `derivedCode`, `statusByLocale`, `description`
- Metrics: `LatestEventTimestampUtc`, `DaysSinceLatestEvent`
- Indicators: `IsPreTransit`, `IsDelivered`, `HasException`, `IsRTS`, `IsStalled`
- Aggregates: `CalculatedStatus`, `CalculatedReasons`

The contract keeps column order stable: original columns first (in original order), then the known suffix list above.

---

*This document tracks the current, working behavior validated by the latest passing tests and your clarified rules (Delivered = `DL`, Pre-Transit = `OC`).*
