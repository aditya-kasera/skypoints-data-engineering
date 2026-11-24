# SkyPoints Data Engineering Technical Assessment

This repository contains the design documentation and local implementation for the SkyPoints data engineering assessment. The assessment requirements, source profiling, architecture, and requirements traceability are documented under `docs/`; implementation is proceeding incrementally against that approved design.

## Requirements

- Python 3.10 or newer
- `pip`

## Setup

Create and activate a virtual environment, then install the package and test dependencies:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
python -m pip install -e ".[test]"
```

## Raw source ingestion

The in-memory ingestion API reads exactly `AUS.xlsx`, `IND.csv`, and `USA.csv` from `data/raw/`. It records source/sheet/row provenance, a content hash, schema fingerprints, a stable content-based batch ID, and a UTC ingestion timestamp. Raw headers and values are retained as read. Ingestion itself does not normalize dates, assign member country, or write output files.

```python
from pathlib import Path

from skypoints_data_engineering.ingestion import ingest_active_sources

batches = ingest_active_sources(Path("data/raw"))
for batch in batches:
    print(batch.source_file, len(batch.records), batch.batch_id, batch.staged_filename)
```

The staged filename is metadata only; the original source files are not renamed or modified.

## Validation

`validate_batch(batch, as_of_date=...)` requires an explicit reference date and returns one outcome for every raw record, with an `accepted`, `warning`, or `quarantined` disposition and structured issues linked to the original record and its complete provenance. It checks required source fields, specified field lengths, source-specific date validity, and duplicate source IDs/member names within that batch. Duplicate Member Names quarantine every row in the batch-scoped duplicate group because the assessment declares the name as a key; duplicate source IDs are warnings because the assessment does not declare them as keys. Same-name records across batches are not compared or merged. Future DOB is quarantined; future enrollment/flight dates are warnings pending source-owner confirmation. No system clock is used.

## Derived fields and local country routing

`transform_batch(validation_result, as_of_date=...)` derives `age` and `stale_member` for non-quarantined records and applies the local filename sample adapter (`AUS.xlsx` → `AUS`, `IND.csv` → `IND`, `USA.csv` → `USA`). Each curated row retains its original raw record and validation outcome. The returned result also retains every validation outcome, including quarantined records excluded from curated rows. Routed records carry `FILENAME_SAMPLE_ADAPTER` provenance; an unrecognized filename is left unrouted and reported with a linked issue.

Age uses birthday-aware calculation. For a February 29 DOB in a non-leap year, this implementation uses March 1 as the birthday; this is a documented implementation assumption, not an assessment rule. Missing or invalid DOB yields no age. `stale_member` is true only when the last flight was more than 90 days before `as_of_date`; exactly 90 days is false. Missing, invalid, or future flight dates yield no stale flag. The same explicit `as_of_date` must be used for validation and transformation; no system clock is consulted.

The local DuckDB runner, redemption processing, and Snowflake SQL deliverables are not implemented yet.

## Execution and testing

No DuckDB runner or full assessment pipeline is implemented yet. Run the tests with:

```bash
python -m pytest
```

GitHub Actions runs the same test command on pushes and pull requests using Python 3.10.

## Repository layout

```text
data/raw/                         Supplied CSV and Excel source files
sql/                              Reserved for Snowflake SQL deliverables
src/skypoints_data_engineering/   Installable Python package
tests/                            Automated tests
```

The assessment PDF and operating-system metadata are kept out of Git. Raw CSV and Excel inputs are preserved as supplied.
