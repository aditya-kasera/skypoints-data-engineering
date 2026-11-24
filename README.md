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

## Redemption JSON

`process_redemption_payloads(...)` accepts one JSON string/bytes/object or an ordered sequence of them and flattens every `redemptions` array element into a transaction outcome. It retains parent member/feed fields, parsed dates where valid, the raw parent and child values, and payload/transaction provenance. Malformed parent payloads and child objects remain in the result with structured issues and quarantined dispositions; duplicate transaction IDs are quarantined within one processing call.

The PDF example is stored only in `tests/fixtures/redemptions_assessment_example.json` as an illustrative test fixture. It is not a supplied production feed. No complete partner or status domain is assumed; non-positive integer miles are retained with a warning pending reversal/zero-value rules. An optional `member_key_mapping` must be supplied explicitly to map redemption member IDs to confirmed profile keys. Without a match, transactions remain queryable with an unmatched-reference warning. The local files' IDs do not establish a join to the illustrative ID `223457`.

The package provides a local end-to-end runner over the in-memory processing modules and DuckDB persistence APIs. It executes the three supplied member adapters and the clearly illustrative assessment redemption fixture. Snowflake SQL deliverables are under `sql/`; see [the SQL execution guide](sql/README.md). They have not been run against a Snowflake account.

## Execution and testing

Run the local demonstration from the repository root with an explicit business reference date:

```bash
python -m skypoints_data_engineering run-demo --as-of-date 2024-01-15
```

The default database is `build/skypoints.duckdb`. Override the database, member input directory, or illustrative fixture with `--database`, `--data-dir`, and `--redemption-fixture`. Each invocation receives a new execution `run_id`, while identical source content retains the same artifact identity. Operational ingestion timestamps are generated in UTC; `as_of_date` is always provided by the caller. A failed run is marked failed and exits non-zero.

The command reports counts read from DuckDB: artifact identities and source record counts, member dispositions and country views, redemption parent/transaction dispositions, unmatched transactions, raw/curated persistence totals, and reconciliation status. Reconciliation is checked against actual persisted rows before the run is marked successful. A representative output shape is:

```text
Run: demo-<execution-id> (succeeded)
As of: 2024-01-15
Input artifacts:
  member: AUS.xlsx artifact=<artifact-id> records=3
  member: IND.csv artifact=<artifact-id> records=3
  member: USA.csv artifact=<artifact-id> records=3
  redemption: redemptions_assessment_example.json artifact=<artifact-id> records=1
Members accepted/warned/quarantined: 6/1/2
Country curated: AUS=2, IND=3, USA=2
Redemption parents accepted/warned/quarantined/total: 1/0/0/1
Transactions accepted/warned/quarantined/unmatched/total: 0/2/0/2/2
Persisted raw members/curated members/raw parents/raw transactions/curated transactions: 9/7/1/2/2
Reconciliation: OK
Database: <database-path>
```

The fixture's redemption rows are unmatched warnings because the illustrative `member_id` has no confirmed mapping to supplied profiles. Quarantined members remain in raw and outcome tables but are excluded from country views. Warnings remain queryable and are not silently dropped.

Inspect the current run using its printed ID:

```sql
SELECT member_name, source_member_id, age, stale_member, routed_country_code
FROM members_aus
WHERE run_id = '<printed-run-id>';

SELECT member_id, txn_id, parsed_transaction_date, partner, miles_redeemed,
       status, matched_member_run_id, matched_member_raw_record_id
FROM curated_redemptions
WHERE run_id = '<printed-run-id>';

SELECT scope, read_count, accepted_count, warned_count, quarantined_count,
       transformed_count, unmatched_count, raw_persisted_count, curated_persisted_count
FROM run_reconciliation
WHERE run_id = '<printed-run-id>';
```

Known limitations: the supplied datasets have source-local IDs but no confirmed global member identity or business update/effective timestamp. The demo does not perform latest-record-wins or prove country movement. Filename routing is sample-only. Its few records do not establish production throughput. The existing persistence retry guard verifies raw/outcome locators, member issue contents, redemption issue counts, and reconciliation counts. It does not compare every persisted raw, outcome, or redemption issue value with its expected source value, so content-level tampering with intact keys remains a known persistence limitation. Each runner invocation gets a new run ID and preserves separate history; it does not repair a prior run.

Run the tests with:

```bash
python -m pytest
```

GitHub Actions runs the same test command on pushes and pull requests using Python 3.10.

## Repository layout

```text
data/raw/                         Supplied CSV and Excel source files
sql/                              Snowflake DDL, transformation SQL, and execution guide
src/skypoints_data_engineering/   Installable Python package
tests/                            Automated tests
tests/fixtures/                   Clearly labelled illustrative/synthetic test inputs
```

To initialize the local DuckDB schema and persist one processed member artifact:

```python
from datetime import date

from skypoints_data_engineering.ingestion import ingest_source_file
from skypoints_data_engineering.persistence import (
    create_pipeline_run,
    finish_pipeline_run,
    initialize_schema,
    open_database,
    persist_member_results,
)
from skypoints_data_engineering.transforms import transform_batch
from skypoints_data_engineering.validation import validate_batch

with open_database("skypoints.duckdb") as connection:
    initialize_schema(connection)
    as_of_date = date(2026, 11, 24)  # Supply the assessment run's explicit reference date.
    batch = ingest_source_file("data/raw/IND.csv")
    validation = validate_batch(batch, as_of_date=as_of_date)
    transformation = transform_batch(validation, as_of_date=as_of_date)
    create_pipeline_run(connection, "demo-run-001", as_of_date=as_of_date)
    persist_member_results(
        connection, "demo-run-001", batch, validation, transformation,
    )
    finish_pipeline_run(connection, "demo-run-001", status="succeeded")
```

`open_database` closes connections it opens from a path; when given an existing
DuckDB connection, its caller retains ownership. Schema initialization is
transactional and idempotent for this schema version; it does not migrate older
schemas. Each artifact persistence call is transactional and idempotent for a
complete successful retry of the same run/artifact. Before returning from a
succeeded-input retry, persistence checks raw/outcome locators, member issue
contents, redemption issue counts, and database-derived reconciliation; missing or inconsistent state raises
`PersistenceIntegrityError`. A failed persistence transaction is rolled back
and its run-input failure status is recorded for retry. Intentional
reprocessing uses a new run ID. Redemption
payloads are hashed from exact `source_bytes` when supplied; in-memory-only
payloads use a deterministic recursive type-tagged representation. Mapping key
order does not affect identity; nested `None`, booleans, integers, finite
floats, strings, bytes, dates, datetimes, mappings, lists, and tuples retain
distinct type tags. Exact text inputs are UTF-8 text identities and bytes inputs
are raw-byte identities, so equal-looking text and bytes intentionally hash
differently. Unsupported objects, cyclic containers, and non-finite floats
raise an explicit error instead of being coerced. When mapping payloads are
stored, `raw_payload_json` uses a versioned `typed-json-v1` envelope so nested
Python date/time and byte values remain distinguishable. These canonical hashes
are not source-file byte hashes; only caller-supplied `source_bytes` provide
that evidence. Member references
are stored only when an explicit matched-key-to-persisted-record mapping is
provided. These APIs do not yet orchestrate a complete run or automatically
persist all three sources. `register_input_artifact` is available for explicit
pending input registration; member and redemption persistence calls register
their own artifacts and mark inputs succeeded only after the artifact
transaction commits. A run cannot be marked succeeded while a registered input
remains pending, running, or failed.

The assessment PDF and operating-system metadata are kept out of Git. Raw CSV and Excel inputs are preserved as supplied.
