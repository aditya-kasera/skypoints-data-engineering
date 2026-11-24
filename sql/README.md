# Snowflake SQL deliverables

These scripts are the assessment's Snowflake DDL and transformation deliverables. They are Snowflake SQL, not DuckDB SQL. No Snowflake account was available, so the scripts have **not** been executed against Snowflake; static review does not establish warehouse execution.

## Prerequisites and raw loading

1. Connect to the intended Snowflake database with a role allowed to create a schema, tables, and session temporary tables. `01_raw_ddl.sql` creates and selects `SKYPOINTS_ASSESSMENT` in the current database.
2. Run `01_raw_ddl.sql`, then `02_staging_ddl.sql` once to create the run/artifact, raw landing, typed staging, quality issue, and reconciliation structures.
3. An approved loader must register `PIPELINE_RUN`, `INPUT_ARTIFACT`, and `RUN_INPUT`, then populate `RAW_MEMBER_LANDING` with one row per source row and `RAW_REDEMPTION_LANDING` with one row per parent payload. Preserve original raw values, source-column presence, source file/sheet/row or payload positions, SHA-256, batch/run identifiers, and UTC ingestion metadata. These transformations do not load local files themselves.

For the local member adapters, `SOURCE_CODE` and `SOURCE_FILENAME` must be exactly `AUS` / `AUS.xlsx`, `IND` / `IND.csv`, or `USA` / `USA.csv`. The filename and code must agree; only these pairs route to the corresponding sample target. Preserve original cell values in `RAW_VALUES`. For AUS Excel-native date cells, also populate `NORMALIZED_VALUES` with the same columns and ISO `YYYY-MM-DD` date strings for date/datetime cells; retain the original raw cell value in `RAW_VALUES`. The transformation parses the normalized value when supplied and falls back to raw for other source fields. This is an adapter contract, not a change to the source workbook. The Python adapter exposes Excel cells as `datetime` values and parses them as their calendar date; the ISO sidecar produces the same calendar-date result in SQL.

The local samples are `AUS.xlsx`, `IND.csv`, and `USA.csv`; Snowflake `COPY` does not ingest XLSX as a tabular source. A client-side adapter or an approved conversion is required for that workbook while retaining its original. The local CSV/XLSX files also differ from the assessment's production pipe-delimited flat-file example. The production file layout, stage, file format, and source manifest must be supplied by the owner. Do not treat the illustrative JSON fixture as a production feed.

For redemption rows, preserve the exact decoded JSON text even when invalid. After the loader inserts the text, set parse results and error diagnostics, for example:

```sql
UPDATE SKYPOINTS_ASSESSMENT.RAW_REDEMPTION_LANDING
SET PAYLOAD = TRY_PARSE_JSON(RAW_PAYLOAD_TEXT),
    JSON_PARSE_ERROR = CHECK_JSON(RAW_PAYLOAD_TEXT)
WHERE RUN_ID = $RUN_ID;
```

Invalid JSON remains in `RAW_PAYLOAD_TEXT` with `PAYLOAD` NULL and a diagnostic. Valid JSON must still have a top-level object and required parent/child structure; those are checked by the transformation scripts.

## Execution order

Set the real run ID and explicit `AS_OF_DATE` in `03_member_transformations.sql`; set the same run ID in scripts 04–06. Replace the example token before execution. Never substitute `CURRENT_DATE()` for `AS_OF_DATE`.

1. `01_raw_ddl.sql` — run, artifact, input, raw member and raw redemption landing, and reconciliation DDL.
2. `02_staging_ddl.sql` — typed member/redemption staging and quality issue DDL.
3. Load and register the run's raw input rows as described above.
4. `03_member_transformations.sql` — normalize the observed AUS/IND/USA source columns; parse dates without guessing ambiguous USA values; validate and derive Age/Stale_Member; record issues.
5. `04_redemption_flattening.sql` — preserve every parent outcome in `REDEMPTION_PARENT_STAGING`, including empty arrays and malformed/missing child arrays; flatten actual array elements with `LATERAL FLATTEN`; validate and retain each child outcome. Empty parents do not create synthetic transaction rows.
6. `05_country_targets.sql` — create physical AUS/IND/USA targets and publish non-quarantined sample rows transactionally.
7. `06_validation_and_reconciliation.sql` — run quality/reconciliation queries and upsert counts from persisted rows.
8. `07_latest_record_wins_synthetic_demo.sql` — optional isolated synthetic-only identity, movement, late-arrival, and tie-break demonstration. It does not read or write supplied member tables.
9. `08_review_regression_scenarios.sql` — optional synthetic SQL checks for parent retention/quarantine propagation, source-date parsing, and member/transaction uniqueness scopes. Each returned assertion column should be true; this fixture does not touch assessment data.

Each DML script that rebuilds run-scoped staging/targets uses a transaction and first replaces only that run's derived rows. Raw landing remains untouched. Standard Snowflake-table primary/unique/foreign-key constraints are metadata and are not enforced; the scripts enforce dispositions and output filters in SQL. Do not enable `RELY` unless the data has separately been verified. Snowflake enforces `NOT NULL` and `CHECK` constraints on standard tables. [Constraint behavior](https://docs.snowflake.com/en/sql-reference/constraints-overview)

## Design boundaries

- Assessment member fields retain the specified types/lengths: `VARCHAR(255)`, `VARCHAR(18)`, `DATE`, optional `CHAR(5)`, `CHAR(255)`, `CHAR(5)`, `CHAR(5)`, `NUMBER(5,0)`, `DATE`, and `CHAR(1)`. Raw date strings remain alongside typed dates.
- `MEMBER_NAME` is an assessment key, but uniqueness scope is unspecified. The SQL applies the documented provisional `(run_id, batch_id, source_code)` scope across artifacts in that batch and quarantines every same-scope duplicate. The target's scoped `UNIQUE` declaration is informational on standard Snowflake tables. `MEMBER_ID` remains non-key; duplicate IDs are warnings.
- Local routing is explicitly `AUS.xlsx → AUS`, `IND.csv → IND`, `USA.csv → USA` with `FILENAME_SAMPLE_ADAPTER` provenance. It is not production country truth. `COUNTRY` remains separate from `TARGET_COUNTRY_CODE`.
- `AS_OF_DATE` drives age and stale status. Stale is true only for a day difference strictly greater than 90; missing/invalid/future flight dates yield NULL. The February 29/March 1 age convention is an implementation assumption documented in the SQL.
- `REDEMPTION_PARENT_STAGING` is the queryable parent outcome relation. It retains parents with empty arrays, missing/non-array `redemptions`, invalid top-level structures, missing/non-string `member_id`, invalid `feed_date`, and malformed JSON (whose raw text and parse diagnostic remain in landing). `FLATTENED_REDEMPTION` has one row per actual array child and retains children of quarantined parents with a quarantined disposition; `CURATED_REDEMPTION` excludes both quarantined children and any child whose parent is quarantined. Empty parents never produce fake transactions.
- Duplicate transaction IDs are checked within `(run_id, batch_id)` across artifacts in that source batch. This is an explicit SQL validation scope only; the assessment does not declare `txn_id` a key or establish uniqueness across separate batches. Python's parser separately uses one invocation scope; this difference is documented and neither behavior claims global uniqueness. No member join is inferred: matched locator fields remain NULL unless a separately confirmed mapping is provided. The assessment fixture's `223457` value does not join to local IDs 1–3. Unmatched issues are emitted only for otherwise valid child transactions under accepted parents.
- Reconciliation reads parent counts from raw landing and disposition/outcome counts from `REDEMPTION_PARENT_STAGING`; transaction child counts are computed by flattening raw arrays and compared with persisted child outcomes. The unmatched count is distinct eligible transaction locators with an unmatched issue, restricted to warning outcomes under accepted parents. Curated counts come from `CURATED_REDEMPTION`.
- The current `USA.csv` header has no DOB column. The SQL preserves that absence as `DOB_STATUS = 'ABSENT'`; if a future file under this adapter includes the documented `DOB` source field, the SQL supports 7/8-digit compact parsing and retains ambiguous raw values. This does not assert that the supplied USA source contains DOB.
- No global member ID or business update timestamp is added to actual-source tables. The latest-record-wins algorithm is only shown with synthetic values in script 07; its source sequence is explicitly synthetic and is a tie-breaker only for that fixture.
- Snowflake `FLATTEN`, `TRY_TO_DATE`, `MERGE`, and constraint declarations follow Snowflake's documented syntax. [FLATTEN](https://docs.snowflake.com/en/sql-reference/functions/flatten), [TRY_TO_DATE](https://docs.snowflake.com/en/sql-reference/functions/try_to_date), [CREATE TABLE constraints](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint)

No credentials, accounts, databases, roles, warehouses, external stages, or cloud infrastructure are supplied or created by these deliverables.
