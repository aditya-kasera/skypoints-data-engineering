# Milestone 1: source profiling and canonical model proposal

## Scope and method

Profiled the three supplied files with read-only pandas/openpyxl inspection: `data/raw/AUS.xlsx`, `data/raw/IND.csv`, and `data/raw/USA.csv`. Original files were not changed. Counts below describe data records, excluding headers.

The original assessment specification is not available in this checkout or its reachable Git history. The prior assessment PDF is not present under the repository, and the initial commit does not contain it. Therefore this report cannot make a factual comparison against the PDF's requirements; see the unresolved questions below.

## Facts observed in the files

### File shape, schemas, and source types

| Source | Sheet / records | Columns in source order | Observed representations |
|---|---|---|---|
| `AUS.xlsx` | One visible sheet, `Sheet1`; 3 rows | `Unique ID`, `Member Name`, `Tier Type`, `Date of Birth`, `Date of Enrollment`, `Date of Flight` | `Unique ID` is numeric; name and tier are text. DOB contains Excel date cells and the text marker `NULL`. Enrollment contains Excel date cells and one text value. Flight date cells are Excel dates. |
| `IND.csv` | 3 rows | `ID`, `Name`, `DOB`, `TierCode`, `EnrollmentDate`, `Individual or Corporate`, `Flight Date` | UTF-8 with BOM. ID is integer-like; all other fields, including dates, are text. Dates use slash-separated month/day/year forms such as `12/1/1998`. |
| `USA.csv` | 3 rows | `ID`, `Name`, `TierCode`, `EnrollmentDate`, `FlightDate` | UTF-8 with BOM. ID, enrollment date, and flight date infer as integers; name and tier are text. Date values have no separators, for example `6152022`. |

There are 9 data rows in total. No blank CSV fields were observed. In the workbook, the `Date of Birth` cell for ID 1 contains the literal text `NULL`; this is not an empty Excel cell. `Date of Enrollment` for ID 2 is text rather than an Excel date cell.

### Missing and source-specific fields

- `DOB` / `Date of Birth` is absent as a column in USA. AUS has a `NULL` marker for one DOB; all IND DOB cells are populated.
- `Individual or Corporate` exists only in IND and contains `I`, `C`, and `I`.
- Field names differ by source: `Unique ID` / `ID`, `Member Name` / `Name`, `Tier Type` / `TierCode`, and multiple flight-date spellings.
- Country is not a column in any source. AUS, IND, and USA are filename labels only.
- `Jonnathan` is the exact AUS spelling observed; it has not been corrected or presumed erroneous.

### Dates and validity

- **AUS:** Valid date values are stored as Excel datetime cells and displayed in a year-month-day format. DOB for ID 1 is the literal `NULL`. Enrollment for ID 2 is the invalid calendar string `2021-13-13` (month 13, day 13); it cannot be parsed as a date. The other observed DOB, enrollment, and flight values are valid Excel dates.
- **IND:** DOB, enrollment, and flight fields use slash-separated values consistent with `M/D/YYYY`; all 9 values parse under that interpretation. The file does not declare a locale or format contract, so this is an observed/inferred format, not an authoritative rule.
- **USA:** All 6 enrollment/flight values are compact digit strings represented as integers, with no separators or zero padding. `6152022`, `8202022`, `12282021`, and `12302021` have a single plausible month/day/year split. `1052022` can be `1/05/2022` or `10/5/2022`; `1152022` can be `1/15/2022` or `11/5/2022`. Interpreting these as January dates would make the member's flight 10 days after enrollment in each row, but that relationship is not specified and must not be used as a business rule. Do not silently select either interpretation.

### IDs, identity, categories, and consistency

- Each source has IDs `1`, `2`, and `3`, with no duplicate ID within that file. Every ID is repeated across all three sources, so IDs are demonstrably not globally unique in this sample.
- `Mike` appears in AUS (source ID 1, missing DOB) and USA (source ID 3, no DOB column). This is a possible name collision, not evidence that the records are the same person. No cross-source identity match can be established from these fields.
- Tier values are consistently uppercase and belong to the same observed set in every source: `PLT`, `GLD`, `SLV`. Their meanings, ordering, and equivalence are not supplied.
- No country values are present to compare. Country attribution based on filenames remains an assumption.
- IND's `I`/`C` member-type values are only observed there; their meaning and whether corporate records can be members in the same entity model are unresolved.

## Assessment specification comparison

The assessment PDF is absent from the current worktree and Git history, so its requested fields, rules, and expected outputs cannot be checked against these files. The observable source-level differences that need reconciliation with the specification are: different headers and date encodings; USA's missing DOB and member-type columns; AUS's `NULL` DOB and invalid enrollment value; repeated source-local IDs; and country inferred only from filenames. No claim is made that these are deviations from the assessment requirements until the specification is available.

## Canonical schema proposal

The following is a proposal for review, not an approved contract. It represents source records without inventing a global person identifier or update timestamp.

| Field | Proposed type | Nullability / notes |
|---|---|---|
| `source_code` | string | Required intake namespace, e.g. `AUS`, `IND`, `USA`, taken from the source filename; confirm these are the intended source identifiers. |
| `source_file` | string | Required exact input filename. |
| `source_sheet` | string | Nullable; `Sheet1` for AUS, null for CSV files. |
| `source_row_number` | integer | Required source data row position for traceability; header excluded. Not a member ID. |
| `source_member_id_raw` | string | Required source-local ID as represented in the source. Do not treat it as globally unique. |
| `member_name_raw` | string | Required as observed; retain spelling and whitespace pending approved standardization rules. |
| `tier_code_raw` | string | Required as observed. Do not map tier semantics or ordering without a supplied definition. |
| `date_of_birth_raw` | string | Nullable; retain the literal `NULL` and source formatting. Null in USA because the column is absent. |
| `date_of_birth` | date | Nullable parsed value; only populate when a source-specific, approved parse rule succeeds. |
| `enrollment_date_raw` | string | Required raw value for traceability, including malformed/ambiguous values. |
| `enrollment_date` | date | Nullable parsed value; invalid or ambiguous raw values must remain available for review. |
| `member_type_code_raw` | string | Nullable; preserve `I`/`C` for IND without assigning meanings. |

`Flight Date` / `FlightDate` looks like a flight-level event date rather than an established member attribute. Keep its raw value and source-row provenance in source staging; model it as a separate flight/event fact only after confirming event semantics and any event key. Do not create synthetic event IDs. Preserve the original files unchanged as the raw layer. A validation outcome can identify unparseable or ambiguous dates, but must not discard their source rows.

### Design decisions recommended for review

1. Namespace source IDs by source for traceability; do not use them as a global member key or merge people by name alone.
2. Keep raw date values alongside nullable parsed dates, and report invalid/ambiguous values explicitly.
3. Keep source filename, optional sheet, and row number with each staged observation so every canonical value can be traced back to its input.
4. Defer country attribution, tier mapping, identity resolution, member-type interpretation, and flight-event modeling until the assessment specification or stakeholder answers define them.

These are recommendations only; they have not been approved as business rules.

## Assumptions

- The `AUS`, `IND`, and `USA` filename prefixes identify source feeds; they do not, by themselves, prove the member's country.
- Slash-separated IND dates appear to use month/day/year, and the USA compact numbers appear intended to encode month/day/year. USA's two 7-digit values remain ambiguous even under this likely interpretation.
- The numeric IDs are local to each source because all three feeds independently use `1`–`3`; confirm before defining source-key constraints.

## Unresolved business questions

1. Can the assessment PDF/specification be supplied so its required source fields, target outputs, and business rules can be compared accurately?
2. Are IDs source-local, and is there any approved cross-source identity key or matching policy?
3. What is the authoritative USA date format, especially for `1052022` and `1152022`? Should invalid and ambiguous dates reject a record, quarantine it, or remain nullable with an issue report?
4. What are the meanings and valid ordering/mappings of `PLT`, `GLD`, and `SLV`?
5. What do IND's `I` and `C` mean, and should both be represented as members?
6. Are the filename prefixes authoritative country/source codes, and should canonical country be present?
7. Is each flight date a flight event, a latest-flight attribute, or something else? Can there be multiple flights per member, and is an event identifier available?
8. Is `NULL` the only missing-value token, and are any source-specific name or date normalization rules required?
