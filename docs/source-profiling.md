# Milestone 1: source profiling and canonical model proposal

## Scope and method

Profiled the three supplied files with read-only pandas/openpyxl inspection: `data/raw/AUS.xlsx`, `data/raw/IND.csv`, and `data/raw/USA.csv`. Original files were not changed. Counts below describe data records, excluding headers.

The authoritative assessment PDF is available locally at `reference/assessment.pdf` and is ignored by Git. It is reference-only and must not be included in the public repository. The comparison below is updated against that PDF; the active input inventory remains exactly these three distinct files: `AUS.xlsx`, `IND.csv`, and `USA.csv`.

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

The assessment PDF specifies an illustrative pipe-delimited member feed and nested JSON feed. The actual inputs profiled here are one Excel workbook and two CSVs; no JSON file is present. The source rows therefore require format-specific adapters and are not evidence that the illustrated flat-file contract has been delivered.

The PDF field table lists Member Name as mandatory and a key, and Member ID as mandatory but not a key, with length 18. Its examples contain shorter member IDs, so 18 is treated as a maximum length, not an exact count. The PDF's file-name/date-time design prompt (`YYYYMMDD`, `HHMMSSTT` or another format) is not a supplied source contract; `TT` and timezone are undefined.

Other discrepancies visible in the PDF are: Post Code appears in the field table but not in its illustrative header/detail or intermediate layout; the active field is named `Is_Active`, `Active Member`, and `FLAG` in different layouts; the intermediate table says `County` where the header says `Country`; and the intermediate DOB drops the leading zero from the detail value `03051985`. These are recorded without silently normalizing source content.

The PDF's illustrative flat-file Elena and JSON parent both use ID `223457`, while all three active local sources use local IDs `1`, `2`, and `3`. Those values repeat across files, and there is no source-supplied global identity or member update timestamp. The local PDF JSON example is illustrative only and does not join to the current data files.

Country is absent from each active row schema. Local country routing by `AUS.xlsx`, `IND.csv`, and `USA.csv` is a documented demo-only adapter with provenance, not verified member-country truth. Production routing requires the assessment row-level Country field or an approved mapping. Duplicate Member Name checks are scoped to a source-country dataset/batch; the PDF does not define that key scope, and names are never used to merge people across countries.

## Canonical schema proposal

The following is a proposal for review, not an approved contract. It represents source records without inventing a global person identifier or update timestamp.

| Field | Proposed type | Nullability / notes |
|---|---|---|
| `source_code` | string | Required intake namespace, e.g. `AUS`, `IND`, `USA`, taken from the source filename; confirm these are the intended source identifiers. |
| `source_file` | string | Required exact input filename. |
| `source_sheet` | string | Nullable; `Sheet1` for AUS, null for CSV files. |
| `source_row_number` | integer | Required source data row position for traceability; header excluded. Not a member ID. |
| `source_member_id_raw` | string | Required source-local ID as represented in the source. Do not treat it as globally unique or as a confirmed `global_member_id`. |
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

- The `AUS`, `IND`, and `USA` filenames are used for explicit local demonstration routing only; they do not, by themselves, prove member country.
- Slash-separated IND dates appear to use month/day/year, and the USA compact numbers appear intended to encode month/day/year. USA's two 7-digit values remain ambiguous even under this likely interpretation.
- The numeric IDs are local to each source because all three feeds independently use `1`–`3`; confirm before defining source-key constraints.
- The AUS workbook's displayed date format suggests `YYYY-MM-DD` for textual fallback, while native Excel date cells remain the primary representation. The fallback is an adapter assumption.
- The assessment PDF's example suggests `YYYYMMDD` for enrollment/flight and `MMDDYYYY` for DOB, but it does not define these as source contracts. The file-time token `TT` and timezone remain unresolved.

## Unresolved business questions

1. Does Member Name uniqueness apply per source-country dataset/batch, per country target, or across a complete flat-file feed? Until confirmed, report duplicates only in the documented scope and never merge by name.
2. Are IDs source-local, and is there an approved cross-source identity key or matching policy? What business update/effective timestamp orders member changes?
3. What is the authoritative USA date format, especially for `1052022` and `1152022`? Should unresolved mandatory values be quarantined and optional values remain null with an issue?
4. What do `TT` and the time portion of the filename represent, and what timezone applies to source-generated timestamps?
5. What are the meanings and valid mappings/order of `PLT`, `GLD`, and `SLV`?
6. What do IND's `I` and `C` mean, and should both be represented as members?
7. Are filename prefixes authoritative only for feed routing, and what country-code aliases are approved for production?
8. Is each flight date a member profile's last flight or a flight event? Can a member have multiple flights, and is an event identifier available?
9. Is `NULL` the only missing-value token, and are any approved name/date normalization rules required?
