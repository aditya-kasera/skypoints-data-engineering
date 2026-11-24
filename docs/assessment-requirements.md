# Incubyte Data Engineer Technical Assessment

## Source and handling

This document records the assessment brief in `reference/assessment.pdf`, which is the authority for assessment requirements. The PDF is kept locally as reference material and must not be committed to the public repository. Project-specific constraints supplied separately are identified as such below.

## Business problem

SkyPoints is a global airline loyalty program. Members receive cards that allow them to use partner lounges, redeem miles, and track tier status. Member data is maintained in one database and is to be distributed into country-specific tables.

The source system is described as sending two feeds daily: a flat file of member profile data and a semi-structured JSON feed of partner-airline redemption transactions. The assessment asks the design to account for billions of records per day across both feeds. This is a scale requirement for the proposed production design, not a performance result demonstrated by the supplied sample files.

## Flat-file layout

The illustrative header and detail records are:

```text
|H|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|Agent_Name|State|Country|DOB|Is_Active
|D|Elena|223457|20101012|20121013|GLD|Sam|CA|USA|03051985|A
```

The field table in the PDF specifies:

| Position | Field | Length | Type | Mandatory | Key |
|---:|---|---:|---|:---:|:---:|
| 1 | Member Name | 255 | VARCHAR | Yes | Yes |
| 2 | Member ID | 18 | VARCHAR | Yes | No |
| 3 | Enrollment Date | 8 | DATE | Yes | No |
| 4 | Last Flight Date | 8 | DATE | No | No |
| 5 | Tier Code | 5 | CHAR | No | No |
| 6 | Agent Name | 255 | CHAR | No | No |
| 7 | State | 5 | CHAR | No | No |
| 8 | Country | 5 | CHAR | No | No |
| 9 | Post Code | 5 | INT | No | No |
| 10 | Date of Birth | 8 | DATE | No | No |
| 11 | Active Member | 1 | CHAR | No | No |

The number 18 is the specified field length; it is not stated as an exact character count. The examples contain shorter IDs. The PDF marks Member Name as the key and Member ID as not a key; it does not define the scope of Member Name uniqueness.

The PDF asks the design documents to state a file-name string/extension and date/time format, giving `YYYYMMDD` and `HHMMSSTT` as examples or allowing another format. It does not define `TT`, timezone, or whether the filename time is an extraction time.

## Illustrative member records and intermediate layout

The PDF says the following five records are sample flat-file data, not a claim that they match every supplied source file:

| Name | Member ID | Enrollment | Last flight | Tier | Agent | State | Country | DOB as shown in intermediate table | Flag |
|---|---:|---|---|---|---|---|---|---|---|
| Elena | 223457 | 20101012 | 20121013 | GLD | Sam | CA | USA | 3051985 | A |
| Ravi | 223458 | 20101012 | 20121013 | SLV | *(blank)* | TN | IND | 3051985 | A |
| Mateo | 223459 | 20101012 | 20121013 | GLD | *(blank)* | NCR | PHIL | 3051985 | A |
| Nora | 22345 | 20101012 | 20121013 | PLT | *(blank)* | ONT | CAN | 3051985 | A |
| Jacob | 2256 | 20101012 | 20121013 | SLV | *(blank)* | VIC | AU | 3051985 | A |

The intermediate table heading is `Name, Mem_Id, Enroll_Dt, Flight_Dt, TIER, Agent_Name, State, County, DOB, FLAG`. It omits Post Code, calls the country-like field `County`, and its DOB example has lost the leading zero from the detail value `03051985`. Preserve raw values; do not copy that loss into canonical data.

## JSON redemption example

The PDF gives this illustrative nested payload:

```json
{
  "member_id": "223457",
  "feed_date": "20240115",
  "redemptions": [
    {
      "txn_id": "RX10091",
      "txn_date": "20240110",
      "partner": "AeroLink",
      "miles_redeemed": 12000,
      "status": "COMPLETED"
    },
    {
      "txn_id": "RX10092",
      "txn_date": "20240113",
      "partner": "SkyPoints",
      "miles_redeemed": 5000,
      "status": "PENDING"
    }
  ]
}
```

This member ID matches Elena's illustrative flat-file ID, but neither example is one of the current local CSV/XLSX rows. The JSON is not supplied as a local feed and may only be used as an explicitly labeled test fixture.

## Assessment deliverables

1. Create Snowflake table queries for raw/landing, staging, and country-specific target tables.
2. Add `Age` computed from DOB and `Stale_Member`, where days since `Flight_Date` is strictly greater than 90.
3. Write SQL and/or Python logic to distribute members to per-country target tables.
4. Parse nested redemption JSON into a flattened, queryable table and describe its join to member profiles.
5. Validate mandatory fields, key uniqueness, and anomalies visible in the examples.
6. Demonstrate the solution live if an interview proceeds.

## Engineering expectations

The PDF expects clarity and structured problem solving, engineering fundamentals and product thinking, thoughtful architecture, production-quality code and tests, intentional AI use with correctness, and incremental commits that show how the solution evolved.

## Reconciliation with the supplied assessment summary and current checkout

- The PDF is available locally at `reference/assessment.pdf`; the earlier statement that it was not stored is outdated. It remains excluded from the public repository.
- The source summary did not call out the PDF's file-name/date-time design-document request or the `TT` ambiguity; both are included here and in the architecture questions.
- The PDF specifies Member ID length 18 but does not require exactly 18 characters.
- The active local inputs are exactly `AUS.xlsx`, `IND.csv`, and `USA.csv`. Two duplicate format variants were removed. These local XLSX/CSV structures differ from the PDF's illustrative pipe-delimited feed; do not invent fields to make them appear identical.
- The source summary's public GitHub/GitLab submission requirement is project guidance, not a requirement stated in the PDF. The PDF does require incremental commits and a live demonstration.
- The PDF does not require a production cloud deployment. The production Snowflake design is a deliverable; deployment is out of scope unless separately requested.
