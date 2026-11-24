-- Standalone Snowflake SQL regression fixture for review findings.
-- Uses synthetic parent payloads and date strings only. It does not read or
-- write supplied source data or production pipeline relations.

CREATE OR REPLACE TEMP TABLE REVIEW_PARENT_INPUTS AS
SELECT CASE_ID, RAW_PAYLOAD_TEXT, JSON_PARSE_ERROR,
       TRY_PARSE_JSON(RAW_PAYLOAD_TEXT) AS PAYLOAD
FROM VALUES
    ('VALID_EMPTY', '{"member_id":"SYN-1","feed_date":"20240115","redemptions":[]}', NULL),
    ('VALID_CHILD', '{"member_id":"SYN-2","feed_date":"20240115","redemptions":[{"txn_id":"SYN-TXN-1","txn_date":"20240114","partner":"Synthetic","miles_redeemed":1,"status":"TEST"}]}', NULL),
    ('MISSING_MEMBER', '{"feed_date":"20240115","redemptions":[{"txn_id":"SYN-TXN-2","txn_date":"20240114","partner":"Synthetic","miles_redeemed":1,"status":"TEST"}]}', NULL),
    ('INVALID_MEMBER_TYPE', '{"member_id":2,"feed_date":"20240115","redemptions":[{"txn_id":"SYN-TXN-4","txn_date":"20240114","partner":"Synthetic","miles_redeemed":1,"status":"TEST"}]}', NULL),
    ('INVALID_FEED_DATE', '{"member_id":"SYN-3","feed_date":"20241340","redemptions":[{"txn_id":"SYN-TXN-3","txn_date":"20240114","partner":"Synthetic","miles_redeemed":1,"status":"TEST"}]}', NULL),
    ('INVALID_TOP_LEVEL', '[{"member_id":"SYN-4","feed_date":"20240115","redemptions":[]}]', NULL),
    ('MISSING_ARRAY', '{"member_id":"SYN-5","feed_date":"20240115"}', NULL),
    ('NON_ARRAY', '{"member_id":"SYN-6","feed_date":"20240115","redemptions":{}}', NULL),
    ('MALFORMED_JSON', '{"member_id":', 'synthetic JSON parse error')
    AS INPUTS(CASE_ID, RAW_PAYLOAD_TEXT, JSON_PARSE_ERROR);

CREATE OR REPLACE TEMP TABLE REVIEW_PARENT_OUTCOMES AS
SELECT CASE_ID, RAW_PAYLOAD_TEXT, JSON_PARSE_ERROR, PAYLOAD,
       COALESCE(TYPEOF(PAYLOAD:"redemptions") = 'ARRAY', FALSE) AS HAS_ARRAY,
       IFF(TYPEOF(PAYLOAD:"redemptions") = 'ARRAY', ARRAY_SIZE(PAYLOAD:"redemptions"), 0) AS CHILD_COUNT,
       CASE WHEN JSON_PARSE_ERROR IS NOT NULL OR PAYLOAD IS NULL
                  OR TYPEOF(PAYLOAD) <> 'OBJECT'
                  OR NULLIF(TRIM(PAYLOAD:"member_id"::VARCHAR), '') IS NULL
                  OR TYPEOF(PAYLOAD:"member_id") <> 'VARCHAR'
                  OR TRY_TO_DATE(PAYLOAD:"feed_date"::VARCHAR, 'YYYYMMDD') IS NULL
                  OR NOT COALESCE(TYPEOF(PAYLOAD:"redemptions") = 'ARRAY', FALSE)
            THEN 'QUARANTINED' ELSE 'ACCEPTED' END AS DISPOSITION
FROM REVIEW_PARENT_INPUTS;

CREATE OR REPLACE TEMP TABLE REVIEW_CHILD_OUTCOMES AS
SELECT P.CASE_ID, F.INDEX + 1 AS CHILD_INDEX, P.DISPOSITION AS PARENT_DISPOSITION,
       F.VALUE AS RAW_CHILD,
       CASE WHEN P.DISPOSITION = 'QUARANTINED' THEN 'QUARANTINED'
            WHEN NULLIF(TRIM(F.VALUE:"txn_id"::VARCHAR), '') IS NULL
              OR TRY_TO_DATE(F.VALUE:"txn_date"::VARCHAR, 'YYYYMMDD') IS NULL
              OR NULLIF(TRIM(F.VALUE:"partner"::VARCHAR), '') IS NULL
              OR F.VALUE:"miles_redeemed" IS NULL
              OR NULLIF(TRIM(F.VALUE:"status"::VARCHAR), '') IS NULL
            THEN 'QUARANTINED' ELSE 'ACCEPTED' END AS DISPOSITION
FROM REVIEW_PARENT_OUTCOMES P,
     LATERAL FLATTEN(INPUT => P.PAYLOAD:"redemptions") F
WHERE P.HAS_ARRAY;

-- Each assertion column should be TRUE. Parent count includes empty/malformed cases;
-- only actual array elements create child rows, and quarantined parents never
-- produce a curated child even when their child object is otherwise valid.
SELECT
    (SELECT COUNT(*) FROM REVIEW_PARENT_OUTCOMES) = 9 AS ALL_PARENTS_RETAINED,
    (SELECT COUNT(*) FROM REVIEW_PARENT_OUTCOMES WHERE CASE_ID = 'VALID_EMPTY' AND CHILD_COUNT = 0) = 1
        AND (SELECT COUNT(*) FROM REVIEW_CHILD_OUTCOMES WHERE CASE_ID = 'VALID_EMPTY') = 0
        AS EMPTY_PARENT_HAS_NO_FAKE_CHILD,
    (SELECT COUNT(*) FROM REVIEW_PARENT_OUTCOMES WHERE CASE_ID IN ('INVALID_TOP_LEVEL', 'MISSING_ARRAY', 'NON_ARRAY', 'MALFORMED_JSON') AND DISPOSITION = 'QUARANTINED') = 4
        AS INVALID_PARENT_SHAPES_RETAINED,
    (SELECT COUNT(*) FROM REVIEW_CHILD_OUTCOMES WHERE CASE_ID IN ('MISSING_MEMBER', 'INVALID_MEMBER_TYPE', 'INVALID_FEED_DATE') AND DISPOSITION = 'QUARANTINED') = 3
        AS MISSING_OR_INVALID_PARENT_CHILDREN_QUARANTINED,
    (SELECT COUNT(*) FROM REVIEW_CHILD_OUTCOMES C JOIN REVIEW_PARENT_OUTCOMES P USING (CASE_ID)
      WHERE C.DISPOSITION <> 'QUARANTINED' AND P.DISPOSITION <> 'QUARANTINED') = 1
        AS ONLY_VALID_PARENT_CHILD_CURATED,
    (SELECT COUNT(*) FROM REVIEW_CHILD_OUTCOMES C JOIN REVIEW_PARENT_OUTCOMES P USING (CASE_ID)
      WHERE C.DISPOSITION <> 'QUARANTINED' AND P.DISPOSITION = 'ACCEPTED'
        AND NULLIF(TRIM(P.PAYLOAD:"member_id"::VARCHAR), '') IS NOT NULL) = 1
        AS ONLY_ELIGIBLE_CHILD_CAN_BE_UNMATCHED;

-- Mirrors source_dates._parse_compact_date: enumerate each legal 1/2-digit
-- month/day split, accept exactly one valid calendar date, leave ambiguity NULL.
WITH DATE_INPUTS AS (
    SELECT CASE_ID, DATE_TEXT
    FROM VALUES
        ('USA_UNIQUE_7', '6152022'),
        ('USA_AMBIGUOUS_7', '1052022'),
        ('USA_VALID_8', '12302021'),
        ('AUS_NORMALIZED_ISO', '2022-05-11') AS D(CASE_ID, DATE_TEXT)
), CANDIDATES AS (
    SELECT CASE_ID, DATE_TEXT,
           CASE
               WHEN CASE_ID = 'AUS_NORMALIZED_ISO' THEN TRY_TO_DATE(DATE_TEXT, 'YYYY-MM-DD')
               WHEN LENGTH(DATE_TEXT) = 8 THEN TRY_TO_DATE(DATE_TEXT, 'MMDDYYYY')
               WHEN LENGTH(DATE_TEXT) = 7 THEN TRY_TO_DATE(
                   LPAD(SUBSTR(DATE_TEXT, 1, 1), 2, '0') ||
                   LPAD(SUBSTR(DATE_TEXT, 2, 2), 2, '0') || SUBSTR(DATE_TEXT, 4, 4), 'MMDDYYYY')
           END AS CANDIDATE_1,
           CASE WHEN LENGTH(DATE_TEXT) = 7 THEN TRY_TO_DATE(
                   LPAD(SUBSTR(DATE_TEXT, 1, 2), 2, '0') ||
                   LPAD(SUBSTR(DATE_TEXT, 3, 1), 2, '0') || SUBSTR(DATE_TEXT, 4, 4), 'MMDDYYYY')
           END AS CANDIDATE_2
    FROM DATE_INPUTS
), PARSED AS (
    SELECT *,
           CASE WHEN CANDIDATE_1 IS NOT NULL AND CANDIDATE_2 IS NOT NULL
                     AND CANDIDATE_1 <> CANDIDATE_2 THEN NULL
                WHEN CANDIDATE_1 IS NOT NULL THEN CANDIDATE_1
                ELSE CANDIDATE_2 END AS PARSED_DATE,
           CASE WHEN CANDIDATE_1 IS NOT NULL AND CANDIDATE_2 IS NOT NULL
                     AND CANDIDATE_1 <> CANDIDATE_2 THEN 'AMBIGUOUS'
                WHEN CANDIDATE_1 IS NULL AND CANDIDATE_2 IS NULL THEN 'INVALID'
                ELSE 'VALID' END AS PARSE_STATUS
    FROM CANDIDATES
)
SELECT
    MAX(IFF(CASE_ID = 'USA_UNIQUE_7' AND PARSE_STATUS = 'VALID' AND PARSED_DATE = '2022-06-15'::DATE, 1, 0)) = 1
        AS USA_UNIQUE_SPLIT,
    MAX(IFF(CASE_ID = 'USA_AMBIGUOUS_7' AND PARSE_STATUS = 'AMBIGUOUS' AND PARSED_DATE IS NULL, 1, 0)) = 1
        AS USA_AMBIGUITY_PRESERVED,
    MAX(IFF(CASE_ID = 'USA_VALID_8' AND PARSE_STATUS = 'VALID' AND PARSED_DATE = '2021-12-30'::DATE, 1, 0)) = 1
        AS USA_EIGHT_DIGIT_PARSE,
    MAX(IFF(CASE_ID = 'AUS_NORMALIZED_ISO' AND PARSE_STATUS = 'VALID' AND PARSED_DATE = '2022-05-11'::DATE, 1, 0)) = 1
        AS AUS_ISO_NORMALIZATION
FROM PARSED;

-- Key-scope fixtures: same source-country batch spans artifacts; a separate
-- batch or country is intentionally outside the provisional name-key scope.
WITH MEMBER_FIXTURE AS (
    SELECT RUN_ID, BATCH_ID, SOURCE_CODE, ARTIFACT_ID, MEMBER_NAME
    FROM VALUES
        ('SYN-RUN', 'SYN-BATCH-1', 'AUS', 'SYN-A1', 'Synthetic Name'),
        ('SYN-RUN', 'SYN-BATCH-1', 'AUS', 'SYN-A2', 'Synthetic Name'),
        ('SYN-RUN', 'SYN-BATCH-2', 'AUS', 'SYN-A3', 'Synthetic Name'),
        ('SYN-RUN', 'SYN-BATCH-1', 'USA', 'SYN-U1', 'Synthetic Name')
        AS V(RUN_ID, BATCH_ID, SOURCE_CODE, ARTIFACT_ID, MEMBER_NAME)
), SCORED AS (
    SELECT *, COUNT(*) OVER (
        PARTITION BY RUN_ID, BATCH_ID, SOURCE_CODE, MEMBER_NAME
    ) AS NAME_COUNT
    FROM MEMBER_FIXTURE
)
SELECT
    COUNT_IF(BATCH_ID = 'SYN-BATCH-1' AND SOURCE_CODE = 'AUS' AND NAME_COUNT = 2) = 2
        AS SAME_BATCH_ACROSS_ARTIFACTS_DETECTED,
    COUNT_IF(BATCH_ID = 'SYN-BATCH-2' AND SOURCE_CODE = 'AUS' AND NAME_COUNT = 1) = 1
        AS OTHER_BATCH_ISOLATED,
    COUNT_IF(BATCH_ID = 'SYN-BATCH-1' AND SOURCE_CODE = 'USA' AND NAME_COUNT = 1) = 1
        AS OTHER_COUNTRY_ISOLATED
FROM SCORED;

-- Transaction ID validation is run + batch scoped across artifacts, not across
-- independent batches or runs.
WITH TXN_FIXTURE AS (
    SELECT RUN_ID, BATCH_ID, ARTIFACT_ID, TXN_ID
    FROM VALUES
        ('SYN-RUN', 'SYN-BATCH-1', 'SYN-R1', 'SYN-TXN-DUP'),
        ('SYN-RUN', 'SYN-BATCH-1', 'SYN-R2', 'SYN-TXN-DUP'),
        ('SYN-RUN', 'SYN-BATCH-2', 'SYN-R3', 'SYN-TXN-DUP'),
        ('SYN-RUN-2', 'SYN-BATCH-1', 'SYN-R4', 'SYN-TXN-DUP')
        AS V(RUN_ID, BATCH_ID, ARTIFACT_ID, TXN_ID)
), SCORED AS (
    SELECT *, COUNT(*) OVER (PARTITION BY RUN_ID, BATCH_ID, TXN_ID) AS TXN_ID_COUNT
    FROM TXN_FIXTURE
)
SELECT
    COUNT_IF(RUN_ID = 'SYN-RUN' AND BATCH_ID = 'SYN-BATCH-1' AND TXN_ID_COUNT = 2) = 2
        AS SAME_BATCH_ACROSS_ARTIFACTS_DUPLICATED,
    COUNT_IF(RUN_ID = 'SYN-RUN' AND BATCH_ID = 'SYN-BATCH-2' AND TXN_ID_COUNT = 1) = 1
        AS OTHER_BATCH_NOT_DUPLICATED,
    COUNT_IF(RUN_ID = 'SYN-RUN-2' AND BATCH_ID = 'SYN-BATCH-1' AND TXN_ID_COUNT = 1) = 1
        AS OTHER_RUN_NOT_DUPLICATED
FROM SCORED;
