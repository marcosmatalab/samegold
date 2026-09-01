-- Reference implementation of gold.dim_customer_scd2, in DuckDB SQL.
--
-- Type 2 built with a window function rather than a MERGE: on the reference side there is
-- no incremental state to maintain, so the whole dimension is recomputed from the event
-- history. That is the point of having a reference - the Spark side has to get the same
-- answer while doing it incrementally with a MERGE, which is where the interesting bugs are
-- (a late-arriving version, a version that closes an interval it should have split, an
-- is_current flag left on two rows).
--
-- Parameters: $glob, $as_of

WITH raw AS (
    SELECT * FROM read_json($glob, format = 'newline_delimited', union_by_name = true,
                            ignore_errors = true, filename = true)
),
arrived AS (
    SELECT * FROM raw
    WHERE event_type = 'customer_upserted'
      AND customer_id IS NOT NULL
      AND CAST(arrival_ts AS TIMESTAMPTZ) <= CAST($as_of AS TIMESTAMPTZ)
),
dedup AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY CAST(event_ts AS TIMESTAMPTZ)) AS rn
        FROM arrived
    ) WHERE rn = 1
),
-- Two events with the same valid_from for one customer: the later one wins. Without this
-- the dimension gets two rows with identical valid_from, the digest refuses to be taken
-- (no total order) and the failure is loud instead of silent.
collapsed AS (
    SELECT customer_id, valid_from, segment, country FROM (
        SELECT customer_id,
               CAST(event_ts AS TIMESTAMPTZ) AS valid_from,
               segment, country,
               row_number() OVER (PARTITION BY customer_id, CAST(event_ts AS TIMESTAMPTZ)
                                  ORDER BY event_id DESC) AS rn
        FROM dedup
    ) WHERE rn = 1
)
-- Timestamps leave as canonical UTC strings on purpose. Two reasons, both learned the hard
-- way: returning TIMESTAMPTZ to Python makes DuckDB import pytz (an undeclared dependency
-- that fails at runtime, not at install time), and a timestamp rendered by the engine is
-- one more thing that can differ between engines inside a digest. The contract for this
-- table is a string in UTC with microsecond precision, and both implementations produce it.
SELECT customer_id,
       strftime(valid_from AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%fZ') AS valid_from,
       strftime(LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from)
                AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%fZ')            AS valid_to,
       segment,
       country,
       LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) IS NULL AS is_current
FROM collapsed
ORDER BY customer_id, valid_from;
