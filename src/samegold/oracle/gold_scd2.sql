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
    SELECT * FROM read_json($glob, format = 'newline_delimited',
                            ignore_errors = true, filename = true,
                            columns = {
                                'event_id': 'VARCHAR', 'event_type': 'VARCHAR',
                                'event_ts': 'VARCHAR', 'arrival_ts': 'VARCHAR',
                                'order_id': 'VARCHAR', 'customer_id': 'VARCHAR',
                                'sku': 'VARCHAR', 'qty': 'JSON', 'new_qty': 'JSON',
                                'unit_price_cents': 'JSON', 'currency': 'VARCHAR',
                                'return_id': 'VARCHAR', 'reason': 'VARCHAR',
                                'segment': 'VARCHAR', 'country': 'VARCHAR',
                                'boundary': 'VARCHAR'
                            })
),
arrived AS (
    SELECT * FROM raw
    WHERE event_type = 'customer_upserted'
      AND customer_id IS NOT NULL
      AND TRY_CAST(event_ts AS TIMESTAMPTZ) IS NOT NULL
      AND TRY_CAST(arrival_ts AS TIMESTAMPTZ) <= CAST($as_of AS TIMESTAMPTZ)
),
-- The same TOTAL order as the revenue reference, for the same reason: ordering only by
-- event_ts leaves ties, and for two copies of one customer_upserted the tie was decided by
-- the physical order of the rows. The tie-break hashes the payload columns that exist on
-- this event, with sha256 on both sides so the two engines break the tie the SAME way.
dedup AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT *, row_number() OVER (
                     PARTITION BY event_id
                     ORDER BY TRY_CAST(event_ts AS TIMESTAMPTZ),
                              TRY_CAST(arrival_ts AS TIMESTAMPTZ),
                              sha256(COALESCE(event_type, '') || '|' || COALESCE(customer_id, '')
                                  || '|' || COALESCE(segment, '') || '|' || COALESCE(country, ''))
                  ) AS rn
        FROM arrived
    ) WHERE rn = 1
),
-- Two events with the same valid_from for one customer: the later one wins. Without this
-- the dimension gets two rows with identical valid_from, the digest refuses to be taken
-- (no total order) and the failure is loud instead of silent.
collapsed AS (
    SELECT customer_id, valid_from, segment, country FROM (
        SELECT customer_id,
               TRY_CAST(event_ts AS TIMESTAMPTZ) AS valid_from,
               segment, country,
               row_number() OVER (PARTITION BY customer_id, CAST(event_ts AS TIMESTAMPTZ)
                                  ORDER BY event_id DESC) AS rn
        FROM dedup
    ) WHERE rn = 1
),
-- A Type 2 dimension records CHANGES. An upsert that repeats the attributes the customer
-- already had opens no new interval: it is a heartbeat, and a dimension that stores one row
-- per heartbeat is a log with extra columns. This CTE is the reference's derivation of the
-- rule that domain.bitemporal.scd2_from_versions states in Python; until an adversarial
-- review compared the three, only the Python one applied it and the three implementations
-- disagreed on any customer whose upsert repeated its segment and country.
changed AS (
    SELECT customer_id, valid_from, segment, country FROM (
        SELECT customer_id, valid_from, segment, country,
               LAG(segment) OVER w AS previous_segment,
               LAG(country) OVER w AS previous_country,
               LAG(valid_from) OVER w AS previous_valid_from
        FROM collapsed
        WINDOW w AS (PARTITION BY customer_id ORDER BY valid_from)
    )
    WHERE previous_valid_from IS NULL
       OR segment IS DISTINCT FROM previous_segment
       OR country IS DISTINCT FROM previous_country
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
FROM changed
ORDER BY customer_id, valid_from;
