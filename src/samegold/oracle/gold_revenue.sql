-- Reference implementation of the close, in ANSI SQL over DuckDB.
--
-- A SECOND derivation of the contract by a different engine. It shares the contract (column
-- names, the 45-day window, the accounting timezone) with the Spark implementation and shares
-- no code with it. What it can and cannot catch is measured in mutation/witness_matrix.py.
--
-- Six details here exist because an adversarial review broke the previous version:
--
--  1. The columns are DECLARED, not inferred. With union_by_name, a batch that happens to
--     contain no order_line_amended event does not create the new_qty column, and the whole
--     close fails with "Binder Error: Referenced column new_qty not found". A pipeline whose
--     schema depends on which files arrived is not a pipeline.
--  2. The 45-day window is compared in SECONDS, not with INTERVAL 45 DAY. DuckDB's interval
--     arithmetic on TIMESTAMPTZ is calendar arithmetic in the session timezone, so under
--     Europe/Madrid the window is an hour short of or an hour past 45 days across a daylight-saving boundary,
--     while the Python rule uses an absolute timedelta. That mismatch produced a real
--     disagreement between the two implementations, on a real seed.
--  3. Deduplication has a TOTAL order. Ordering only by (event_ts, arrival_ts) leaves ties
--     undefined, and an undefined tie makes the answer depend on the physical order of rows
--     in the files. The tie is broken by a hash of the whole payload.
--     Two properties of that hash were wrong until an adversarial review measured them.
--     It used md5 here and sha2(...,256) in Spark: different functions induce different
--     lexicographic orders, so on a pair of rows sharing an event_id with different payloads
--     the two engines picked DIFFERENT copies, on 48% of such pairs. And it covered six
--     fields, so for a customer_upserted (where all six are NULL) both copies hashed to the
--     same value and the winner was decided by the shuffle. The function is now sha256 on
--     both sides, over EVERY payload column. The generator never emits a colliding pair, so
--     nothing but a deliberate test would ever have noticed.
--
--  5. The three integer columns are read as JSON, not as BIGINT, and converted in the
--     `typed` CTE below. DuckDB's BIGINT coercion accepts `"qty": 2.0` and `"qty": "2"`;
--     Spark's declared LongType under PERMISSIVE mode nulls both and rescues the record.
--     So a producer sending a float or a quoted number booked revenue in one engine and was
--     quarantined in the other - a divergence no seed could produce, because the generator
--     writes the types the contract asks for. `json_type` is what distinguishes an integer
--     from a float from a string, which is exactly the distinction Spark's schema makes.
--     The conversion is TRY_CAST, not CAST: json_type reports 2^63 as UBIGINT, which the
--     guard admits and a plain CAST then fails on, aborting the whole close - the very
--     "record with no door" this file removed two paragraphs above, reintroduced by its own
--     fix. Spark's declared LongType nulls the same value.
--
--  6. A timestamp is converted ONCE, in the `stamped` CTE, and only if it matches an ISO
--     SHAPE. TRY_CAST alone is too generous: DuckDB accepts the keywords 'epoch' (which
--     becomes 1970-01-01) and 'infinity', both of which Spark's try_to_timestamp rejects. A
--     single record stamped "infinity" produced an accounting month literally called
--     "infinity"; two copies of one event_id, one of them "epoch", had different winners in
--     the two engines even after NULLS LAST. The regex is the set of spellings the two
--     engines agree on, and tests/spark checks the list case by case.
--
--  4. Timestamps are TRY_CAST, not CAST. A single malformed event_ts used to abort the whole
--     close with a conversion error, in both engines: the one record shape for which the
--     pipeline had no door at all. It is now a NULL timestamp, which the filters below
--     exclude, and which the Spark side counts as missing_required_field.
--
-- Parameters: $glob (bronze files), $as_of (ISO instant of the close being reproduced).

WITH raw AS (
    SELECT * FROM read_json($glob,
                            format = 'newline_delimited',
                            ignore_errors = true,
                            filename = true,
                            columns = {
                                'event_id': 'VARCHAR',
                                'event_type': 'VARCHAR',
                                'event_ts': 'VARCHAR',
                                'arrival_ts': 'VARCHAR',
                                'order_id': 'VARCHAR',
                                'customer_id': 'VARCHAR',
                                'sku': 'VARCHAR',
                                'qty': 'JSON',
                                'new_qty': 'JSON',
                                'unit_price_cents': 'JSON',
                                'currency': 'VARCHAR',
                                'return_id': 'VARCHAR',
                                'reason': 'VARCHAR',
                                'segment': 'VARCHAR',
                                'country': 'VARCHAR',
                                'boundary': 'VARCHAR'
                            })
),
typed AS (
    SELECT * EXCLUDE (qty, new_qty, unit_price_cents),
           CASE WHEN json_type(qty) IN ('BIGINT', 'UBIGINT')
                THEN TRY_CAST(qty AS BIGINT) END AS qty,
           CASE WHEN json_type(new_qty) IN ('BIGINT', 'UBIGINT')
                THEN TRY_CAST(new_qty AS BIGINT) END AS new_qty,
           CASE WHEN json_type(unit_price_cents) IN ('BIGINT', 'UBIGINT')
                THEN TRY_CAST(unit_price_cents AS BIGINT) END AS unit_price_cents
    FROM raw
),
stamped AS (
    SELECT * EXCLUDE (event_ts, arrival_ts),
           CASE WHEN regexp_full_match(event_ts,
                    '\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?\s*(Z|[+-]\d{2}:?\d{2})?')
                THEN TRY_CAST(event_ts AS TIMESTAMPTZ) END AS event_ts,
           CASE WHEN regexp_full_match(arrival_ts,
                    '\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?\s*(Z|[+-]\d{2}:?\d{2})?')
                THEN TRY_CAST(arrival_ts AS TIMESTAMPTZ) END AS arrival_ts
    FROM typed
),
arrived AS (
    SELECT * FROM stamped
    WHERE event_id IS NOT NULL
      AND arrival_ts IS NOT NULL
      AND event_ts IS NOT NULL
      AND arrival_ts <= CAST($as_of AS TIMESTAMPTZ)
),
dedup AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT *, row_number() OVER (
                     PARTITION BY event_id
                     ORDER BY event_ts, arrival_ts,
                              sha256(COALESCE(event_type, '') || '|' || COALESCE(order_id, '') || '|'
                                  || COALESCE(customer_id, '') || '|' || COALESCE(sku, '') || '|'
                                  || COALESCE(CAST(qty AS VARCHAR), '') || '|'
                                  || COALESCE(CAST(new_qty AS VARCHAR), '') || '|'
                                  || COALESCE(CAST(unit_price_cents AS VARCHAR), '') || '|'
                                  || COALESCE(currency, '') || '|' || COALESCE(return_id, '') || '|'
                                  || COALESCE(reason, '') || '|' || COALESCE(segment, '') || '|'
                                  || COALESCE(country, ''))
                  ) AS rn
        FROM arrived
    ) WHERE rn = 1
),
lines AS (
    SELECT order_id, customer_id, sku,
           qty AS qty0,
           unit_price_cents,
           event_ts AS sale_ts
    FROM dedup
    WHERE event_type = 'order_placed'
      AND order_id IS NOT NULL AND sku IS NOT NULL AND customer_id IS NOT NULL
      AND qty > 0
      AND unit_price_cents IS NOT NULL AND unit_price_cents >= 0
      AND currency = 'EUR'
),
amendments AS (
    SELECT order_id, sku, qty FROM (
        SELECT order_id, sku, new_qty AS qty,
               row_number() OVER (PARTITION BY order_id, sku
                                  ORDER BY event_ts DESC, event_id DESC) AS rn
        FROM dedup WHERE event_type = 'order_line_amended' AND new_qty IS NOT NULL
                     AND new_qty > 0
                     AND order_id IS NOT NULL AND sku IS NOT NULL
    ) WHERE rn = 1
),
effective AS (
    SELECT l.order_id, l.customer_id, l.sku, l.unit_price_cents, l.sale_ts,
           COALESCE(a.qty, l.qty0) AS qty
    FROM lines l LEFT JOIN amendments a ON a.order_id = l.order_id AND a.sku = l.sku
),
return_candidates AS (
    SELECT d.order_id, d.sku, d.qty AS return_qty,
           d.event_ts AS return_ts,
           e.sale_ts, e.unit_price_cents, e.qty AS sold_qty,
           -- CUMULATIVE, not per event. Comparing each return against the quantity sold let
           -- three returns of three units each be accepted against one sale of three: gross
           -- 3000, refunds 9000, net MINUS 6000, and returns_rejected_count zero. Both
           -- engines agreed on it, so the parity claim was blind, and the generator never
           -- emits a second return for a line, so no seed reached it. The window is ordered
           -- by (return_ts, event_id), which is the same total order the Spark side uses.
           SUM(d.qty) OVER (PARTITION BY d.order_id, d.sku
                            ORDER BY d.event_ts, d.event_id
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
               AS returned_including_this
    FROM dedup d
    LEFT JOIN effective e ON e.order_id = d.order_id AND e.sku = d.sku
    WHERE d.event_type = 'return_registered' AND d.qty IS NOT NULL AND d.qty > 0
      AND d.order_id IS NOT NULL AND d.sku IS NOT NULL
),
returns_classified AS (
    SELECT *,
           CASE
               WHEN sale_ts IS NULL THEN 'return_without_order'
               WHEN return_ts < sale_ts THEN 'return_outside_window'
               WHEN epoch(return_ts) - epoch(sale_ts) > 45 * 86400 THEN 'return_outside_window'
               WHEN returned_including_this > sold_qty THEN 'return_exceeds_sold_qty'
               ELSE 'accepted'
           END AS return_reason
    FROM return_candidates
),
returns AS (SELECT * FROM returns_classified WHERE return_reason = 'accepted'),
-- Rejected returns are reported, not discarded. Finance asks how many refunds were refused
-- and why, and a number that leaves the pipeline without a counter is the failure nobody
-- detects. It also makes the rejection rules observable in gold, so a mutation that widens
-- or narrows them changes a published figure instead of disappearing quietly.
rejected AS (
    SELECT strftime(sale_ts AT TIME ZONE 'Europe/Madrid', '%Y-%m') AS accounting_month,
           COUNT(*) AS returns_rejected_count
    FROM returns_classified
    WHERE return_reason <> 'accepted' AND sale_ts IS NOT NULL
    GROUP BY 1
),
gross AS (
    SELECT strftime(sale_ts AT TIME ZONE 'Europe/Madrid', '%Y-%m') AS accounting_month,
           SUM(qty * unit_price_cents) AS gross_cents,
           COUNT(*)                     AS line_count
    FROM effective GROUP BY 1
),
refunds AS (
    SELECT strftime(sale_ts AT TIME ZONE 'Europe/Madrid', '%Y-%m') AS accounting_month,
           SUM(return_qty * unit_price_cents) AS returns_cents,
           COUNT(*)                           AS return_count
    FROM returns GROUP BY 1
)
SELECT COALESCE(g.accounting_month, r.accounting_month) AS accounting_month,
       COALESCE(g.gross_cents, 0)                        AS gross_cents,
       COALESCE(r.returns_cents, 0)                      AS returns_cents,
       COALESCE(g.gross_cents, 0) - COALESCE(r.returns_cents, 0) AS net_cents,
       COALESCE(g.line_count, 0)                         AS line_count,
       COALESCE(r.return_count, 0)                       AS return_count,
       COALESCE(x.returns_rejected_count, 0)             AS returns_rejected_count
FROM gross g
FULL OUTER JOIN refunds r ON g.accounting_month = r.accounting_month
LEFT JOIN rejected x ON x.accounting_month = COALESCE(g.accounting_month, r.accounting_month)
ORDER BY 1;
