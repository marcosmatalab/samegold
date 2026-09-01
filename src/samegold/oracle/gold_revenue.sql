-- Reference implementation of the close, in ANSI SQL over DuckDB.
--
-- A SECOND derivation of the contract by a different engine. It shares the contract (column
-- names, the 45-day window, the accounting timezone) with the Spark implementation and shares
-- no code with it. What it can and cannot catch is measured in mutation/witness_matrix.py.
--
-- Three details here exist because an adversarial review broke the previous version:
--
--  1. The columns are DECLARED, not inferred. With union_by_name, a batch that happens to
--     contain no order_line_amended event does not create the new_qty column, and the whole
--     close fails with "Binder Error: Referenced column new_qty not found". A pipeline whose
--     schema depends on which files arrived is not a pipeline.
--  2. The 45-day window is compared in SECONDS, not with INTERVAL 45 DAY. DuckDB's interval
--     arithmetic on TIMESTAMPTZ is calendar arithmetic in the session timezone, so under
--     Europe/Madrid the window is 44h23 or 45h01 long across a daylight-saving boundary,
--     while the Python rule uses an absolute timedelta. That mismatch produced a real
--     disagreement between the two implementations, on a real seed.
--  3. Deduplication has a TOTAL order. Ordering only by (event_ts, arrival_ts) leaves ties
--     undefined, and an undefined tie makes the answer depend on the physical order of rows
--     in the files. The tie is broken by a hash of the whole record.
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
                                'qty': 'BIGINT',
                                'new_qty': 'BIGINT',
                                'unit_price_cents': 'BIGINT',
                                'currency': 'VARCHAR',
                                'return_id': 'VARCHAR',
                                'reason': 'VARCHAR',
                                'segment': 'VARCHAR',
                                'country': 'VARCHAR',
                                'boundary': 'VARCHAR'
                            })
),
arrived AS (
    SELECT * FROM raw
    WHERE event_id IS NOT NULL
      AND arrival_ts IS NOT NULL
      AND CAST(arrival_ts AS TIMESTAMPTZ) <= CAST($as_of AS TIMESTAMPTZ)
),
dedup AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT *, row_number() OVER (
                     PARTITION BY event_id
                     ORDER BY CAST(event_ts AS TIMESTAMPTZ),
                              CAST(arrival_ts AS TIMESTAMPTZ),
                              md5(COALESCE(event_type, '') || '|' || COALESCE(order_id, '') || '|'
                                  || COALESCE(sku, '') || '|' || COALESCE(CAST(qty AS VARCHAR), '')
                                  || '|' || COALESCE(CAST(new_qty AS VARCHAR), '') || '|'
                                  || COALESCE(CAST(unit_price_cents AS VARCHAR), ''))
                  ) AS rn
        FROM arrived
    ) WHERE rn = 1
),
lines AS (
    SELECT order_id, customer_id, sku,
           qty AS qty0,
           unit_price_cents,
           CAST(event_ts AS TIMESTAMPTZ) AS sale_ts
    FROM dedup
    WHERE event_type = 'order_placed'
      AND order_id IS NOT NULL AND sku IS NOT NULL AND customer_id IS NOT NULL
      AND qty > 0
      AND unit_price_cents >= 0
      AND currency = 'EUR'
),
amendments AS (
    SELECT order_id, sku, qty FROM (
        SELECT order_id, sku, new_qty AS qty,
               row_number() OVER (PARTITION BY order_id, sku
                                  ORDER BY CAST(event_ts AS TIMESTAMPTZ) DESC, event_id DESC) AS rn
        FROM dedup WHERE event_type = 'order_line_amended' AND new_qty IS NOT NULL
    ) WHERE rn = 1
),
effective AS (
    SELECT l.order_id, l.customer_id, l.sku, l.unit_price_cents, l.sale_ts,
           COALESCE(a.qty, l.qty0) AS qty
    FROM lines l LEFT JOIN amendments a ON a.order_id = l.order_id AND a.sku = l.sku
),
return_candidates AS (
    SELECT d.order_id, d.sku, d.qty AS return_qty,
           CAST(d.event_ts AS TIMESTAMPTZ) AS return_ts,
           e.sale_ts, e.unit_price_cents, e.qty AS sold_qty
    FROM dedup d
    LEFT JOIN effective e ON e.order_id = d.order_id AND e.sku = d.sku
    WHERE d.event_type = 'return_registered' AND d.qty IS NOT NULL AND d.qty > 0
),
returns_classified AS (
    SELECT *,
           CASE
               WHEN sale_ts IS NULL THEN 'return_without_order'
               WHEN return_ts < sale_ts THEN 'return_outside_window'
               WHEN epoch(return_ts) - epoch(sale_ts) > 45 * 86400 THEN 'return_outside_window'
               WHEN return_qty > sold_qty THEN 'return_exceeds_sold_qty'
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
