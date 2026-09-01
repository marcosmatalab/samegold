-- Reference implementation of gold.revenue_by_month, in ANSI SQL over DuckDB.
--
-- This is a SECOND derivation of the contract by a different engine. It shares the
-- contract (column names, the 45-day window, the accounting timezone) with the Spark
-- implementation and shares NO code with it. What it can and cannot catch is measured in
-- mutation/witness_matrix.py rather than asserted here; the short version is that it
-- catches implementation mistakes in Spark and is blind, by construction, to a
-- misunderstanding of the contract that both implementations inherit.
--
-- Parameters: $glob (bronze files), $as_of (ISO instant of the close being reproduced).

WITH raw AS (
    SELECT * FROM read_json($glob,
                            format = 'newline_delimited',
                            union_by_name = true,
                            ignore_errors = true,
                            filename = true,
                            maximum_object_size = 1048576)
),
-- As-of cut. Everything downstream sees only what had arrived by the close instant.
arrived AS (
    SELECT * FROM raw
    WHERE event_id IS NOT NULL
      AND arrival_ts IS NOT NULL
      AND CAST(arrival_ts AS TIMESTAMPTZ) <= CAST($as_of AS TIMESTAMPTZ)
),
-- Deduplication by the producer's idempotency key. The file path is deliberately not part
-- of the key: identical content re-delivered under a new path must be a no-op.
dedup AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT *, row_number() OVER (PARTITION BY event_id
                                     ORDER BY CAST(event_ts AS TIMESTAMPTZ),
                                              CAST(arrival_ts AS TIMESTAMPTZ)) AS rn
        FROM arrived
    ) WHERE rn = 1
),
lines AS (
    SELECT order_id, customer_id, sku,
           CAST(qty AS BIGINT) AS qty0,
           CAST(unit_price_cents AS BIGINT) AS unit_price_cents,
           CAST(event_ts AS TIMESTAMPTZ) AS sale_ts
    FROM dedup
    WHERE event_type = 'order_placed'
      AND order_id IS NOT NULL AND sku IS NOT NULL AND customer_id IS NOT NULL
      AND CAST(qty AS BIGINT) > 0
      AND CAST(unit_price_cents AS BIGINT) >= 0
      AND currency = 'EUR'
),
amendments AS (
    SELECT order_id, sku, qty FROM (
        SELECT order_id, sku, CAST(new_qty AS BIGINT) AS qty,
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
returns AS (
    SELECT e.sale_ts, e.unit_price_cents, CAST(d.qty AS BIGINT) AS qty,
           CAST(d.event_ts AS TIMESTAMPTZ) AS return_ts
    FROM dedup d
    JOIN effective e ON e.order_id = d.order_id AND e.sku = d.sku
    WHERE d.event_type = 'return_registered'
      AND CAST(d.qty AS BIGINT) > 0
      AND CAST(d.event_ts AS TIMESTAMPTZ) >= e.sale_ts
      AND CAST(d.event_ts AS TIMESTAMPTZ) <= e.sale_ts + INTERVAL 45 DAY
      AND CAST(d.qty AS BIGINT) <= e.qty
),
gross AS (
    SELECT strftime(sale_ts AT TIME ZONE 'Europe/Madrid', '%Y-%m') AS accounting_month,
           SUM(qty * unit_price_cents) AS gross_cents,
           COUNT(*)                     AS line_count
    FROM effective GROUP BY 1
),
refunds AS (
    SELECT strftime(sale_ts AT TIME ZONE 'Europe/Madrid', '%Y-%m') AS accounting_month,
           SUM(qty * unit_price_cents) AS returns_cents,
           COUNT(*)                     AS return_count
    FROM returns GROUP BY 1
)
SELECT COALESCE(g.accounting_month, r.accounting_month) AS accounting_month,
       COALESCE(g.gross_cents, 0)                        AS gross_cents,
       COALESCE(r.returns_cents, 0)                      AS returns_cents,
       COALESCE(g.gross_cents, 0) - COALESCE(r.returns_cents, 0) AS net_cents,
       COALESCE(g.line_count, 0)                         AS line_count,
       COALESCE(r.return_count, 0)                       AS return_count
FROM gross g FULL OUTER JOIN refunds r ON g.accounting_month = r.accounting_month
ORDER BY 1;
