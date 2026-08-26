-- Seeds bronze.market_data.position_changes with synthetic Alpaca paper-account
-- position/order activity. The real paper account has no position history to
-- source a CDC feed from, so this stands in for it — enough to demonstrate the
-- Silver CDC pipeline's apply_changes pattern (SCD Type 1 upsert + delete) in
-- dlt_market_data_pipeline.py. Run once against any SQL warehouse.

CREATE TABLE IF NOT EXISTS bronze.market_data.position_changes (
  account_id STRING,
  symbol STRING,
  quantity DOUBLE,
  avg_cost DOUBLE,
  operation STRING,
  event_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

INSERT INTO bronze.market_data.position_changes
  (account_id, symbol, quantity, avg_cost, operation, event_timestamp)
VALUES
  ('PA3C0H606N6M', 'AAPL', 10, 225.50, 'UPSERT', TIMESTAMP'2026-08-20 14:31:00'),
  ('PA3C0H606N6M', 'MSFT', 5,  410.00, 'UPSERT', TIMESTAMP'2026-08-20 15:02:00'),
  ('PA3C0H606N6M', 'AAPL', 25, 226.80, 'UPSERT', TIMESTAMP'2026-08-21 10:15:00'),
  ('PA3C0H606N6M', 'TSLA', 8,  245.10, 'UPSERT', TIMESTAMP'2026-08-21 11:47:00'),
  ('PA3C0H606N6M', 'MSFT', 5,  410.00, 'UPSERT', TIMESTAMP'2026-08-22 09:35:00'),
  ('PA3C0H606N6M', 'AAPL', 15, 227.90, 'UPSERT', TIMESTAMP'2026-08-22 13:20:00'),
  ('PA3C0H606N6M', 'TSLA', 0,  0.0,    'DELETE', TIMESTAMP'2026-08-24 09:31:00'),
  ('PA3C0H606N6M', 'NVDA', 12, 118.25, 'UPSERT', TIMESTAMP'2026-08-24 10:05:00'),
  ('PA3C0H606N6M', 'MSFT', 0,  0.0,    'DELETE', TIMESTAMP'2026-08-25 15:50:00'),
  ('PA3C0H606N6M', 'NVDA', 20, 119.40, 'UPSERT', TIMESTAMP'2026-08-25 16:10:00');

-- Additional scenarios: a partial trim (GOOGL, quantity decreases via UPSERT
-- rather than a DELETE), a simple buy-and-hold (AMZN), and a reopen-after-close
-- (MSFT bought again after its DELETE above) — broader CDC coverage than the
-- original close/no-close-only scenario.
INSERT INTO bronze.market_data.position_changes
  (account_id, symbol, quantity, avg_cost, operation, event_timestamp)
VALUES
  ('PA3C0H606N6M', 'GOOGL', 30, 168.20, 'UPSERT', TIMESTAMP'2026-08-21 09:45:00'),
  ('PA3C0H606N6M', 'AMZN',  18, 178.50, 'UPSERT', TIMESTAMP'2026-08-22 11:00:00'),
  ('PA3C0H606N6M', 'GOOGL', 15, 169.00, 'UPSERT', TIMESTAMP'2026-08-23 14:10:00'),
  ('PA3C0H606N6M', 'MSFT',  10, 415.00, 'UPSERT', TIMESTAMP'2026-08-26 09:40:00');

-- Expected final state in silver.market_data.silver_positions_current after
-- the pipeline runs (keyed by account_id+symbol, sequenced by
-- event_timestamp): AAPL 15 @ 227.90, NVDA 20 @ 119.40, GOOGL 15 @ 169.00,
-- AMZN 18 @ 178.50, MSFT 10 @ 415.00 (reopened after its earlier DELETE).
-- TSLA remains absent (deleted, never reopened).
