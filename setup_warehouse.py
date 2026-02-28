"""
setup_warehouse.py  —  E-Commerce Analytics Data Warehouse
==========================================================
This script creates the star schema in SQLite:

  FACT TABLE:
    fact_orders  (the measurable events — one row per order line)

  DIMENSION TABLES:
    dim_customer   (who bought)
    dim_product    (what was bought)
    dim_date       (when it was bought)
    dim_location   (where it was shipped)

Run this ONCE before running load_data.py
"""

import sqlite3

DB_FILE = "ecommerce_warehouse.db"

SCHEMA = """
-- ─── DIMENSION: Customer ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key    INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id     TEXT    NOT NULL UNIQUE,
    full_name       TEXT    NOT NULL,
    email           TEXT,
    age_group       TEXT,       -- 18-24, 25-34, 35-44, 45+
    gender          TEXT,
    loyalty_tier    TEXT        -- Bronze, Silver, Gold, Platinum
);

-- ─── DIMENSION: Product ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_product (
    product_key     INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id      TEXT    NOT NULL UNIQUE,
    product_name    TEXT    NOT NULL,
    category        TEXT    NOT NULL,
    sub_category    TEXT,
    brand           TEXT,
    unit_cost       REAL    NOT NULL
);

-- ─── DIMENSION: Date ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        TEXT    PRIMARY KEY,    -- format: YYYY-MM-DD
    year            INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    month_name      TEXT    NOT NULL,
    week_of_year    INTEGER NOT NULL,
    day_of_month    INTEGER NOT NULL,
    day_name        TEXT    NOT NULL,
    is_weekend      INTEGER NOT NULL        -- 0 = weekday, 1 = weekend
);

-- ─── DIMENSION: Location ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_location (
    location_key    INTEGER PRIMARY KEY AUTOINCREMENT,
    city            TEXT    NOT NULL,
    state           TEXT    NOT NULL,
    region          TEXT    NOT NULL,       -- North, South, East, West
    pincode         TEXT
);

-- ─── FACT: Orders ─────────────────────────────────────────────
-- Each row = one product line within one order
CREATE TABLE IF NOT EXISTS fact_orders (
    order_line_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        TEXT    NOT NULL,
    customer_key    INTEGER REFERENCES dim_customer(customer_key),
    product_key     INTEGER REFERENCES dim_product(product_key),
    date_key        TEXT    REFERENCES dim_date(date_key),
    location_key    INTEGER REFERENCES dim_location(location_key),
    quantity        INTEGER NOT NULL,
    unit_price      REAL    NOT NULL,
    discount_pct    REAL    DEFAULT 0,
    gross_revenue   REAL    NOT NULL,       -- quantity * unit_price
    net_revenue     REAL    NOT NULL,       -- gross_revenue * (1 - discount)
    profit          REAL    NOT NULL,       -- net_revenue - (quantity * unit_cost)
    is_returned     INTEGER DEFAULT 0       -- 0 = kept, 1 = returned
);
"""

def setup():
    conn = sqlite3.connect(DB_FILE)
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()
    print(f"Star schema created in '{DB_FILE}'")
    print("Tables: fact_orders, dim_customer, dim_product, dim_date, dim_location")

if __name__ == "__main__":
    setup()
