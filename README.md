# Project 2: E-Commerce Analytics Data Warehouse

## What This Project Does
Designs and implements a full star schema data warehouse from scratch for an
e-commerce dataset with 100,000+ rows, then runs advanced SQL analytical queries.

## Star Schema Design
```
                    dim_customer
                         |
dim_product ── fact_orders ── dim_location
                         |
                      dim_date
```

## Skills Demonstrated
- Star schema design (fact + 4 dimension tables)
- Foreign key relationships using surrogate keys
- Advanced SQL: window functions (RANK, LAG, running totals), multi-table JOINs
- Business intelligence queries: revenue trends, return rates, regional analysis
- Python: sqlite3, pandas, data generation at scale

## Resume Bullet Points (copy these)
- Designed and implemented a star schema data warehouse (fact_orders + 4 dimension
  tables) in SQLite, loading 100,000+ rows of simulated e-commerce transaction data
- Wrote 7 advanced analytical SQL queries using window functions (RANK, LAG, running
  totals) to surface insights on revenue trends, product performance, and return rates
- Implemented surrogate key relationships between fact and dimension tables, mirroring
  production data warehouse patterns used in Redshift and BigQuery

## How to Run (Step by Step)

### Step 1 — Install dependencies
```
pip install pandas
```

### Step 2 — Create the star schema
```
python setup_warehouse.py
```
Creates 5 tables in ecommerce_warehouse.db

### Step 3 — Load 100,000+ rows of data
```
python load_data.py
```
Populates all dimension tables, then generates and loads fact rows.

### Step 4 — Run the analytics queries
```
python analytics.py
```
Runs 7 business intelligence queries. Study every one — these are real interview questions.

### Step 5 — Explore in DB Browser for SQLite
Open ecommerce_warehouse.db and browse the schema visually.

## Key SQL Patterns Used
- RANK() OVER (PARTITION BY category ORDER BY revenue DESC)
- LAG(revenue) OVER (ORDER BY month)
- SUM(revenue) OVER (ORDER BY month) — running total
- Multi-table JOINs across all 5 tables
- Subqueries in FROM clause
