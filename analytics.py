"""
analytics.py  —  Business Intelligence Queries on the Data Warehouse
=====================================================================
This is where we USE the warehouse. Each query answers a real business question.
These are exactly the kinds of queries you'll be asked to write in interviews.

Run AFTER setup_warehouse.py and load_data.py.
"""

import sqlite3
import pandas as pd

DB_FILE = "ecommerce_warehouse.db"

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 120)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

def run_query(conn, title, sql):
    print(f"\n{'='*65}")
    print(f"  {title}")
    print("="*65)
    df = pd.read_sql(sql, conn)
    print(df.to_string(index=False))

def main():
    conn = sqlite3.connect(DB_FILE)

    # ── Q1: Total revenue and profit by product category ──────
    run_query(conn, "Q1: Revenue and Profit by Category", """
        SELECT
            p.category,
            COUNT(DISTINCT f.order_id)          AS total_orders,
            SUM(f.quantity)                     AS units_sold,
            ROUND(SUM(f.net_revenue), 2)        AS net_revenue,
            ROUND(SUM(f.profit), 2)             AS total_profit,
            ROUND(AVG(f.discount_pct) * 100, 1) AS avg_discount_pct
        FROM fact_orders f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.category
        ORDER BY net_revenue DESC
    """)

    # ── Q2: Month-over-month revenue trend ────────────────────
    run_query(conn, "Q2: Monthly Revenue Trend (2023)", """
        SELECT
            d.month_name,
            d.month,
            ROUND(SUM(f.net_revenue), 2)  AS net_revenue,
            COUNT(DISTINCT f.order_id)    AS orders
        FROM fact_orders f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.month
    """)

    # ── Q3: Window function — rank products by revenue ────────
    run_query(conn, "Q3: Product Revenue Ranking (Window Function)", """
        SELECT
            product_name,
            category,
            ROUND(net_revenue, 2)  AS net_revenue,
            RANK() OVER (ORDER BY net_revenue DESC)           AS overall_rank,
            RANK() OVER (PARTITION BY category ORDER BY net_revenue DESC) AS rank_in_category
        FROM (
            SELECT
                p.product_name,
                p.category,
                SUM(f.net_revenue) AS net_revenue
            FROM fact_orders f
            JOIN dim_product p ON f.product_key = p.product_key
            GROUP BY p.product_name, p.category
        )
        ORDER BY overall_rank
        LIMIT 10
    """)

    # ── Q4: Running total revenue by month ────────────────────
    run_query(conn, "Q4: Cumulative Revenue by Month (Running Total)", """
        SELECT
            month_name,
            month,
            ROUND(monthly_revenue, 2) AS monthly_revenue,
            ROUND(SUM(monthly_revenue) OVER (ORDER BY month), 2) AS cumulative_revenue
        FROM (
            SELECT
                d.month_name,
                d.month,
                SUM(f.net_revenue) AS monthly_revenue
            FROM fact_orders f
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.month, d.month_name
        )
        ORDER BY month
    """)

    # ── Q5: Revenue by region and loyalty tier ────────────────
    run_query(conn, "Q5: Revenue by Region and Customer Loyalty Tier", """
        SELECT
            l.region,
            c.loyalty_tier,
            COUNT(DISTINCT f.order_id)    AS orders,
            ROUND(SUM(f.net_revenue), 2)  AS net_revenue
        FROM fact_orders f
        JOIN dim_location l ON f.location_key = l.location_key
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY l.region, c.loyalty_tier
        ORDER BY l.region, net_revenue DESC
    """)

    # ── Q6: Return rate by product ─────────────────────────────
    run_query(conn, "Q6: Return Rate by Product (Top 8 Highest)", """
        SELECT
            p.product_name,
            COUNT(*)                                          AS total_lines,
            SUM(f.is_returned)                               AS returned,
            ROUND(SUM(f.is_returned) * 100.0 / COUNT(*), 1) AS return_rate_pct
        FROM fact_orders f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.product_name
        ORDER BY return_rate_pct DESC
        LIMIT 8
    """)

    # ── Q7: Revenue lag — compare each month vs previous month
    run_query(conn, "Q7: Month-over-Month Revenue Change (LAG Function)", """
        SELECT
            month_name,
            month,
            ROUND(monthly_rev, 2)            AS revenue,
            ROUND(LAG(monthly_rev) OVER (ORDER BY month), 2) AS prev_month_rev,
            ROUND(monthly_rev - LAG(monthly_rev) OVER (ORDER BY month), 2) AS change
        FROM (
            SELECT
                d.month, d.month_name,
                SUM(f.net_revenue) AS monthly_rev
            FROM fact_orders f
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.month, d.month_name
        )
        ORDER BY month
    """)

    conn.close()
    print("\n" + "="*65)
    print("  All queries complete. Warehouse validated successfully.")
    print("="*65)

if __name__ == "__main__":
    main()
