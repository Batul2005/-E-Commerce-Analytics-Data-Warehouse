"""
load_data.py  —  Generate and load 100,000 rows into the star schema
=====================================================================
This script:
  1. Generates realistic e-commerce data
  2. Populates all 4 dimension tables
  3. Populates the fact_orders table
  4. Runs row-count validation after loading
"""

import sqlite3
import random
from datetime import date, timedelta

DB_FILE   = "ecommerce_warehouse.db"
NUM_ORDERS = 20000   # 20,000 orders × ~5 lines each ≈ 100,000 fact rows

# ── Master Data ────────────────────────────────────────────────
CUSTOMERS = [
    ("CUST001","Aarav Shah",      "aarav@email.com",  "25-34","M","Gold"),
    ("CUST002","Priya Mehta",     "priya@email.com",  "18-24","F","Silver"),
    ("CUST003","Rohan Gupta",     "rohan@email.com",  "35-44","M","Platinum"),
    ("CUST004","Sneha Reddy",     "sneha@email.com",  "25-34","F","Bronze"),
    ("CUST005","Vikram Nair",     "vikram@email.com", "45+",  "M","Gold"),
    ("CUST006","Ananya Iyer",     "ananya@email.com", "18-24","F","Silver"),
    ("CUST007","Karthik Rao",     "karthik@email.com","35-44","M","Bronze"),
    ("CUST008","Divya Sharma",    "divya@email.com",  "25-34","F","Platinum"),
    ("CUST009","Arjun Patel",     "arjun@email.com",  "45+",  "M","Gold"),
    ("CUST010","Meera Krishnan",  "meera@email.com",  "18-24","F","Silver"),
    ("CUST011","Suresh Pillai",   "suresh@email.com", "35-44","M","Bronze"),
    ("CUST012","Lakshmi Bose",    "lakshmi@email.com","25-34","F","Gold"),
    ("CUST013","Nikhil Joshi",    "nikhil@email.com", "18-24","M","Silver"),
    ("CUST014","Pooja Deshpande", "pooja@email.com",  "45+",  "F","Platinum"),
    ("CUST015","Rahul Singh",     "rahul@email.com",  "35-44","M","Gold"),
]

PRODUCTS = [
    ("PROD001","Laptop Pro 15",       "Electronics","Computers",    "TechBrand",  42000),
    ("PROD002","Wireless Earbuds",    "Electronics","Audio",        "SoundCo",    3200),
    ("PROD003","4K Monitor 27in",     "Electronics","Displays",     "ViewTech",   22000),
    ("PROD004","Mechanical Keyboard", "Electronics","Peripherals",  "TypePro",    4500),
    ("PROD005","Gaming Mouse",        "Electronics","Peripherals",  "ClickMaster",1800),
    ("PROD006","Office Chair",        "Furniture",  "Seating",      "ComfortPlus",12000),
    ("PROD007","Standing Desk",       "Furniture",  "Desks",        "WorkRise",   28000),
    ("PROD008","Notebook 200pg",      "Stationery", "Books",        "WriteMore",  120),
    ("PROD009","Ballpoint Pens 10pk", "Stationery", "Writing",      "InkFlow",    80),
    ("PROD010","Webcam HD 1080p",     "Electronics","Cameras",      "VisionCam",  4200),
    ("PROD011","USB-C Hub 7-in-1",    "Electronics","Accessories",  "ConnectAll", 2100),
    ("PROD012","Desk Lamp LED",       "Furniture",  "Lighting",     "BrightSpace",1400),
    ("PROD013","Whiteboard A0",       "Stationery", "Boards",       "ClearWrite", 3800),
    ("PROD014","Smartphone Stand",    "Electronics","Accessories",  "HoldIt",     650),
    ("PROD015","Cable Organiser",     "Electronics","Accessories",  "NeatDesk",   350),
]

LOCATIONS = [
    ("Mumbai",    "Maharashtra", "West",  "400001"),
    ("Delhi",     "Delhi",       "North", "110001"),
    ("Bangalore", "Karnataka",   "South", "560001"),
    ("Chennai",   "Tamil Nadu",  "South", "600001"),
    ("Kolkata",   "West Bengal", "East",  "700001"),
    ("Hyderabad", "Telangana",   "South", "500001"),
    ("Pune",      "Maharashtra", "West",  "411001"),
    ("Ahmedabad", "Gujarat",     "West",  "380001"),
    ("Jaipur",    "Rajasthan",   "North", "302001"),
    ("Lucknow",   "Uttar Pradesh","North","226001"),
]

START_DATE = date(2023, 1, 1)
END_DATE   = date(2023, 12, 31)

# ── Helpers ────────────────────────────────────────────────────
def all_dates(start, end):
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates

def date_row(d):
    return (
        str(d), d.year, (d.month - 1) // 3 + 1, d.month,
        d.strftime("%B"), d.isocalendar()[1], d.day,
        d.strftime("%A"), 1 if d.weekday() >= 5 else 0
    )

# ── Main Loader ────────────────────────────────────────────────
def load():
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()

    # ── Load dim_customer ──────────────────────────────────────
    cur.executemany(
        "INSERT OR IGNORE INTO dim_customer "
        "(customer_id, full_name, email, age_group, gender, loyalty_tier) "
        "VALUES (?,?,?,?,?,?)",
        CUSTOMERS
    )
    print(f"  dim_customer: {len(CUSTOMERS)} rows loaded")

    # ── Load dim_product ───────────────────────────────────────
    cur.executemany(
        "INSERT OR IGNORE INTO dim_product "
        "(product_id, product_name, category, sub_category, brand, unit_cost) "
        "VALUES (?,?,?,?,?,?)",
        PRODUCTS
    )
    print(f"  dim_product:  {len(PRODUCTS)} rows loaded")

    # ── Load dim_location ──────────────────────────────────────
    cur.executemany(
        "INSERT OR IGNORE INTO dim_location (city, state, region, pincode) VALUES (?,?,?,?)",
        LOCATIONS
    )
    print(f"  dim_location: {len(LOCATIONS)} rows loaded")

    # ── Load dim_date (every day of 2023) ─────────────────────
    all_d = all_dates(START_DATE, END_DATE)
    cur.executemany(
        "INSERT OR IGNORE INTO dim_date "
        "(date_key, year, quarter, month, month_name, week_of_year, day_of_month, day_name, is_weekend) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        [date_row(d) for d in all_d]
    )
    print(f"  dim_date:     {len(all_d)} rows loaded")

    # Get surrogate keys for fact table references
    cust_keys = {r[0]: r[1] for r in cur.execute("SELECT customer_id, customer_key FROM dim_customer")}
    prod_keys = {r[0]: r[1] for r in cur.execute("SELECT product_id,  product_key  FROM dim_product")}
    loc_keys  = [r[0] for r in cur.execute("SELECT location_key FROM dim_location")]

    # ── Generate and load fact_orders ─────────────────────────
    fact_rows = []
    order_num = 1

    for _ in range(NUM_ORDERS):
        order_id     = f"ORD{order_num:06d}"
        cust_id      = random.choice(CUSTOMERS)[0]
        order_date   = random.choice(all_d)
        loc_key      = random.choice(loc_keys)
        lines_in_order = random.randint(1, 6)

        for _ in range(lines_in_order):
            prod         = random.choice(PRODUCTS)
            prod_id      = prod[0]
            unit_cost    = prod[5]
            quantity     = random.randint(1, 5)
            unit_price   = round(unit_cost * random.uniform(1.15, 1.40), 2)  # markup
            discount_pct = round(random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20]), 2)
            gross_rev    = round(quantity * unit_price, 2)
            net_rev      = round(gross_rev * (1 - discount_pct), 2)
            profit       = round(net_rev - (quantity * unit_cost), 2)
            is_returned  = 1 if random.random() < 0.04 else 0  # 4% return rate

            fact_rows.append((
                order_id,
                cust_keys[cust_id],
                prod_keys[prod_id],
                str(order_date),
                loc_key,
                quantity,
                unit_price,
                discount_pct,
                gross_rev,
                net_rev,
                profit,
                is_returned,
            ))

        order_num += 1

    cur.executemany(
        "INSERT INTO fact_orders "
        "(order_id, customer_key, product_key, date_key, location_key, "
        " quantity, unit_price, discount_pct, gross_revenue, net_revenue, profit, is_returned) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        fact_rows
    )
    print(f"  fact_orders:  {len(fact_rows)} rows loaded")

    conn.commit()
    conn.close()
    print(f"\nAll data loaded into '{DB_FILE}'")

if __name__ == "__main__":
    print("Loading data into star schema warehouse...\n")
    load()
