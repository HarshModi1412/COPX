import sqlite3
import os
import pandas as pd

DB_FILE = "data/cafe_pos.db"
os.makedirs("data", exist_ok=True)

def connect():
    return sqlite3.connect(DB_FILE)

def init_db():
    conn = connect()
    cur = conn.cursor()

    # Customers
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            customer_number TEXT UNIQUE,
            customer_name TEXT
        )
    """)

    # Inventory (raw materials)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory (
            ingredient TEXT PRIMARY KEY,
            quantity REAL NOT NULL DEFAULT 0,
            unit TEXT
        )
    """)

    # Billing (invoice line items)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS billing (
            invoice_id TEXT,
            customer_id TEXT,
            product_id TEXT,
            product_name TEXT,
            quantity INTEGER,
            unit_price REAL,
            total REAL,
            timestamp TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)

    # BOM (Bill of Materials) — per product, per ingredient
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bom (
            product_id TEXT,
            ingredient TEXT,
            qty_per_unit REAL,
            unit TEXT,
            PRIMARY KEY (product_id, ingredient)
        )
    """)

    conn.commit()
    conn.close()

def query_db(query, params=(), fetch=False, many=False, seq=None, ignore_errors=False):
    """Execute a database query with optional error ignoring."""
    conn = connect()
    cur = conn.cursor()
    try:
        if many and seq is not None:
            cur.executemany(query, seq)
        else:
            cur.execute(query, params)
        rows = cur.fetchall() if fetch else None
        conn.commit()
        return rows
    except Exception as e:
        if not ignore_errors:
            raise
        # If ignoring errors, just skip and return None
        return None
    finally:
        conn.close()

def fetch_df(sql, params=()):
    """Fetch query results directly as a pandas DataFrame."""
    conn = connect()
    try:
        df = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()
    return df

def upsert_inventory_row(ingredient, unit):
    """Ensure inventory row exists or update its unit."""
    query_db("""
        INSERT INTO inventory (ingredient, quantity, unit)
        VALUES (?, 0, ?)
        ON CONFLICT(ingredient) DO UPDATE SET unit=excluded.unit
    """, (ingredient, unit))
