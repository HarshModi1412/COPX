import os
import pandas as pd
import pyodbc
from datetime import datetime

# Remote SQL Server connection details
SERVER = "den1.mssql7.gear.host"
DATABASE = "billinghistory"
USERNAME = "billinghistory"
PASSWORD = "Pk0Z-57_avQe"  

def connect():
    """Establish connection to remote SQL Server database."""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD}"
    )
    return pyodbc.connect(conn_str)

def init_db():
    """Create tables if they do not exist (SQL Server syntax)."""
    conn = connect()
    cur = conn.cursor()

    # Customers
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='customers' AND xtype='U')
        CREATE TABLE customers (
            customer_id NVARCHAR(50) PRIMARY KEY,
            customer_number NVARCHAR(50) UNIQUE,
            customer_name NVARCHAR(255)
        )
    """)

    # Inventory
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='inventory' AND xtype='U')
        CREATE TABLE inventory (
            ingredient NVARCHAR(255) PRIMARY KEY,
            quantity FLOAT NOT NULL DEFAULT 0,
            unit NVARCHAR(50),
            safety_stock FLOAT DEFAULT 0
        )
    """)

    # Inventory Logs
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='inventory_logs' AND xtype='U')
        CREATE TABLE inventory_logs (
            log_id INT IDENTITY(1,1) PRIMARY KEY,
            ingredient NVARCHAR(255),
            change_type NVARCHAR(50),   -- Added / Wasted
            quantity_changed FLOAT,
            old_quantity FLOAT,
            new_quantity FLOAT,
            timestamp DATETIME DEFAULT GETDATE()
        )
    """)

    # Billing
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='billing' AND xtype='U')
        CREATE TABLE billing (
            invoice_id NVARCHAR(50),
            customer_id NVARCHAR(50),
            product_id NVARCHAR(50),
            product_name NVARCHAR(255),
            quantity INT,
            unit_price FLOAT,
            total FLOAT,
            timestamp NVARCHAR(50),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)

    # BOM
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='bom' AND xtype='U')
        CREATE TABLE bom (
            product_id NVARCHAR(50),
            ingredient NVARCHAR(255),
            qty_per_unit FLOAT,
            unit NVARCHAR(50),
            PRIMARY KEY (product_id, ingredient)
        )
    """)

    conn.commit()
    conn.close()

def query_db(query, params=(), fetch=False, many=False, seq=None, ignore_errors=False):
    """Execute a database query."""
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
    except Exception:
        if not ignore_errors:
            raise
        return None
    finally:
        conn.close()

def fetch_df(sql, params=()):
    """Fetch results into a Pandas DataFrame."""
    conn = connect()
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

def upsert_inventory_row(ingredient, unit):
    """Ensure inventory row exists or update its unit."""
    query_db("""
        MERGE inventory AS target
        USING (SELECT ? AS ingredient, ? AS unit) AS source
        ON target.ingredient = source.ingredient
        WHEN MATCHED THEN
            UPDATE SET unit = source.unit
        WHEN NOT MATCHED THEN
            INSERT (ingredient, quantity, unit, safety_stock) VALUES (source.ingredient, 0, source.unit, 0);
    """, (ingredient, unit))

def replace_inventory(df):
    """Replace inventory table with new data from a DataFrame."""
    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE inventory")

        for _, row in df.iterrows():
            cur.execute(
                "INSERT INTO inventory (ingredient, quantity, unit, safety_stock) VALUES (?, ?, ?, ?)",
                (row["ingredient"], row["quantity"], row["unit"], row.get("safety_stock", 0.0))
            )
        conn.commit()
    finally:
        conn.close()
