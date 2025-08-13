# billing_history.py
import os
from datetime import datetime
import streamlit as st
import pyodbc
import pandas as pd
import sqlite3

# =========================================================
# CONFIGURATION
# =========================================================
SERVER = st.secrets.get("MSSQL_SERVER", "den1.mssql7.gear.host")
DATABASE = st.secrets.get("MSSQL_DATABASE", "billinghistory")
USERNAME = st.secrets.get("MSSQL_USERNAME", "billinghistory")
PASSWORD = st.secrets.get("MSSQL_PASSWORD", "Pk0Z-57_avQe")
LOCAL_DB_FILE = "data/cafe_pos.db"

# =========================================================
# SQL SERVER CONNECTION
# =========================================================
def detect_sql_driver() -> str:
    """Detect the latest available SQL Server ODBC driver."""
    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    if not drivers:
        raise RuntimeError("No SQL Server ODBC drivers found.")
    return drivers[-1]

def get_sql_connection() -> pyodbc.Connection:
    """Connect to SQL Server."""
    driver = detect_sql_driver()
    conn_str = (
        f"Driver={{{driver}}};"
        f"Server={SERVER};"
        f"Database={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

# =========================================================
# DATABASE INITIALIZATION
# =========================================================
def ensure_tables_exist():
    """Ensure all required tables exist on SQL Server."""
    with get_sql_connection() as conn:
        cur = conn.cursor()

        cur.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='customers' AND xtype='U')
            CREATE TABLE customers (
                customer_id VARCHAR(255) PRIMARY KEY,
                customer_name VARCHAR(255),
                customer_number VARCHAR(255)
            );
        """)

        cur.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='inventory' AND xtype='U')
            CREATE TABLE inventory (
                ingredient VARCHAR(255) PRIMARY KEY,
                quantity DECIMAL(18,4) NOT NULL DEFAULT 0,
                unit VARCHAR(50)
            );
        """)

        cur.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='billing' AND xtype='U')
            CREATE TABLE billing (
                invoice_id VARCHAR(255),
                [timestamp] DATETIME,
                customer_id VARCHAR(255),
                product_id VARCHAR(255),
                product_name VARCHAR(255),
                quantity INT,
                unit_price DECIMAL(18,4),
                total DECIMAL(18,4),
                CONSTRAINT PK_billing PRIMARY KEY (invoice_id, product_id),
                CONSTRAINT FK_billing_customer
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            );
        """)

        conn.commit()

# =========================================================
# FETCH HELPERS
# =========================================================
def fetch_server_billing_df() -> pd.DataFrame:
    """Retrieve billing history from SQL Server."""
    query = """
        SELECT b.invoice_id, b.[timestamp], c.customer_id, c.customer_name, c.customer_number,
               b.product_id, b.product_name, b.quantity, b.unit_price, b.total
        FROM billing b
        LEFT JOIN customers c ON b.customer_id = c.customer_id
        ORDER BY b.[timestamp] DESC;
    """
    with get_sql_connection() as conn:
        return pd.read_sql(query, conn)

def fetch_existing_customer_ids() -> set:
    """Get set of all existing customer IDs."""
    with get_sql_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT customer_id FROM customers;")
        return {row[0] for row in cur.fetchall()}

# =========================================================
# UPSERT OPERATIONS
# =========================================================
def upsert_customers(df: pd.DataFrame) -> int:
    """Insert only new customers."""
    if df.empty or "customer_id" not in df:
        return 0

    existing_ids = fetch_existing_customer_ids()
    new_customers = df[~df["customer_id"].isin(existing_ids)]

    if new_customers.empty:
        return 0

    with get_sql_connection() as conn:
        cur = conn.cursor()
        for _, row in new_customers.iterrows():
            cur.execute("""
                INSERT INTO customers (customer_id, customer_name, customer_number)
                VALUES (?, ?, ?);
            """, (row["customer_id"], row.get("customer_name"), row.get("customer_number")))
        conn.commit()

    return len(new_customers)

def upsert_billing(df: pd.DataFrame):
    """Insert or update billing records."""
    if df.empty:
        return

    required_cols = ["invoice_id", "timestamp", "customer_id", "product_id", 
                     "product_name", "quantity", "unit_price", "total"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    with get_sql_connection() as conn:
        cur = conn.cursor()
        for _, r in df.drop_duplicates(subset=["invoice_id", "product_id"]).iterrows():
            cur.execute("""
                MERGE billing AS tgt
                USING (SELECT ? AS invoice_id, ? AS [timestamp], ? AS customer_id,
                              ? AS product_id, ? AS product_name, ? AS quantity,
                              ? AS unit_price, ? AS total) AS src
                ON tgt.invoice_id = src.invoice_id AND tgt.product_id = src.product_id
                WHEN MATCHED THEN UPDATE SET
                    [timestamp] = src.[timestamp],
                    customer_id = src.customer_id,
                    product_name = src.product_name,
                    quantity = src.quantity,
                    unit_price = src.unit_price,
                    total = src.total
                WHEN NOT MATCHED THEN INSERT
                    (invoice_id, [timestamp], customer_id, product_id, product_name, quantity, unit_price, total)
                    VALUES (src.invoice_id, src.[timestamp], src.customer_id, src.product_id, src.product_name, src.quantity, src.unit_price, src.total);
            """, (
                r["invoice_id"], r["timestamp"], r["customer_id"], r["product_id"],
                r.get("product_name"), int(r["quantity"]) if pd.notna(r["quantity"]) else None,
                float(r["unit_price"]) if pd.notna(r["unit_price"]) else None,
                float(r["total"]) if pd.notna(r["total"]) else None
            ))
        conn.commit()

def replace_inventory(items: list[dict]):
    """Replace inventory table content with new data."""
    with get_sql_connection() as conn:
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE inventory;")
        for it in items:
            cur.execute("""
                INSERT INTO inventory (ingredient, quantity, unit)
                VALUES (?, ?, ?);
            """, (it["ingredient"], float(it.get("quantity", 0)), it.get("unit")))
        conn.commit()

# =========================================================
# LOCAL SQLITE HELPERS
# =========================================================
def fetch_local_sqlite_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Read from local SQLite."""
    if not os.path.exists(LOCAL_DB_FILE):
        return pd.DataFrame()
    with sqlite3.connect(LOCAL_DB_FILE) as conn:
        return pd.read_sql_query(sql, conn, params=params)

def load_local_billing_snapshot() -> pd.DataFrame:
    """Get billing history from local SQLite."""
    sql = """
        SELECT b.invoice_id, b.timestamp, c.customer_id, c.customer_name, c.customer_number,
               b.product_id, b.product_name, b.quantity, b.unit_price, b.total
        FROM billing b
        LEFT JOIN customers c ON b.customer_id = c.customer_id
        ORDER BY b.timestamp DESC;
    """
    return fetch_local_sqlite_df(sql)

# =========================================================
# STREAMLIT PAGE
# =========================================================
def billing_history_page():
    st.title("🧾 Billing History (SQL Server)")

    try:
        ensure_tables_exist()
    except Exception as e:
        st.error(f"Failed to create required tables: {e}")
        return

    tabs = st.tabs(["📡 Live Server Data", "🔀 Sync from Local SQLite"])

    # --- TAB 1 ---
    with tabs[0]:
        try:
            df_server = fetch_server_billing_df()
            if df_server.empty:
                st.info("No billing records found on server.")
            else:
                st.dataframe(df_server, use_container_width=True)
                st.download_button(
                    "⬇ Download CSV",
                    df_server.to_csv(index=False).encode("utf-8"),
                    file_name=f"billing_history_{datetime.utcnow():%Y%m%d_%H%M%S}Z.csv",
                    mime="text/csv"
                )
        except Exception as e:
            st.error(f"Error fetching from SQL Server: {e}")

    # --- TAB 2 ---
    with tabs[1]:
        st.caption(f"Local DB: `{LOCAL_DB_FILE}`")
        if os.path.exists(LOCAL_DB_FILE):
            df_local = load_local_billing_snapshot()
            if df_local.empty:
                st.info("Local DB has no billing records.")
            else:
                st.success(f"Loaded {len(df_local)} local billing rows.")
                st.dataframe(df_local.head(100), use_container_width=True)

                sample_inventory = [
                    {"ingredient": "Hot Water", "quantity": 9850, "unit": "ml"},
                    {"ingredient": "Espresso Beans", "quantity": 928, "unit": "g"},
                    {"ingredient": "Milk", "quantity": 9850, "unit": "ml"},
                    {"ingredient": "Chocolate Syrup", "quantity": 1000, "unit": "g"}
                ]

                if st.button("📤 Sync Local → Server"):
                    try:
                        inserted_count = upsert_customers(
                            df_local[["customer_id", "customer_name", "customer_number"]].drop_duplicates()
                        )
                        upsert_billing(df_local)
                        replace_inventory(sample_inventory)
                        st.success(f"✅ Sync complete. {inserted_count} new customers inserted.")
                    except Exception as e:
                        st.error(f"❌ Sync failed: {e}")
        else:
            st.info("No local SQLite file found.")
