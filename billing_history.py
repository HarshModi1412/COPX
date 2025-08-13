# billing_history.py
import os
from datetime import datetime
import streamlit as st
import pyodbc
import pandas as pd

# =========================================================
#  CONFIG (use Streamlit secrets if available)
# =========================================================
SERVER   = st.secrets.get("MSSQL_SERVER",   "den1.mssql7.gear.host")
DATABASE = st.secrets.get("MSSQL_DATABASE", "billinghistory")
USERNAME = st.secrets.get("MSSQL_USERNAME", "billinghistory")
PASSWORD = st.secrets.get("MSSQL_PASSWORD", "Pk0Z-57_avQe")
LOCAL_DB_FILE = "data/cafe_pos.db"   # optional legacy sync source (SQLite)

# =========================================================
#  SQL SERVER CONNECTION HELPERS
# =========================================================
def detect_driver() -> str:
    """Return the newest installed SQL Server ODBC driver name."""
    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    if not drivers:
        raise RuntimeError(
            "No ODBC SQL Server drivers found. Install 'ODBC Driver 17/18 for SQL Server'."
        )
    return drivers[-1]

def get_connection() -> pyodbc.Connection:
    """Open a connection to SQL Server."""
    driver = detect_driver()
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
#  SCHEMA
# =========================================================
def ensure_tables_exist():
    """Create tables if they don't exist."""
    with get_connection() as conn:
        cur = conn.cursor()

        cur.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='customers' AND xtype='U')
            CREATE TABLE customers (
                customer_id   VARCHAR(255) PRIMARY KEY,
                customer_name VARCHAR(255),
                customer_number VARCHAR(255)
            );
        """)

        cur.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='inventory' AND xtype='U')
            CREATE TABLE inventory (
                ingredient VARCHAR(255) PRIMARY KEY,
                quantity   DECIMAL(18,4) NOT NULL DEFAULT 0,
                unit       VARCHAR(50)
            );
        """)

        cur.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='billing' AND xtype='U')
            CREATE TABLE billing (
                invoice_id  VARCHAR(255),
                [timestamp] DATETIME,
                customer_id VARCHAR(255),
                product_id  VARCHAR(255),
                product_name VARCHAR(255),
                quantity    INT,
                unit_price  DECIMAL(18,4),
                total       DECIMAL(18,4),
                CONSTRAINT PK_billing PRIMARY KEY (invoice_id, product_id),
                CONSTRAINT FK_billing_customer
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            );
        """)

        conn.commit()

# =========================================================
#  FETCH (SQL Server)
# =========================================================
def fetch_server_billing_df() -> pd.DataFrame:
    with get_connection() as conn:
        sql = """
            SELECT b.invoice_id,
                   b.[timestamp],
                   c.customer_id,
                   c.customer_name,
                   c.customer_number,
                   b.product_id,
                   b.product_name,
                   b.quantity,
                   b.unit_price,
                   b.total
            FROM billing b
            LEFT JOIN customers c ON b.customer_id = c.customer_id
            ORDER BY b.[timestamp] DESC;
        """
        return pd.read_sql(sql, conn)

def fetch_existing_customer_ids() -> set:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT customer_id FROM customers;")
        return {row[0] for row in cur.fetchall()}

# =========================================================
#  UPSERTS
# =========================================================
def upsert_customers(df: pd.DataFrame) -> int:
    """Insert customers that don't exist yet. Returns count inserted."""
    if df.empty:
        return 0

    existing = fetch_existing_customer_ids()
    new_df = df[~df["customer_id"].isin(existing)] if "customer_id" in df else pd.DataFrame()

    if new_df.empty:
        return 0

    with get_connection() as conn:
        cur = conn.cursor()
        for _, row in new_df.iterrows():
            cur.execute(
                "INSERT INTO customers (customer_id, customer_name, customer_number) VALUES (?, ?, ?);",
                (row["customer_id"], row.get("customer_name"), row.get("customer_number")),
            )
        conn.commit()
    return len(new_df)

def upsert_billing(df: pd.DataFrame):
    """Upsert billing lines via MERGE on (invoice_id, product_id)."""
    if df.empty:
        return

    # Ensure expected columns exist
    needed = ["invoice_id","timestamp","customer_id","product_id","product_name","quantity","unit_price","total"]
    for col in needed:
        if col not in df.columns:
            df[col] = None

    with get_connection() as conn:
        cur = conn.cursor()
        for _, r in df.drop_duplicates(subset=["invoice_id","product_id"]).iterrows():
            cur.execute("""
                MERGE billing AS tgt
                USING (SELECT ? AS invoice_id,
                              ? AS [timestamp],
                              ? AS customer_id,
                              ? AS product_id,
                              ? AS product_name,
                              ? AS quantity,
                              ? AS unit_price,
                              ? AS total) AS src
                  ON tgt.invoice_id = src.invoice_id AND tgt.product_id = src.product_id
                WHEN MATCHED THEN UPDATE SET
                    [timestamp]  = src.[timestamp],
                    customer_id  = src.customer_id,
                    product_name = src.product_name,
                    quantity     = src.quantity,
                    unit_price   = src.unit_price,
                    total        = src.total
                WHEN NOT MATCHED THEN INSERT
                    (invoice_id, [timestamp], customer_id, product_id, product_name, quantity, unit_price, total)
                    VALUES (src.invoice_id, src.[timestamp], src.customer_id, src.product_id, src.product_name, src.quantity, src.unit_price, src.total);
            """, (
                r["invoice_id"],
                r["timestamp"] if pd.notna(r["timestamp"]) else None,
                r["customer_id"],
                r["product_id"],
                r.get("product_name"),
                int(r["quantity"]) if pd.notna(r["quantity"]) else None,
                float(r["unit_price"]) if pd.notna(r["unit_price"]) else None,
                float(r["total"]) if pd.notna(r["total"]) else None,
            ))
        conn.commit()

def upsert_inventory(items: list[dict]):
    """Upsert inventory items: [{'ingredient','quantity','unit'}, ...]."""
    if not items:
        return
    with get_connection() as conn:
        cur = conn.cursor()
        for it in items:
            cur.execute("""
                MERGE inventory AS tgt
                USING (SELECT ? AS ingredient, ? AS quantity, ? AS unit) AS src
                  ON tgt.ingredient = src.ingredient
                WHEN MATCHED THEN UPDATE SET
                    quantity = src.quantity,
                    unit     = src.unit
                WHEN NOT MATCHED THEN INSERT (ingredient, quantity, unit)
                    VALUES (src.ingredient, src.quantity, src.unit);
            """, (it["ingredient"], float(it.get("quantity", 0)), it.get("unit")))
        conn.commit()

# =========================================================
#  OPTIONAL: READ FROM LOCAL SQLITE (for one-time syncs)
# =========================================================
def fetch_local_sqlite_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Read from a local SQLite file if it exists (used to migrate data once)."""
    import sqlite3
    if not os.path.exists(LOCAL_DB_FILE):
        return pd.DataFrame()
    with sqlite3.connect(LOCAL_DB_FILE) as conn:
        return pd.read_sql_query(sql, conn, params=params)

def load_local_billing_snapshot() -> pd.DataFrame:
    sql = """
        SELECT b.invoice_id,
               b.timestamp,
               c.customer_id,
               c.customer_name,
               c.customer_number,
               b.product_id,
               b.product_name,
               b.quantity,
               b.unit_price,
               b.total
        FROM billing b
        LEFT JOIN customers c ON b.customer_id = c.customer_id
        ORDER BY b.timestamp DESC;
    """
    return fetch_local_sqlite_df(sql)

# =========================================================
#  MAIN STREAMLIT PAGE
# =========================================================
def billing_history_page():
    st.title("🧾 Billing History (SQL Server)")

    # Ensure schema
    try:
        ensure_tables_exist()
    except Exception as e:
        st.error(f"Failed to ensure tables exist: {e}")
        return

    tabs = st.tabs(["📡 View on Server", "🔀 (Optional) Sync from Local SQLite"])

    # ---- TAB 1: View live data from SQL Server ----
    with tabs[0]:
        try:
            server_df = fetch_server_billing_df()
            if server_df.empty:
                st.info("No billing records on the server yet.")
            else:
                st.dataframe(server_df, use_container_width=True)
                st.download_button(
                    "⬇ Download Billing History (CSV)",
                    data=server_df.to_csv(index=False).encode("utf-8"),
                    file_name=f"billing_history_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}Z.csv",
                    mime="text/csv",
                )
        except Exception as e:
            st.error(f"Error reading from SQL Server: {type(e).__name__}: {e}")

    # ---- TAB 2: One-time migration from local SQLite ----
    with tabs[1]:
        st.caption(f"Local SQLite path: `{LOCAL_DB_FILE}`")
        if os.path.exists(LOCAL_DB_FILE):
            df_local = load_local_billing_snapshot()
            if df_local.empty:
                st.info("Local SQLite found, but no billing rows to sync.")
            else:
                st.success(f"Loaded {len(df_local)} billing rows from local SQLite.")
                st.dataframe(df_local.head(100), use_container_width=True)

                # You can also prepare some sample inventory to sync:
                inv_sample = [
                    {"ingredient": "Hot Water",        "quantity": 9850, "unit": "ml"},
                    {"ingredient": "Espresso Beans",   "quantity":  928, "unit": "g"},
                    {"ingredient": "Milk",             "quantity": 9850, "unit": "ml"},
                    {"ingredient": "Chocolate Syrup",  "quantity": 1000, "unit": "g"},
                ]

                if st.button("📤 Sync local → SQL Server"):
                    try:
                        # Upsert customers first (avoid FK issues)
                        cust_df = df_local[["customer_id","customer_name","customer_number"]].drop_duplicates()
                        inserted = upsert_customers(cust_df)
                        upsert_billing(df_local)
                        upsert_inventory(inv_sample)
                        st.success(f"✅ Synced! New customers inserted: {inserted}. Billing & inventory upserted.")
                    except Exception as e:
                        st.error(f"❌ Sync failed: {type(e).__name__}: {e}")
        else:
            st.info("No local SQLite file found. You can ignore this tab.")
