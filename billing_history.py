import os
import streamlit as st
import pyodbc
import pandas as pd
from db import init_db, fetch_df, DB_FILE  # Local DB utilities

# =========================================================
#  SQL SERVER CONNECTION HELPERS
# =========================================================
def detect_driver() -> str:
    """Find the latest installed SQL Server ODBC driver."""
    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    if not drivers:
        raise RuntimeError(
            "❌ No ODBC SQL Server drivers installed.\n"
            "Please install 'ODBC Driver 18 for SQL Server'."
        )
    return drivers[-1]  # Use the latest version


def get_connection():
    """Return a live SQL Server connection."""
    driver = detect_driver()
    conn_str = (
        f"Driver={{{driver}}};"
        "Server=den1.mssql7.gear.host;"
        "Database=billinghistory;"
        "UID=billinghistory;"
        "PWD=Pk0Z-57_avQe;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


# =========================================================
#  TABLE CREATION
# =========================================================
def ensure_tables_exist():
    """Ensure required tables exist in SQL Server."""
    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sysobjects WHERE name='customers' AND xtype='U'
            )
            CREATE TABLE customers (
                customer_id VARCHAR(255) PRIMARY KEY,
                customer_name VARCHAR(255),
                customer_number VARCHAR(255)
            )
        """)

        cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sysobjects WHERE name='inventory' AND xtype='U'
            )
            CREATE TABLE inventory (
                ingredient VARCHAR(255) PRIMARY KEY,
                quantity DECIMAL(10,2),
                unit VARCHAR(20)
            )
        """)

        cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sysobjects WHERE name='billing' AND xtype='U'
            )
            CREATE TABLE billing (
                invoice_id VARCHAR(255),
                timestamp DATETIME,
                customer_id VARCHAR(255),
                product_id VARCHAR(255),
                quantity INT,
                unit_price DECIMAL(10,2),
                total DECIMAL(10,2),
                PRIMARY KEY (invoice_id, product_id)
            )
        """)

        conn.commit()


# =========================================================
#  FETCH HELPERS
# =========================================================
def get_existing_customer_ids() -> set:
    """Return set of existing customer IDs."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT customer_id FROM customers")
        return {row[0] for row in cursor.fetchall()}


# =========================================================
#  MAIN UPDATE FUNCTION
# =========================================================
def update_sql_server(df: pd.DataFrame, inventory_data: list = None):
    """
    Updates SQL Server with customers, billing, and inventory.
    Prevents duplicates for customers & billing.
    Replaces inventory table entirely with provided inventory_data.
    """
    try:
        ensure_tables_exist()
        existing_customers = get_existing_customer_ids()

        # -------- INSERT NEW CUSTOMERS --------
        customers_df = df[["customer_id", "customer_name", "customer_number"]].drop_duplicates()
        new_customers = customers_df[~customers_df["customer_id"].isin(existing_customers)]

        with get_connection() as conn:
            cursor = conn.cursor()

            for _, row in new_customers.iterrows():
                cursor.execute(
                    "INSERT INTO customers (customer_id, customer_name, customer_number) VALUES (?, ?, ?)",
                    (row["customer_id"], row["customer_name"], row["customer_number"])
                )

            # -------- INSERT BILLING DATA --------
            billing_df = df[[
                "invoice_id", "timestamp", "customer_id", "product_id",
                "quantity", "unit_price", "total"
            ]].drop_duplicates(subset=["invoice_id", "product_id"])

            for _, row in billing_df.iterrows():
                cursor.execute("""
                    IF NOT EXISTS (
                        SELECT 1 FROM billing WHERE invoice_id = ? AND product_id = ?
                    )
                    BEGIN
                        INSERT INTO billing (invoice_id, timestamp, customer_id, product_id, quantity, unit_price, total)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    END
                """, (
                    row["invoice_id"], row["product_id"],  # check params
                    row["invoice_id"], row["timestamp"], row["customer_id"], row["product_id"],
                    row["quantity"], row["unit_price"], row["total"]
                ))

            # -------- REPLACE INVENTORY TABLE --------
            if inventory_data:
                cursor.execute("TRUNCATE TABLE inventory")
                for item in inventory_data:
                    cursor.execute("""
                        INSERT INTO inventory (ingredient, quantity, unit)
                        VALUES (?, ?, ?)
                    """, (item["ingredient"], item["quantity"], item["unit"]))

            conn.commit()

        st.success(f"✅ {len(new_customers)} new customers added. Billing and inventory updated successfully.")

    except Exception as e:
        st.error(f"❌ Failed to update SQL Server:\n{type(e).__name__}: {e}")


# =========================================================
#  STREAMLIT PAGE
# =========================================================
def billing_history_page():
    st.title("🧾 Billing History")
    init_db()  # Local SQLite init

    if not os.path.exists(DB_FILE):
        st.warning("Local database not found yet.")
        return

    # -------- FETCH LOCAL DATA --------
    df = fetch_df("""
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
        ORDER BY b.timestamp DESC
    """)

    if df.empty:
        st.info("No billing records available.")
        return

    st.dataframe(df, use_container_width=True)

    # -------- CSV DOWNLOAD --------
    csv_data = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇ Download Billing History (CSV)",
        data=csv_data,
        file_name="billing_history.csv",
        mime="text/csv"
    )

    # -------- SAMPLE INVENTORY --------
    inventory_data = [
        {"ingredient": "Hot Water", "quantity": 9850, "unit": "ml"},
        {"ingredient": "Espresso Beans", "quantity": 928, "unit": "g"},
        {"ingredient": "Milk", "quantity": 9850, "unit": "ml"},
        {"ingredient": "Chocolate Syrup", "quantity": 1000, "unit": "g"},
    ]

    if st.button("📤 Update on Server"):
        update_sql_server(df, inventory_data)
