import streamlit as st
import pandas as pd
from db import fetch_df, query_db

def ensure_safety_stock_column():
    """Add safety_stock column to inventory if missing."""
    query_db("""
        ALTER TABLE inventory ADD COLUMN safety_stock REAL DEFAULT 0
    """, ignore_errors=True)  # Won’t break if already exists

def analyst_page():
    st.title("📊 Business Analyst Dashboard")
    st.write("Insights on inventory levels and restocking needs.")

    # Ensure safety_stock column exists
    ensure_safety_stock_column()

    # --- Fetch inventory data with safety stock ---
    df = fetch_df("""
        SELECT 
            ingredient AS 'Ingredient',
            quantity AS 'Quantity',
            unit AS 'Unit',
            safety_stock AS 'Safety Stock'
        FROM inventory
    """)

    if df.empty:
        st.warning("No inventory data found.")
        return

    # --- Check against safety stock ---
    df["Below Safety Level?"] = df.apply(
        lambda row: row["Quantity"] < row["Safety Stock"], axis=1
    )

    # --- Highlight critical items ---
    low_stock_df = df[df["Below Safety Level?"] == True]

    st.subheader("📉 Current Inventory Status")
    st.dataframe(df, use_container_width=True)

    if not low_stock_df.empty:
        st.subheader("⚠️ Items Below Safety Stock")
        st.dataframe(low_stock_df, use_container_width=True)
    else:
        st.success("✅ All inventory items are above safety stock levels.")

    # --- Potential extension: Predict days left based on usage rate ---
    st.markdown("---")
    st.info("💡 Tip: You can extend this page to predict 'days left' based on sales data in the billing table.")
