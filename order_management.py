import streamlit as st
import pandas as pd
from db import fetch_df, query_db

def order_management_page():
    st.title("📦 Order Management")

    # Fetch ongoing orders
    orders_df = fetch_df("""
        SELECT billing_id, customer_name, total_amount, status, invoice_date
        FROM billing
        WHERE status = 'ongoing'
        ORDER BY invoice_date ASC
    """)

    if orders_df.empty:
        st.info("No ongoing orders at the moment.")
        return

    # Show as interactive list
    for _, row in orders_df.iterrows():
        col1, col2, col3, col4, col5 = st.columns([3, 3, 2, 2, 2])
        col1.write(f"**Order ID:** {row['billing_id']}")
        col2.write(f"**Customer:** {row['customer_name']}")
        col3.write(f"💰 {row['total_amount']}")
        col4.write(f"📅 {row['invoice_date']}")

        with col5:
            if st.button("✅ Done", key=f"done_{row['billing_id']}"):
                query_db("UPDATE billing SET status = 'done' WHERE billing_id = ?", (row['billing_id'],))
                st.success(f"Order {row['billing_id']} marked as done.")
                st.rerun()

            if st.button("❌ Cancel", key=f"cancel_{row['billing_id']}"):
                query_db("UPDATE billing SET status = 'canceled' WHERE billing_id = ?", (row['billing_id'],))
                st.warning(f"Order {row['billing_id']} canceled.")
                st.rerun()

# Run the page
if __name__ == "__main__":
    order_management_page()
