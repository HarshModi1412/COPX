import streamlit as st
import pandas as pd
from db import fetch_df, query_db

def order_management_page():
    st.title("📦 Order Management")

    # Fetch and group orders
    orders_df = fetch_df("""
        SELECT 
            invoice_id,
            customer_id,
            SUM(total) AS total_amount,
            MIN(timestamp) AS order_time,
            status
        FROM billing
        GROUP BY invoice_id, customer_id, status
        HAVING status = 'ongoing'
        ORDER BY order_time ASC
    """)

    if orders_df.empty:
        st.info("No ongoing orders at the moment.")
        return

    # Display orders
    for _, row in orders_df.iterrows():
        with st.expander(f"🧾 Order {row['invoice_id']} - Customer {row['customer_id']}"):
            st.write(f"**Total Amount:** ₹{row['total_amount']}")
            st.write(f"**Order Time:** {row['order_time']}")

            # Show order items
            items_df = fetch_df("""
                SELECT product_name, quantity, unit_price, total
                FROM billing
                WHERE invoice_id = ?
            """, (row['invoice_id'],))
            st.table(items_df)

            col1, col2 = st.columns(2)

            with col1:
                if st.button("✅ Mark as Done", key=f"done_{row['invoice_id']}"):
                    query_db("UPDATE billing SET status = 'done' WHERE invoice_id = ?", (row['invoice_id'],))
                    st.success(f"Order {row['invoice_id']} marked as done.")
                    st.rerun()

            with col2:
                if st.button("❌ Cancel Order", key=f"cancel_{row['invoice_id']}"):
                    query_db("UPDATE billing SET status = 'canceled' WHERE invoice_id = ?", (row['invoice_id'],))
                    st.warning(f"Order {row['invoice_id']} canceled.")
                    st.rerun()

if __name__ == "__main__":
    order_management_page()
