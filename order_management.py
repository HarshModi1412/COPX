import streamlit as st
import pandas as pd
from db import fetch_df, query_db
from bom_handler import calculate_deduction, ensure_bom_seeded, INGREDIENT_UNITS

def order_management_page():
    ensure_bom_seeded()  # Make sure BOM is ready
    st.title("📦 Order Management")

    # Fetch grouped orders
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

    for _, row in orders_df.iterrows():
        with st.expander(f"🧾 Order {row['invoice_id']} - Customer {row['customer_id']}"):
            st.write(f"**Total Amount:** ₹{row['total_amount']}")
            st.write(f"**Order Time:** {row['order_time']}")

            # Show items for this invoice
            items_df = fetch_df("""
                SELECT product_id, product_name, quantity, unit_price, total
                FROM billing
                WHERE invoice_id = ?
            """, (row['invoice_id'],))
            st.table(items_df)

            col1, col2 = st.columns(2)

            # ✅ Mark as Done
            with col1:
                if st.button("✅ Mark as Done", key=f"done_{row['invoice_id']}"):
                    query_db(
                        "UPDATE billing SET status = 'done' WHERE invoice_id = ?",
                        (row['invoice_id'],)
                    )
                    st.success(f"Order {row['invoice_id']} marked as done.")
                    st.rerun()

            # ❌ Cancel Order — restore inventory
            with col2:
                if st.button("❌ Cancel Order", key=f"cancel_{row['invoice_id']}"):
                    # Restore inventory
                    cart_items = items_df.to_dict(orient="records")
                    if cart_items:
                        from billing import ensure_inventory_rows_exist  # reuse from billing.py
                        deduction = calculate_deduction(cart_items)
                        if deduction:
                            ensure_inventory_rows_exist(list(deduction.keys()))
                            for ing, qty in deduction.items():
                                query_db(
                                    "UPDATE inventory SET quantity = ISNULL(quantity, 0) + ? WHERE ingredient = ?",
                                    (float(qty), ing)
                                )

                    # Update order status
                    query_db(
                        "UPDATE billing SET status = 'canceled' WHERE invoice_id = ?",
                        (row['invoice_id'],)
                    )
                    st.warning(f"Order {row['invoice_id']} canceled and inventory restored.")
                    st.rerun()

if __name__ == "__main__":
    order_management_page()
