# billing.py
import streamlit as st
import pandas as pd
import uuid
from db import init_db, query_db, fetch_df
from bom_handler import calculate_deduction, ensure_bom_seeded, INGREDIENT_UNITS

# Product list
PRODUCTS = [
    {"product_id": "C1001", "name": "Amaretto", "price": 2.50},
    {"product_id": "C1002", "name": "Caffe Latte", "price": 3.00},
    {"product_id": "C1003", "name": "Caffe Mocha", "price": 3.20},
    {"product_id": "C1004", "name": "Chamomile", "price": 2.80},
    {"product_id": "C1005", "name": "Columbian", "price": 3.50},
    {"product_id": "C1006", "name": "Darjeeling", "price": 2.50},
    {"product_id": "C1007", "name": "Decaf Espresso", "price": 3.00},
    {"product_id": "C1008", "name": "Decaf Irish Cream", "price": 3.20},
    {"product_id": "C1009", "name": "Earl Grey", "price": 2.80},
    {"product_id": "C1010", "name": "Green Tea", "price": 3.50},
    {"product_id": "C1011", "name": "Lemon", "price": 3.20},
    {"product_id": "C1012", "name": "Mint", "price": 2.80},
    {"product_id": "C1013", "name": "Regular Espresso", "price": 3.50},
]

# Quick lookup mappings
PRODUCT_OPTIONS = [f"{p['product_id']} — {p['name']}" for p in PRODUCTS]
PID_BY_LABEL = {opt: p['product_id'] for opt, p in zip(PRODUCT_OPTIONS, PRODUCTS)}
NAME_BY_LABEL = {opt: p['name'] for opt, p in zip(PRODUCT_OPTIONS, PRODUCTS)}
PRICE_BY_LABEL = {opt: p['price'] for opt, p in zip(PRODUCT_OPTIONS, PRODUCTS)}


def get_or_create_customer(customer_number: str, customer_name: str | None):
    """Return (customer_id, customer_name). Create a new customer if needed."""
    customer_number = (customer_number or "").strip()
    if not customer_number:
        return None, None

    # Check if exists
    row = query_db(
        "SELECT customer_id, customer_name FROM customers WHERE customer_number=?",
        (customer_number,), fetch=True
    )
    if row:
        return row[0][0], row[0][1]

    # Require name for new
    if not customer_name or not customer_name.strip():
        return None, None

    # Create
    count = query_db("SELECT COUNT(*) FROM customers", fetch=True)[0][0]
    new_id = f"CUST-{count + 1:04d}"
    query_db(
        "INSERT INTO customers (customer_id, customer_number, customer_name) VALUES (?, ?, ?)",
        (new_id, customer_number, customer_name.strip())
    )
    return new_id, customer_name.strip()


def ensure_inventory_rows_exist(ingredients: list[str]):
    """Ensure each ingredient exists in inventory; insert with 0 quantity if missing."""
    for ing in ingredients:
        unit = INGREDIENT_UNITS.get(ing, "")
        query_db("""
            MERGE inventory AS target
            USING (SELECT ? AS ingredient, CAST(0 AS FLOAT) AS quantity, ? AS unit) AS source
            ON target.ingredient = source.ingredient
            WHEN NOT MATCHED THEN
                INSERT (ingredient, quantity, unit) VALUES (source.ingredient, source.quantity, source.unit);
        """, (ing, unit))


def billing_page():
    init_db()
    ensure_bom_seeded()

    if "cart" not in st.session_state:
        st.session_state.cart = []

    st.header("🧾 Cafe Billing")

    # Add item form
    with st.form("add_item_form", clear_on_submit=True):
        col1, col2 = st.columns([3, 1])
        label = col1.selectbox("Product", PRODUCT_OPTIONS, index=0)
        qty = col2.number_input("Quantity", min_value=1, value=1, step=1)
        if st.form_submit_button("Add to Cart"):
            st.session_state.cart.append({
                "product_id": PID_BY_LABEL[label],
                "product_name": NAME_BY_LABEL[label],
                "quantity": int(qty),
                "unit_price": PRICE_BY_LABEL[label],
                "total": PRICE_BY_LABEL[label] * int(qty)
            })
            st.success("Item added to cart.")

    # Stop if empty
    if not st.session_state.cart:
        return

    # Cart display
    st.subheader("🛒 Current Cart")
    st.markdown("""
        <style>
        button[title="Remove"] {
            font-size: 0.8rem !important;
            padding: 0.1rem 0.3rem !important;
            color: red !important;
        }
        </style>
    """, unsafe_allow_html=True)

    for idx, item in enumerate(st.session_state.cart):
        col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 0.5])
        col1.write(item["product_name"])
        col2.write(f"Qty: {item['quantity']}")
        col3.write(f"Unit: ${item['unit_price']:.2f}")
        col4.write(f"Total: ${item['total']:.2f}")
        if col5.button("❌", key=f"remove_{idx}", help="Remove", type="secondary"):
            st.session_state.cart.pop(idx)
            st.rerun()

    # Customer details
    st.subheader("👤 Customer Details")
    customer_number = st.text_input("Customer Number").strip()
    auto_name = None
    new_name = ""

    if customer_number:
        row = query_db("SELECT customer_name FROM customers WHERE customer_number=?",
                       (customer_number,), fetch=True)
        if row:
            auto_name = row[0][0]
            st.info(f"Existing customer: {auto_name}")
        else:
            new_name = st.text_input("Customer Name (New Customer)")

    # Save invoice
    if st.button("💾 Save Invoice"):
        if not customer_number:
            st.warning("Customer number is empty.")
            return

        cust_id, cust_name = get_or_create_customer(
            customer_number, new_name if not auto_name else auto_name
        )
        if cust_id is None:
            st.error("Customer name required for new customer.")
            return

        invoice_id = str(uuid.uuid4())
        ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

        # Save billing
        for item in st.session_state.cart:
            query_db("""
                INSERT INTO billing
                (invoice_id, customer_id, product_id, product_name, quantity, unit_price, total, [timestamp])
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                invoice_id, cust_id, item["product_id"], item["product_name"],
                int(item["quantity"]), float(item["unit_price"]), float(item["total"]), ts
            ))

        # Deduct from inventory
        deduction = calculate_deduction(st.session_state.cart)
        if deduction:
            ensure_inventory_rows_exist(list(deduction.keys()))
            for ing, dec_qty in deduction.items():
                query_db(
                    "UPDATE inventory SET quantity = ISNULL(quantity, 0) - ? WHERE ingredient = ?",
                    (float(dec_qty), ing)
                )

        st.success(f"Invoice {invoice_id} saved for Customer {cust_id}. Inventory updated.")
        st.session_state.cart = []
