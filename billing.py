# billing.py
import streamlit as st
import pandas as pd
import uuid
from db import init_db, query_db, fetch_df
from bom_handler import calculate_deduction, ensure_bom_seeded, INGREDIENT_UNITS

# 5 café products
PRODUCTS = [
    {"product_id": "C1001", "name": "Espresso",  "price": 2.50},
    {"product_id": "C1002", "name": "Cappuccino","price": 3.00},
    {"product_id": "C1003", "name": "Latte",     "price": 3.20},
    {"product_id": "C1004", "name": "Americano", "price": 2.80},
    {"product_id": "C1005", "name": "Mocha",     "price": 3.50},
]
PRODUCT_OPTIONS = [f"{p['product_id']} — {p['name']}" for p in PRODUCTS]
PID_BY_LABEL   = {f"{p['product_id']} — {p['name']}": p['product_id'] for p in PRODUCTS}
NAME_BY_LABEL  = {f"{p['product_id']} — {p['name']}": p['name']      for p in PRODUCTS}
PRICE_BY_LABEL = {f"{p['product_id']} — {p['name']}": p['price']     for p in PRODUCTS}


def get_or_create_customer(customer_number: str, customer_name: str | None):
    """Return (customer_id, customer_name). Create a new customer if needed."""
    customer_number = (customer_number or "").strip()
    if not customer_number:
        return None, None

    # Existing?
    row = query_db(
        "SELECT customer_id, customer_name FROM customers WHERE customer_number=?",
        (customer_number,), fetch=True
    )
    if row:
        return row[0][0], row[0][1]

    # Need a name to create
    if not customer_name or not customer_name.strip():
        return None, None

    # Create new with sequential ID
    count = query_db("SELECT COUNT(*) FROM customers", fetch=True)[0][0]
    new_id = f"CUST-{count + 1:04d}"
    query_db(
        "INSERT INTO customers (customer_id, customer_number, customer_name) VALUES (?, ?, ?)",
        (new_id, customer_number, customer_name.strip())
    )
    return new_id, customer_name.strip()


def ensure_inventory_rows_exist(ingredients: list[str]):
    """Ensure each ingredient exists in inventory; if missing, insert with quantity=0 and correct unit."""
    for ing in ingredients:
        unit = INGREDIENT_UNITS.get(ing, "")
        # SQL Server: upsert with MERGE to avoid PK violation
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

    # Add-to-cart UI
    with st.form("add_item_form", clear_on_submit=True):
        c1, c2 = st.columns([3, 1])
        with c1:
            label = st.selectbox("Product", PRODUCT_OPTIONS, index=0)
        with c2:
            qty = st.number_input("Quantity", min_value=1, value=1, step=1)
        if st.form_submit_button("Add to Cart"):
            st.session_state.cart.append({
                "product_id": PID_BY_LABEL[label],
                "product_name": NAME_BY_LABEL[label],
                "quantity": int(qty),
                "unit_price": PRICE_BY_LABEL[label],
                "total": PRICE_BY_LABEL[label] * int(qty)
            })
            st.success("Item added to cart.")

    if not st.session_state.cart:
        return

    st.subheader("🛒 Current Cart")
    st.dataframe(pd.DataFrame(st.session_state.cart), use_container_width=True)

    # Customer
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

        # Insert invoice line items (timestamp is a reserved word, bracket it)
        for item in st.session_state.cart:
            query_db("""
                INSERT INTO billing
                (invoice_id, customer_id, product_id, product_name, quantity, unit_price, total, [timestamp])
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                invoice_id, cust_id, item["product_id"], item["product_name"],
                int(item["quantity"]), float(item["unit_price"]), float(item["total"]), ts
            ))

        # Deduct inventory using BOM — do it with UPDATE so we don't read/write whole frames
        deduction = calculate_deduction(st.session_state.cart)
        if deduction:
            ensure_inventory_rows_exist(list(deduction.keys()))
            for ing, dec_qty in deduction.items():
                # Safe arithmetic update; ensure row exists first (done above)
                query_db(
                    "UPDATE inventory SET quantity = ISNULL(quantity, 0) - ? WHERE ingredient = ?",
                    (float(dec_qty), ing)
                )

        st.success(f"Invoice {invoice_id} saved for Customer {cust_id}. Inventory updated.")
        st.session_state.cart = []
