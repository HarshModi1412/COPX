# orders.py
import uuid
import pandas as pd
import streamlit as st
from db import init_db, query_db, fetch_df
from bom_handler import calculate_deduction, ensure_bom_seeded, INGREDIENT_UNITS
from billing import PRODUCTS, PRODUCT_OPTIONS, PID_BY_LABEL, NAME_BY_LABEL, PRICE_BY_LABEL

ORDER_STATUSES = ["Pending", "Paid", "Cancelled"]  # minimal KDS-like flow

def ensure_order_tables():
    init_db()
    # orders
    query_db("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='orders' AND xtype='U')
        CREATE TABLE orders (
            order_id NVARCHAR(50) PRIMARY KEY,
            customer_id NVARCHAR(50) NULL,
            customer_number NVARCHAR(50) NULL,
            customer_name NVARCHAR(255) NULL,
            status NVARCHAR(20) NOT NULL,
            created_at DATETIME NOT NULL DEFAULT GETDATE()
        )
    """)
    # order_items
    query_db("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='order_items' AND xtype='U')
        CREATE TABLE order_items (
            order_id NVARCHAR(50),
            product_id NVARCHAR(50),
            product_name NVARCHAR(255),
            quantity INT,
            unit_price FLOAT,
            total FLOAT,
            CONSTRAINT PK_order_items PRIMARY KEY (order_id, product_id)
        )
    """)
    # minimal FK (customer optional until payment time)
    query_db("""
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_order_items_orders')
        ALTER TABLE order_items
        ADD CONSTRAINT FK_order_items_orders FOREIGN KEY(order_id) REFERENCES orders(order_id)
    """, ignore_errors=True)

def upsert_customer(customer_number: str, customer_name: str | None):
    # Return (customer_id, name) — create if needed (same as billing.get_or_create_customer)
    customer_number = (customer_number or "").strip()
    if not customer_number:
        return None, None
    row = query_db(
        "SELECT customer_id, customer_name FROM customers WHERE customer_number=?",
        (customer_number,), fetch=True
    )
    if row:
        return row[0][0], row[0][1]
    if not customer_name or not customer_name.strip():
        return None, None
    count = query_db("SELECT COUNT(*) FROM customers", fetch=True)[0][0]
    new_id = f"CUST-{count + 1:04d}"
    query_db(
        "INSERT INTO customers (customer_id, customer_number, customer_name) VALUES (?, ?, ?)",
        (new_id, customer_number, customer_name.strip())
    )
    return new_id, customer_name.strip()

def list_pending_orders() -> pd.DataFrame:
    return fetch_df("""
        SELECT o.order_id, o.created_at, o.customer_number, o.customer_name, o.status,
               SUM(oi.total) AS order_total
        FROM orders o
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        WHERE o.status='Pending'
        GROUP BY o.order_id, o.created_at, o.customer_number, o.customer_name, o.status
        ORDER BY o.created_at DESC
    """)

def load_order_items(order_id: str) -> list[dict]:
    df = fetch_df("""
        SELECT product_id, product_name, quantity, unit_price, total
        FROM order_items WHERE order_id=?
    """, (order_id,))
    items = []
    for _, r in df.iterrows():
        items.append({
            "product_id": r["product_id"],
            "product_name": r["product_name"],
            "quantity": int(r["quantity"]),
            "unit_price": float(r["unit_price"]),
            "total": float(r["total"])
        })
    return items

def save_cart_to_order(order_id: str, cart: list[dict]):
    # replace order_items for this order_id
    query_db("DELETE FROM order_items WHERE order_id=?", (order_id,))
    if cart:
        rows = []
        for it in cart:
            rows.append((
                order_id, it["product_id"], it["product_name"],
                int(it["quantity"]), float(it["unit_price"]), float(it["total"])
            ))
        query_db("""
            INSERT INTO order_items (order_id, product_id, product_name, quantity, unit_price, total)
            VALUES (?, ?, ?, ?, ?, ?)
        """, many=True, seq=rows)

def finalize_payment(order_id: str, customer_number: str, customer_name_input: str | None):
    """Mark order Paid -> create invoice rows -> deduct inventory."""
    ensure_bom_seeded()

    # Ensure/attach customer
    cust_id, cust_name = upsert_customer(customer_number, customer_name_input)
    if cust_id is None:
        return None, "Customer name required for new customer."

    # Get items
    items = load_order_items(order_id)
    if not items:
        return None, "Order has no items."

    invoice_id = str(uuid.uuid4())
    ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    # Write billing rows
    for it in items:
        query_db("""
            INSERT INTO billing
            (invoice_id, customer_id, product_id, product_name, quantity, unit_price, total, [timestamp])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            invoice_id, cust_id, it["product_id"], it["product_name"],
            int(it["quantity"]), float(it["unit_price"]), float(it["total"]), ts
        ))

    # Deduct inventory based on BOM
    deduction = calculate_deduction(items)
    if deduction:
        for ing, dec_qty in deduction.items():
            unit = INGREDIENT_UNITS.get(ing, "")
            query_db("""
                MERGE inventory AS target
                USING (SELECT ? AS ingredient, CAST(0 AS FLOAT) AS quantity, ? AS unit) AS source
                ON target.ingredient = source.ingredient
                WHEN NOT MATCHED THEN INSERT (ingredient, quantity, unit) VALUES (source.ingredient, source.quantity, source.unit);
            """, (ing, unit))
            query_db("UPDATE inventory SET quantity = ISNULL(quantity,0) - ? WHERE ingredient=?",
                     (float(dec_qty), ing))

    # Update order header to Paid + attach IDs
    query_db("""
        UPDATE orders SET status='Paid', customer_id=?, customer_number=?, customer_name=? WHERE order_id=?
    """, (cust_id, customer_number, cust_name, order_id))

    return invoice_id, None

def orders_page():
    ensure_order_tables()
    st.header("🧾 Orders")

    # Session state
    if "current_order_id" not in st.session_state:
        st.session_state.current_order_id = None
    if "order_cart" not in st.session_state:
        st.session_state.order_cart = []

    # --- Left: Build order
    left, right = st.columns([2, 1])

    with left:
        st.subheader("Create / Edit Order")
        with st.form("add_item", clear_on_submit=True):
            col1, col2 = st.columns([3, 1])
            label = col1.selectbox("Product", PRODUCT_OPTIONS, index=0, key="order_sel")
            qty = col2.number_input("Qty", min_value=1, value=1, step=1, key="order_qty")
            if st.form_submit_button("Add Item"):
                st.session_state.order_cart.append({
                    "product_id": PID_BY_LABEL[label],
                    "product_name": NAME_BY_LABEL[label],
                    "quantity": int(qty),
                    "unit_price": PRICE_BY_LABEL[label],
                    "total": PRICE_BY_LABEL[label] * int(qty)
                })
                st.success("Added.")

        # cart table
        if st.session_state.order_cart:
            st.write("### Cart")
            for idx, it in enumerate(st.session_state.order_cart):
                c1, c2, c3, c4, c5 = st.columns([4, 2, 2, 2, 1])
                c1.write(it["product_name"])
                c2.write(f"Qty: {it['quantity']}")
                c3.write(f"Unit: ${it['unit_price']:.2f}")
                c4.write(f"Total: ${it['total']:.2f}")
                if c5.button("❌", key=f"del_{idx}"):
                    st.session_state.order_cart.pop(idx)
                    st.rerun()
            order_total = sum(x["total"] for x in st.session_state.order_cart)
            st.info(f"Order total: ${order_total:.2f}")
        else:
            st.caption("Cart is empty.")

        # Save / Park
        cA, cB, cC = st.columns(3)
        if cA.button("📝 New Order"):
            st.session_state.current_order_id = str(uuid.uuid4())
            st.session_state.order_cart = []
            # create header with Pending
            query_db("""
                INSERT INTO orders (order_id, status) VALUES (?, 'Pending')
            """, (st.session_state.current_order_id,))
            st.success(f"New order {st.session_state.current_order_id} started.")
        if cB.button("💾 Park / Update Order", disabled=not st.session_state.order_cart):
            if not st.session_state.current_order_id:
                st.session_state.current_order_id = str(uuid.uuid4())
                query_db("INSERT INTO orders (order_id, status) VALUES (?, 'Pending')",
                         (st.session_state.current_order_id,))
            save_cart_to_order(st.session_state.current_order_id, st.session_state.order_cart)
            st.success("Order saved (Pending).")
        if cC.button("🗑 Cancel Order", disabled=not st.session_state.current_order_id):
            query_db("UPDATE orders SET status='Cancelled' WHERE order_id=?",
                     (st.session_state.current_order_id,))
            query_db("DELETE FROM order_items WHERE order_id=?",
                     (st.session_state.current_order_id,))
            st.session_state.current_order_id = None
            st.session_state.order_cart = []
            st.info("Order cancelled.")

    # --- Right: Pending / Payment
    with right:
        st.subheader("Pending Orders")
        dfp = list_pending_orders()
        if dfp is not None and not dfp.empty:
            options = [f"{r.order_id} — ${r.order_total:.2f} — {r.created_at:%H:%M}"
                       for _, r in dfp.iterrows()]
            pick = st.selectbox("Load", options)
            if st.button("Load Selected"):
                oid = dfp.iloc[options.index(pick)]["order_id"]
                st.session_state.current_order_id = oid
                st.session_state.order_cart = load_order_items(oid)
                st.success(f"Loaded order {oid}.")
        else:
            st.caption("No pending orders.")

        st.markdown("---")
        st.subheader("Take Payment")
        st.caption("Attach/auto-create customer, create invoice, deduct inventory.")
        cn = st.text_input("Customer Number")
        # show name if existing
        show_name = ""
        if cn:
            row = query_db("SELECT customer_name FROM customers WHERE customer_number=?",
                           (cn,), fetch=True)
            if row:
                show_name = row[0][0]
                st.info(f"Existing customer: {show_name}")
        new_name = st.text_input("Customer Name (only if new)", value="" if show_name else "")

        can_pay = bool(st.session_state.current_order_id)
        if st.button("💳 Mark as Paid", disabled=not can_pay):
            if not cn:
                st.warning("Customer number is required.")
            else:
                invoice_id, err = finalize_payment(
                    st.session_state.current_order_id, cn, new_name if not show_name else show_name
                )
                if err:
                    st.error(err)
                else:
                    st.success(f"Paid. Invoice {invoice_id} created.")
                    # clear local state but keep order record as Paid
                    st.session_state.order_cart = []
                    st.session_state.current_order_id = None
                    st.rerun()
