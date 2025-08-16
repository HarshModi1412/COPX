import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from db import init_db, fetch_df, query_db
from bom_handler import ensure_bom_seeded, INGREDIENT_UNITS, DEFAULT_BOM

# Admin credentials
ADMIN_ID = "123"
ADMIN_PASS = "456"

# --- DB helpers ---
def ensure_safety_stock_column():
    query_db("""
        IF COL_LENGTH('inventory', 'safety_stock') IS NULL
        BEGIN
            ALTER TABLE inventory 
            ADD safety_stock FLOAT NULL 
            CONSTRAINT DF_inventory_safety_stock DEFAULT 0 WITH VALUES;
        END
    """, ignore_errors=True)

def get_all_bom_ingredients():
    ingredients = []
    for recipe in DEFAULT_BOM.values():
        for ing in recipe.keys():
            if ing not in ingredients:
                ingredients.append(ing)
    return ingredients

def sync_inventory_with_bom():
    """Insert BOM ingredients if missing, keep existing ones."""
    ensure_bom_seeded()
    ensure_safety_stock_column()
    ingredients = get_all_bom_ingredients()
    for ing in ingredients:
        unit = INGREDIENT_UNITS.get(ing, "")
        query_db("""
            MERGE inventory AS target
            USING (SELECT ? AS ingredient, CAST(0 AS FLOAT) AS quantity, ? AS unit, CAST(0 AS FLOAT) AS safety_stock) AS source
            ON target.ingredient = source.ingredient
            WHEN NOT MATCHED THEN
                INSERT (ingredient, quantity, unit, safety_stock)
                VALUES (source.ingredient, source.quantity, source.unit, source.safety_stock);
        """, (ing, unit))

def load_full_inventory_df():
    """Load the full inventory without dropping any rows."""
    df = fetch_df("""
        SELECT ingredient AS Ingredient,
               quantity AS Quantity,
               unit AS Unit,
               safety_stock AS [Safety Stock]
        FROM inventory
    """)
    if df is None:
        df = pd.DataFrame(columns=["Ingredient", "Quantity", "Unit", "Safety Stock"])
    return df

# --- Logging helpers ---
def _get_self_life_days(ingredient: str) -> int | None:
    """Fetch shelf life (in days) from SQL Server table `self_life` for the given ingredient."""
    try:
        df = fetch_df("SELECT self_life_days FROM self_life WHERE ingredient = ?", (ingredient.strip(),))
        if df is not None and not df.empty:
            val = df.iloc[0]["self_life_days"]
            if pd.notna(val):
                return int(val)
    except Exception as e:
        print(f"[WARN] Shelf life lookup failed for {ingredient}: {e}")
    return None

def log_inventory_change(ingredient, old_qty, new_qty):
    """Insert inventory change into logs, including use_before date."""
    try:
        if old_qty is None or new_qty is None:
            return

        old_qty = float(old_qty)
        new_qty = float(new_qty)
        diff = new_qty - old_qty

        if diff == 0:
            return  # no change, skip

        change_type = "Added" if diff > 0 else "Wasted"

        # ✅ Fetch shelf life days from self_life table
        shelf_life = _get_self_life_days(str(ingredient))

        # ✅ Default fallback if not found
        if not shelf_life or shelf_life <= 0:
            shelf_life = 7  # fallback

        # ✅ Use the current timestamp of change as base
        ts_now = datetime.now()
        use_before = (ts_now + timedelta(days=shelf_life)).date()

        # ✅ Prepare parameters
        params = (
            str(ingredient),
            change_type,
            float(abs(diff)),
            old_qty,
            new_qty,
            ts_now,       # log timestamp
            use_before    # calculated based on shelf life
        )

        query_db("""
            INSERT INTO inventory_logs 
                (ingredient, change_type, quantity_changed, old_quantity, new_quantity, timestamp, use_before)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, params)

    except Exception as e:
        print(f"[ERROR] log_inventory_change failed: {e}")

def save_inventory_df(df: pd.DataFrame):
    """Upsert inventory changes and log them."""
    for _, row in df.iterrows():
        ing = row["Ingredient"].strip()
        new_qty = float(row["Quantity"]) if pd.notna(row["Quantity"]) else 0.0
        unit = row.get("Unit", "") or ""
        safety_stock = float(row.get("Safety Stock", 0.0)) if pd.notna(row.get("Safety Stock", 0.0)) else 0.0

        # Get old quantity (None if not found)
        old_qty_row = fetch_df("SELECT quantity FROM inventory WHERE ingredient = ?", (ing,))
        old_qty = old_qty_row.iloc[0]["quantity"] if old_qty_row is not None and not old_qty_row.empty else None

        # Update / insert inventory
        query_db("""
            MERGE inventory AS target
            USING (SELECT ? AS ingredient, ? AS quantity, ? AS unit, ? AS safety_stock) AS source
            ON target.ingredient = source.ingredient
            WHEN MATCHED THEN
                UPDATE SET 
                    quantity = source.quantity,
                    unit = source.unit,
                    safety_stock = source.safety_stock
            WHEN NOT MATCHED THEN
                INSERT (ingredient, quantity, unit, safety_stock)
                VALUES (source.ingredient, source.quantity, source.unit, source.safety_stock);
        """, (str(ing), float(new_qty), str(unit), float(safety_stock)))

        # Log change if quantity differs
        if old_qty is not None:
            log_inventory_change(ing, old_qty, new_qty)

# --- UI Page ---
def inventory_page():
    # Ensure DB schema is ready (including inventory_logs created in db.init_db)
    init_db()

    st.header("📦 Inventory Management")

    # Ensure BOM ingredients exist (no deletion of extras)
    sync_inventory_with_bom()

    # Load data
    df = load_full_inventory_df()

    # Display names with units (purely visual)
    df_disp = df.copy()
    df_disp["Ingredient"] = [
        f"{ing} ({unit})" if unit else ing
        for ing, unit in zip(df["Ingredient"], df["Unit"])
    ]

    # Make table taller
    st.markdown("""
        <style>
        .stDataFrame, .stDataEditor {
            height: auto !important;
            max-height: none !important;
        }
        </style>
    """, unsafe_allow_html=True)

    # Session state
    if "inventory_edit_enabled" not in st.session_state:
        st.session_state.inventory_edit_enabled = False
    if "login_prompt" not in st.session_state:
        st.session_state.login_prompt = False

    # Enable editing button
    if not st.session_state.inventory_edit_enabled and not st.session_state.login_prompt:
        if st.button("🔓 Enable Editing"):
            st.session_state.login_prompt = True
            st.rerun()

    # Login form
    if st.session_state.login_prompt and not st.session_state.inventory_edit_enabled:
        with st.form("auth_form", clear_on_submit=True):
            uid = st.text_input("Enter User ID")
            pwd = st.text_input("Enter Password", type="password")
            ok = st.form_submit_button("Login")
        if ok:
            if uid == ADMIN_ID and pwd == ADMIN_PASS:
                st.session_state.inventory_edit_enabled = True
                st.session_state.login_prompt = False
                st.rerun()
            else:
                st.error("❌ Invalid ID or password.")

        st.dataframe(df_disp, use_container_width=True, height=800)
        return

    # Editing mode
    if st.session_state.inventory_edit_enabled:
        st.success("✅ Editing enabled. Update inventory values below.")
        edited = st.data_editor(df_disp, num_rows="fixed", use_container_width=True, height=800)

        # Map visual name back to raw Ingredient
        edited["Ingredient"] = df["Ingredient"]

        if st.button("💾 Save Inventory"):
            save_inventory_df(edited)
            st.success("Inventory saved successfully! Changes logged in history.")
            st.session_state.inventory_edit_enabled = False
            st.rerun()

    # Read-only view
    if not st.session_state.inventory_edit_enabled and not st.session_state.login_prompt:
        st.dataframe(df_disp, use_container_width=True, height=800)

