import streamlit as st
import pandas as pd
from db import init_db, fetch_df, query_db
from bom_handler import ensure_bom_seeded, INGREDIENT_UNITS, DEFAULT_BOM

ADMIN_ID = "123"
ADMIN_PASS = "456"

def ensure_safety_stock_column():
    """Add safety_stock column to inventory if not exists."""
    query_db("ALTER TABLE inventory ADD COLUMN safety_stock REAL DEFAULT 0", ignore_errors=True)

def ensure_all_bom_ingredients_in_inventory():
    """Ensure every ingredient from DEFAULT_BOM exists in inventory with correct unit."""
    ensure_bom_seeded()
    ensure_safety_stock_column()
    ingredients = {ing for ing_map in DEFAULT_BOM.values() for ing in ing_map.keys()}

    for ing in ingredients:
        unit = INGREDIENT_UNITS.get(ing, "")
        query_db("""
            INSERT OR IGNORE INTO inventory (ingredient, quantity, unit, safety_stock)
            VALUES (?, 0, ?, 0)
        """, (ing, unit))
    return list(ingredients)

def load_inventory_df_ordered(ingredients):
    df = fetch_df("""
        SELECT ingredient AS Ingredient, 
               quantity AS Quantity, 
               unit AS Unit, 
               safety_stock AS 'Safety Stock'
        FROM inventory
    """)
    for ing in ingredients:
        if ing not in df["Ingredient"].values:
            df.loc[len(df)] = [ing, 0, INGREDIENT_UNITS.get(ing, ""), 0]
    return df.set_index("Ingredient").reindex(ingredients).reset_index()

def save_inventory_df(df):
    rows_to_update = [
        (row["Ingredient"], float(row["Quantity"]), row.get("Unit", ""), float(row.get("Safety Stock", 0)))
        for _, row in df.iterrows()
    ]
    query_db("""
        INSERT INTO inventory (ingredient, quantity, unit, safety_stock)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(ingredient) DO UPDATE SET 
            quantity=excluded.quantity,
            unit=excluded.unit,
            safety_stock=excluded.safety_stock
    """, many=True, seq=rows_to_update)

def inventory_page():
    init_db()
    st.header("📦 Inventory Management")

    ingredients = ensure_all_bom_ingredients_in_inventory()
    df = load_inventory_df_ordered(ingredients)

    display_names = [
        f"{ing} ({INGREDIENT_UNITS.get(ing, '')})" if INGREDIENT_UNITS.get(ing) else ing 
        for ing in ingredients
    ]
    df_disp = df.copy()
    df_disp["Ingredient"] = display_names

    if "inventory_edit_enabled" not in st.session_state:
        st.session_state.inventory_edit_enabled = False
    if "login_prompt" not in st.session_state:
        st.session_state.login_prompt = False

    if not st.session_state.inventory_edit_enabled and not st.session_state.login_prompt:
        if st.button("🔓 Enable Editing"):
            st.session_state.login_prompt = True
            st.rerun()

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
        st.dataframe(df_disp, use_container_width=True)
        return

    if st.session_state.inventory_edit_enabled:
        st.success("✅ Editing enabled. You can now update inventory.")

        edited = st.data_editor(df_disp, num_rows="fixed", use_container_width=True)

        edited["Ingredient"] = ingredients
        edited["Unit"] = [INGREDIENT_UNITS.get(ing, "") for ing in ingredients]

        if st.button("💾 Save Inventory"):
            save_inventory_df(edited)
            st.success("Inventory saved successfully!")
            st.session_state.inventory_edit_enabled = False
            st.session_state.login_prompt = False
            st.rerun()

    if not st.session_state.inventory_edit_enabled and not st.session_state.login_prompt:
        st.dataframe(df_disp, use_container_width=True)
