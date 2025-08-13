import streamlit as st
import pandas as pd
from db import init_db, fetch_df, query_db
from bom_handler import ensure_bom_seeded, INGREDIENT_UNITS, DEFAULT_BOM

# Admin credentials
ADMIN_ID = "123"
ADMIN_PASS = "456"


def ensure_safety_stock_column():
    """Add safety_stock column to inventory if not exists."""
    query_db("""
        IF COL_LENGTH('inventory', 'safety_stock') IS NULL
        BEGIN
            ALTER TABLE inventory ADD safety_stock FLOAT DEFAULT 0
        END
    """, ignore_errors=True)


def reset_inventory_from_bom():
    """
    Completely reset the inventory table to match DEFAULT_BOM.
    All old records will be deleted and replaced.
    """
    ensure_bom_seeded()
    ensure_safety_stock_column()

    # Extract all unique ingredients from the BOM
    ingredients = {ing for ing_map in DEFAULT_BOM.values() for ing in ing_map.keys()}

    # Delete existing data
    query_db("DELETE FROM inventory")

    # Prepare fresh data
    rows = [(ing, 0, INGREDIENT_UNITS.get(ing, ""), 0) for ing in ingredients]

    # Insert all in one go (SQL Server style)
    for row in rows:
        query_db("""
            INSERT INTO inventory (ingredient, quantity, unit, safety_stock)
            VALUES (?, ?, ?, ?)
        """, row)

    return list(ingredients)


def load_inventory_df_ordered(ingredients):
    """Load inventory as a DataFrame, ensuring all BOM ingredients appear in correct order."""
    df = fetch_df("""
        SELECT ingredient AS Ingredient, 
               quantity AS Quantity, 
               unit AS Unit, 
               safety_stock AS [Safety Stock]
        FROM inventory
    """)
    # Add missing BOM ingredients
    for ing in ingredients:
        if ing not in df["Ingredient"].values:
            df.loc[len(df)] = [ing, 0, INGREDIENT_UNITS.get(ing, ""), 0]
    return df.set_index("Ingredient").reindex(ingredients).reset_index()


def save_inventory_df(df):
    """
    Save updated inventory to SQL Server.
    Uses MERGE to update existing records or insert if missing.
    """
    for _, row in df.iterrows():
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
        """, (
            row["Ingredient"],
            float(row["Quantity"]),
            row.get("Unit", ""),
            float(row.get("Safety Stock", 0))
        ))


def inventory_page():
    """Streamlit Inventory Management page."""
    init_db()
    st.header("📦 Inventory Management")

    # Always reset inventory from BOM at page load
    ingredients = reset_inventory_from_bom()
    df = load_inventory_df_ordered(ingredients)

    # Display-friendly names with units
    display_names = [
        f"{ing} ({INGREDIENT_UNITS.get(ing, '')})" if INGREDIENT_UNITS.get(ing) else ing 
        for ing in ingredients
    ]
    df_disp = df.copy()
    df_disp["Ingredient"] = display_names

    # Session state controls
    if "inventory_edit_enabled" not in st.session_state:
        st.session_state.inventory_edit_enabled = False
    if "login_prompt" not in st.session_state:
        st.session_state.login_prompt = False

    # Show edit button
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
        st.dataframe(df_disp, use_container_width=True)
        return

    # Editing mode
    if st.session_state.inventory_edit_enabled:
        st.success("✅ Editing enabled. You can now update inventory.")
        edited = st.data_editor(df_disp, num_rows="fixed", use_container_width=True)

        # Keep original ingredient keys for saving
        edited["Ingredient"] = ingredients
        edited["Unit"] = [INGREDIENT_UNITS.get(ing, "") for ing in ingredients]

        if st.button("💾 Save Inventory"):
            save_inventory_df(edited)
            st.success("Inventory saved successfully!")
            st.session_state.inventory_edit_enabled = False
            st.session_state.login_prompt = False
            st.rerun()

    # Read-only display
    if not st.session_state.inventory_edit_enabled and not st.session_state.login_prompt:
        st.dataframe(df_disp, use_container_width=True)
