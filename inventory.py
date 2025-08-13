# inventory.py
import streamlit as st
import pandas as pd
from db import init_db, fetch_df, query_db
from bom_handler import ensure_bom_seeded, INGREDIENT_UNITS, DEFAULT_BOM

# Admin credentials
ADMIN_ID = "123"
ADMIN_PASS = "456"


def ensure_safety_stock_column():
    """Add safety_stock column to inventory if it doesn't exist (SQL Server)."""
    query_db("""
        IF COL_LENGTH('inventory', 'safety_stock') IS NULL
        BEGIN
            ALTER TABLE inventory ADD safety_stock FLOAT NULL CONSTRAINT DF_inventory_safety_stock DEFAULT 0 WITH VALUES
        END
    """, ignore_errors=True)


def sync_inventory_with_bom():
    """
    Add any BOM ingredients that are missing from inventory (no deletes).
    Returns the full list of BOM ingredients (for ordering/display).
    """
    ensure_bom_seeded()
    ensure_safety_stock_column()

    ingredients = {ing for ing_map in DEFAULT_BOM.values() for ing in ing_map.keys()}

    # Insert only the missing ones via MERGE
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

    return list(ingredients)


def reset_inventory_from_bom():
    """
    Completely replace inventory contents from BOM (admin action).
    """
    ensure_bom_seeded()
    ensure_safety_stock_column()

    ingredients = {ing for ing_map in DEFAULT_BOM.values() for ing in ing_map.keys()}

    # Safer than TRUNCATE (in case of FKs)
    query_db("DELETE FROM inventory")

    for ing in ingredients:
        query_db("""
            INSERT INTO inventory (ingredient, quantity, unit, safety_stock)
            VALUES (?, ?, ?, ?)
        """, (ing, 0.0, INGREDIENT_UNITS.get(ing, ""), 0.0))

    return list(ingredients)


def load_inventory_df_ordered(ingredients: list[str]):
    """Load inventory as a DataFrame, ensuring every BOM ingredient appears (ordered by BOM)."""
    df = fetch_df("""
        SELECT ingredient AS Ingredient, 
               quantity AS Quantity, 
               unit AS Unit, 
               safety_stock AS [Safety Stock]
        FROM inventory
    """)

    if df is None or df.empty:
        df = pd.DataFrame(columns=["Ingredient", "Quantity", "Unit", "Safety Stock"])

    # Ensure all BOM ingredients are present in the dataframe (default zeros)
    for ing in ingredients:
        if ing not in df["Ingredient"].values:
            df.loc[len(df)] = [ing, 0.0, INGREDIENT_UNITS.get(ing, ""), 0.0]

    return (
        df.set_index("Ingredient")
          .reindex(ingredients)
          .reset_index()
    )


def save_inventory_df(df: pd.DataFrame):
    """Upsert each row via MERGE (SQL Server)."""
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
            float(row["Quantity"]) if pd.notna(row["Quantity"]) else 0.0,
            row.get("Unit", "") or "",
            float(row.get("Safety Stock", 0.0)) if pd.notna(row.get("Safety Stock", 0.0)) else 0.0
        ))


def inventory_page():
    """Streamlit Inventory Management page (SQL Server–safe)."""
    init_db()
    st.header("📦 Inventory Management")

    # Ensure all BOM ingredients exist (no deletes)
    ingredients = sync_inventory_with_bom()
    df = load_inventory_df_ordered(ingredients)

    # Display-friendly names with units
    display_names = [
        f"{ing} ({INGREDIENT_UNITS.get(ing, '')})" if INGREDIENT_UNITS.get(ing) else ing
        for ing in ingredients
    ]
    df_disp = df.copy()
    df_disp["Ingredient"] = display_names

    # Session state
    if "inventory_edit_enabled" not in st.session_state:
        st.session_state.inventory_edit_enabled = False
    if "login_prompt" not in st.session_state:
        st.session_state.login_prompt = False
    if "reset_mode" not in st.session_state:
        st.session_state.reset_mode = False

    # Admin reset button (explicit action)
    if st.button("♻ Reset Inventory from BOM (Admin)"):
        st.session_state.login_prompt = True
        st.session_state.reset_mode = True
        st.rerun()

    # Enable editing
    if not st.session_state.inventory_edit_enabled and not st.session_state.login_prompt:
        if st.button("🔓 Enable Editing"):
            st.session_state.login_prompt = True
            st.session_state.reset_mode = False
            st.rerun()

    # Admin login gate
    if st.session_state.login_prompt and not st.session_state.inventory_edit_enabled:
        with st.form("auth_form", clear_on_submit=True):
            uid = st.text_input("Enter User ID")
            pwd = st.text_input("Enter Password", type="password")
            ok = st.form_submit_button("Login")
        if ok:
            if uid == ADMIN_ID and pwd == ADMIN_PASS:
                if st.session_state.reset_mode:
                    reset_inventory_from_bom()
                    st.success("✅ Inventory fully reset from BOM.")
                else:
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

        # Map back to raw ingredient keys for saving
        edited["Ingredient"] = ingredients
        edited["Unit"] = [INGREDIENT_UNITS.get(ing, "") for ing in ingredients]

        if st.button("💾 Save Inventory"):
            save_inventory_df(edited)
            st.success("Inventory saved successfully!")
            st.session_state.inventory_edit_enabled = False
            st.rerun()

    # Read-only display
    if not st.session_state.inventory_edit_enabled and not st.session_state.login_prompt:
        st.dataframe(df_disp, use_container_width=True)
