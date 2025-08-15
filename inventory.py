# inventory.py
import streamlit as st
import pandas as pd
from db import init_db, fetch_df, query_db
from bom_handler import ensure_bom_seeded, INGREDIENT_UNITS, DEFAULT_BOM

# Admin credentials
ADMIN_ID = "123"
ADMIN_PASS = "456"


# --- DB helpers ---
def ensure_safety_stock_column():
    """Ensure 'safety_stock' column exists in inventory table."""
    query_db("""
        IF COL_LENGTH('inventory', 'safety_stock') IS NULL
        BEGIN
            ALTER TABLE inventory 
            ADD safety_stock FLOAT NULL 
            CONSTRAINT DF_inventory_safety_stock DEFAULT 0 WITH VALUES;
        END
    """, ignore_errors=True)


def get_all_bom_ingredients():
    """
    Return BOM ingredients in a consistent list order.
    Flatten DEFAULT_BOM while preserving the insertion order of products & ingredients.
    """
    ingredients = []
    for recipe in DEFAULT_BOM.values():
        for ing in recipe.keys():
            if ing not in ingredients:  # Avoid duplicates while keeping order
                ingredients.append(ing)
    return ingredients


def sync_inventory_with_bom():
    """Ensure inventory contains all BOM ingredients (no deletions)."""
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

    return ingredients


def reset_inventory_from_bom():
    """Completely reset inventory table with BOM ingredients."""
    ensure_bom_seeded()
    ensure_safety_stock_column()

    ingredients = get_all_bom_ingredients()

    query_db("DELETE FROM inventory")

    for ing in ingredients:
        query_db("""
            INSERT INTO inventory (ingredient, quantity, unit, safety_stock)
            VALUES (?, ?, ?, ?)
        """, (ing, 0.0, INGREDIENT_UNITS.get(ing, ""), 0.0))

    return ingredients


def load_inventory_df_ordered(ingredients: list[str]):
    """Load inventory DataFrame ensuring all BOM ingredients appear in the given order."""
    df = fetch_df("""
        SELECT ingredient AS Ingredient,
               quantity AS Quantity,
               unit AS Unit,
               safety_stock AS [Safety Stock]
        FROM inventory
    """)

    if df is None or df.empty:
        df = pd.DataFrame(columns=["Ingredient", "Quantity", "Unit", "Safety Stock"])

    # Ensure every BOM ingredient is present
    for ing in ingredients:
        if ing not in df["Ingredient"].values:
            df.loc[len(df)] = [ing, 0.0, INGREDIENT_UNITS.get(ing, ""), 0.0]

    return df.set_index("Ingredient").reindex(ingredients).reset_index()


def save_inventory_df(df: pd.DataFrame):
    """Upsert all rows from the edited DataFrame."""
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


# --- UI Page ---
def inventory_page():
    """Streamlit Inventory Management Page."""
    init_db()
    st.header("📦 Inventory Management")

    # Sync with BOM
    ingredients = sync_inventory_with_bom()
    df = load_inventory_df_ordered(ingredients)

    # Create display names with units
    df_disp = df.copy()
    df_disp["Ingredient"] = [
        f"{ing} ({INGREDIENT_UNITS.get(ing, '')})" if INGREDIENT_UNITS.get(ing) else ing
        for ing in df["Ingredient"]
    ]

    # Session state setup
    if "inventory_edit_enabled" not in st.session_state:
        st.session_state.inventory_edit_enabled = False
    if "login_prompt" not in st.session_state:
        st.session_state.login_prompt = False
    if "reset_mode" not in st.session_state:
        st.session_state.reset_mode = False

    # Admin Reset
    if st.button("♻ Reset Inventory from BOM (Admin)"):
        st.session_state.login_prompt = True
        st.session_state.reset_mode = True
        st.rerun()

    # Enable Editing
    if not st.session_state.inventory_edit_enabled and not st.session_state.login_prompt:
        if st.button("🔓 Enable Editing"):
            st.session_state.login_prompt = True
            st.session_state.reset_mode = False
            st.rerun()

    # Admin login
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
        st.success("✅ Editing enabled. Update inventory values below.")
        edited = st.data_editor(df_disp, num_rows="fixed", use_container_width=True)

        # Map display names back to raw ingredient names
        edited["Ingredient"] = ingredients
        edited["Unit"] = [INGREDIENT_UNITS.get(ing, "") for ing in ingredients]

        if st.button("💾 Save Inventory"):
            save_inventory_df(edited)
            st.success("Inventory saved successfully!")
            st.session_state.inventory_edit_enabled = False
            st.rerun()

    # Read-only view
    if not st.session_state.inventory_edit_enabled and not st.session_state.login_prompt:
        st.dataframe(df_disp, use_container_width=True)
