import streamlit as st
import pandas as pd
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

def save_inventory_df(df: pd.DataFrame):
    """Upsert inventory changes."""
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
    init_db()
    st.header("📦 Inventory Management")

    # Ensure BOM ingredients exist in table but keep all items
    sync_inventory_with_bom()

    # Load full table
    df = load_full_inventory_df()

    # Add display names with units
    df_disp = df.copy()
    df_disp["Ingredient"] = [
        f"{ing} ({unit})" if unit else ing
        for ing, unit in zip(df["Ingredient"], df["Unit"])
    ]

    # Make table bigger with CSS
    st.markdown("""
        <style>
        .stDataFrame, .stDataEditor {
            height: auto !important;
            max-height: none !important;
        }
        </style>
    """, unsafe_allow_html=True)

    # State setup
    if "inventory_edit_enabled" not in st.session_state:
        st.session_state.inventory_edit_enabled = False
    if "login_prompt" not in st.session_state:
        st.session_state.login_prompt = False

    # Enable editing button
    if not st.session_state.inventory_edit_enabled and not st.session_state.login_prompt:
        if st.button("🔓 Enable Editing"):
            st.session_state.login_prompt = True
            st.rerun()

    # Admin login
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

        # Map display names back to raw names
        edited["Ingredient"] = df["Ingredient"]

        if st.button("💾 Save Inventory"):
            save_inventory_df(edited)
            st.success("Inventory saved successfully!")
            st.session_state.inventory_edit_enabled = False
            st.rerun()

    # Read-only view
    if not st.session_state.inventory_edit_enabled and not st.session_state.login_prompt:
        st.dataframe(df_disp, use_container_width=True, height=800)
