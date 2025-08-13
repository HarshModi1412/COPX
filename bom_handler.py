# bom_handler.py
from db import init_db, query_db, fetch_df
from typing import List, Dict

# Ingredients + display units
INGREDIENT_UNITS = {
    "Espresso Beans": "g",
    "Milk": "ml",
    "Hot Water": "ml",
    "Chocolate Syrup": "g"
}

# DEFAULT BOM for 5 café items (per 1 unit sold)
# You can tweak these easily.
DEFAULT_BOM = {
    "C1001": {  # Espresso
        "Espresso Beans": 18
    },
    "C1002": {  # Cappuccino
        "Espresso Beans": 18,
        "Milk": 150
    },
    "C1003": {  # Latte
        "Espresso Beans": 18,
        "Milk": 200
    },
    "C1004": {  # Americano
        "Espresso Beans": 18,
        "Hot Water": 150
    },
    "C1005": {  # Mocha
        "Espresso Beans": 18,
        "Milk": 150,
        "Chocolate Syrup": 25
    }
}

def ensure_bom_seeded():
    """Seed the BOM table once if empty."""
    init_db()
    existing = query_db("SELECT COUNT(*) FROM bom", fetch=True)
    if existing and existing[0][0] > 0:
        return

    rows = []
    for pid, ing_map in DEFAULT_BOM.items():
        for ing, qty in ing_map.items():
            unit = INGREDIENT_UNITS.get(ing, "")
            rows.append((pid, ing, float(qty), unit))

    if rows:
        query_db("""
            INSERT INTO bom (product_id, ingredient, qty_per_unit, unit)
            VALUES (?, ?, ?, ?)
        """, many=True, seq=rows)

def calculate_deduction(cart: List[Dict]) -> Dict[str, float]:
    """
    cart = [{product_id, product_name, quantity, ...}, ...]
    returns {ingredient: total_qty_to_deduct}
    """
    ensure_bom_seeded()
    deduction = {}
    for item in cart:
        pid = item["product_id"]
        qty = float(item["quantity"])
        # fetch BOM for product
        df = fetch_df("SELECT ingredient, qty_per_unit FROM bom WHERE product_id=?", (pid,))
        for _, row in df.iterrows():
            ing = row["ingredient"]
            per_unit = float(row["qty_per_unit"])
            deduction[ing] = deduction.get(ing, 0.0) + per_unit * qty
    return deduction
