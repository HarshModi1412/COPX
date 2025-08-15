# app.py
import streamlit as st
from billing import billing_page
from inventory import inventory_page
from billing_history import billing_history_page
from analyst import analyst_page
from orders import orders_page          # <-- add this import
from db import init_db

st.set_page_config(page_title="Cafe POS & Inventory", layout="wide")
init_db()

menu = st.sidebar.radio(
    "Navigation",
    ["Orders", "Billing", "Inventory Management", "Billing History", "Business Analyst"]  # <-- add Orders
)

if menu == "Orders":
    orders_page()
elif menu == "Billing":
    billing_page()
elif menu == "Inventory Management":
    inventory_page()
elif menu == "Billing History":
    billing_history_page()
elif menu == "Business Analyst":
    analyst_page()
