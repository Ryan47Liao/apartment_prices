# Standard library imports
import datetime
import os
import sys
sys.path.append("./..")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Third party imports
import pandas as pd
import streamlit as st

# Local application imports
# Use absolute import here, you might need to adjust it according to your actual project structure
from Logic.data_analyzer import DataAnalyzer

# Initialize DataAnalyzer
da = DataAnalyzer()

# Fetch and process data once at the start
da.fetch_new_data()
df = da.process_data()

# Constants
PAGE_CONFIG = {
    "page_title": "Apartment Watcher",
    "page_icon": ":house:",
    "layout": "wide"
}

# Set page configuration
st.set_page_config(**PAGE_CONFIG)

# Header
st.title("Semi real time apartment price historical data")
st.subheader("See the trend of apartment prices!")

# Slider constants
MIN_PRICE, MAX_PRICE = df['price_floor'].min(), df['price_floor'].max()
FLOOR_PLANS = df['Floor_Plan'].unique().tolist()

# Date constants
TODAY = datetime.date.today()
DEFAULT_START_DATE = TODAY - datetime.timedelta(days=30)
DEFAULT_END_DATE = TODAY

# Functions
def filter_dataframe(price_min, price_max, start_date, end_date, selected_floor_plans):
    # Fetch and process data when this is triggerd
    da.fetch_new_data()
    df = da.process_data()
    mask = (
        (df['price_floor'] > price_min) &
        (df['price_floor'] < price_max) &
        (df['Floor_Plan'].isin(selected_floor_plans)) &
        (df['date_update'] >= pd.to_datetime(start_date)) &
        (df['date_update'] <= pd.to_datetime(end_date))
    )
    return df[mask]

def plot_figures(df_plot):
    figs = [da.show_history(df_plot, 'Selected')]
    for Floor_Plan, df_floor_group in df_plot.groupby(['Floor_Plan', 'Sq.Ft']):
        figs.append(da.show_history(df_floor_group, Floor_Plan))
    return figs

def show_latest_images(price_min, price_max, start_date, end_date, selected_floor_plans):
    df_plot = filter_dataframe(price_min, price_max, start_date, end_date, selected_floor_plans)
    figs = plot_figures(df_plot)
    for fig in figs:
        st.pyplot(fig)

# Main
st.write("---")

# Slider
price_range = st.slider(
    "Enter the price range:",
    min_value=int(MIN_PRICE),
    max_value=int(MAX_PRICE),
    value=(int(MIN_PRICE), int(MAX_PRICE))
)

# Multi-select
selected_floor_plans = st.multiselect(
    "Choose the floor plans to display:",
    FLOOR_PLANS,
    default=FLOOR_PLANS  # Default to all floor plans
)

# Date inputs
start_date = st.date_input("Start date", value=DEFAULT_START_DATE)
end_date = st.date_input("End date", value=DEFAULT_END_DATE)

# Refresh button
refresh_btn = st.button("Refresh")

# Columns
img_col, txt_col = st.columns((2, 1))

if refresh_btn:
    with img_col:
        show_latest_images(MIN_PRICE, MAX_PRICE, start_date, end_date, selected_floor_plans)
    with txt_col:
        st.write(f"Last Update: {datetime.datetime.now()}")

refresh_btn = False

