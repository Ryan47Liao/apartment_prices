import datetime
import sys

sys.path.append("./..")

import streamlit as st
from Logic.data_analyzer import DataAnalyzer

da = DataAnalyzer()
#:house:
st.set_page_config(page_title='Apartment Watcher', page_icon=':house:', layout='wide')

# --header
with st.container()

    st.title('Semi real time apartment price historical data')
    st.subheader('See the trend of apartment prices!')

# -- Details
def show_latest_images():
    da.fetch_new_data()
    df = da.process_data()
    df_plot = df.query('price_floor < 2300 and `Sq.Ft` <= 799')
    ###
    figs = [da.show_history(df_plot, 'ALL')]
    for Floor_Plan, df_floor_group in df_plot.groupby(['Floor_Plan', 'Sq.Ft']):
        figs.append(da.show_history(df_floor_group, Floor_Plan))
    for fig in figs:
        st.pyplot(fig)

with st.container():
    st.write('---')
    st.write('Overall market trends')
    #
    refresh_btn = st.button('Refresh')
    # while True:
    img_col, txt_col = st.columns((2, 1))
    if refresh_btn:
        with img_col:
            show_latest_images()
        with txt_col:
            st.write('Last Update:{}'.format(datetime.datetime.now()))
        refresh_btn = False


