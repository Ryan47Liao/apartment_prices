from sqlalchemy import create_engine
from backend.rent_scrapper import Arkadia_scrapper
import pandas as pd
from Logic.data_analyzer import DataAnalyzer
import datetime
# Localize
import sqlite3

da = DataAnalyzer()
with da.db.pool.connect() as conn:
    sql = 'SELECT * FROM apartments.room_meta'
    df_meta = pd.read_sql(sql, conn)
    df_price = pd.read_sql('SELECT * FROM apartments.prices', conn)
    with sqlite3.connect("../database/apartments.db") as conn1:
        df_meta.to_sql('room_meta', conn1, if_exists='append')
        print(pd.read_sql('SELECT * FROM room_meta', conn1).shape)
        df_price.to_sql('prices', conn1, if_exists='append')
        print(pd.read_sql('SELECT * FROM prices', conn1).shape)
