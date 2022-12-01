import sys

sys.path.append("..")

import pandas as pd
from Logic.data_analyzer import DataAnalyzer
# Localize
import sqlite3

da = DataAnalyzer(local=False)
with da.db.pool.connect() as conn:
    sql = 'SELECT * FROM apartments.room_meta_old'
    df_meta = pd.read_sql(sql, conn)
    df_price = pd.read_sql('SELECT * FROM apartments.prices_old', conn)
    with sqlite3.connect("../database/apartments.db") as conn1:
        df_meta.to_sql('room_meta', conn1, if_exists='append')
        df_price.to_sql('prices', conn1, if_exists='append')
    with sqlite3.connect("../database/apartments.db") as conn2:
        print(pd.read_sql('SELECT * FROM room_meta', conn2).shape)
        print(pd.read_sql('SELECT * FROM prices', conn2).shape)
