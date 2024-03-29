import sys

sys.path.append("./..")

import pymysql

pymysql.install_as_MySQLdb()

from sqlalchemy import create_engine
import pandas as pd


class DataBaseManager:
    def __init__(self, config=None, local=True):
        if config is None:
            config = {
                'user': "admin",
                'password': "a19990721",
                'host': "capstone.cfykpx21wk0f.us-east-2.rds.amazonaws.com",
                'database': "apartments",
                'port': '3306'}
        self.config = config
        if local:
            self.pool = create_engine('sqlite:///../database/apartments.db', echo=True)
        else:
            self.pool = create_engine(url=DataBaseManager.create_url(**self.config),
                                      pool_size=20, max_overflow=0
                                      )

    @staticmethod
    def create_url(user, password, host, database, port):
        return f"mysql://{user}:{password}@{host}:{port}/{database}"

    def get_all_data(self):
        with self.pool.connect() as conn:
            sql = """SELECT m.room_number ,
        m.apartment ,
        date_update ,
        price_floor ,
        price_ceil ,
        Avaliable_date ,
        `Sq.Ft` ,
        Floor_Plan ,
        num_bedroom ,
        num_bathroom
        FROM prices p
        	LEFT JOIN room_meta m
        		ON p.apartment = m.apartment AND p.room_number = m.room_number;"""
            df = pd.read_sql(sql, conn)
        return df

    def push_newest_data(self, df):
        self._update_meta(df, self.pool, 'Arkadia')
        df = df[['room_number', 'apartment', 'date_update', 'price_floor', 'price_ceil']]
        with self.pool.connect() as conn:
            df.to_sql('prices', conn, if_exists='append', index=False)
            pass

    def _update_meta(self, df, pool, apartment='Arkadia'):
        try:
            df_meta = pd.read_sql("select * from room_meta where apartment = '{}';".format(apartment), pool)
            existing_metas = df_meta.room_number.to_list()
        except:
            existing_metas = []
        out_dfs = []
        for room_number, df_group in df.groupby('room_number'):
            if room_number not in existing_metas:
                out_dfs.append(
                    df_group[['room_number', 'apartment', 'Avaliable_date', 'Sq.Ft', 'Floor_Plan', 'num_bedroom',
                              'num_bathroom']])
        if len(out_dfs) > 0:
            with pool.connect() as conn:
                pd.concat(out_dfs).to_sql('room_meta', conn, if_exists='append', index=False)
            print(len(out_dfs), 'new rooms entered the market')
