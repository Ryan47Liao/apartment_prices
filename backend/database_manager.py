from sqlalchemy import create_engine
import pandas as pd

class DataBaseManager:
    def __init__(self, config=None):
        if config is None:
            config = {
            'user': "admin",
            'password': "a19990721",
            'host': "capstone.cfykpx21wk0f.us-east-2.rds.amazonaws.com",
            'database': "Features",
            'port': '3306'}
        self.config = config
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
        FROM apartments.prices p
        	LEFT JOIN apartments.room_meta m
        		ON p.apartment = m.apartment AND p.room_number = m.room_number;"""
            df = pd.read_sql(sql, conn)
        return df

    def push_newest_data(self,df):
        config = {
                'user': "admin",
                'password': "a19990721",
                'host': "capstone.cfykpx21wk0f.us-east-2.rds.amazonaws.com",
                'database': "Features",
                'port': '3306'}

        # scarpper_west_arkadia = Arkadia_scrapper()
        # df = scarpper_west_arkadia.main()
        pool = create_engine(url=DataBaseManager.create_url(**config),
                             pool_size=20, max_overflow=0,
                             )
        self._update_meta(df, pool, 'Arkadia')
        df = df[['room_number', 'apartment', 'date_update', 'price_floor', 'price_ceil']]
        with pool.connect() as conn:
            df.to_sql('prices', conn, if_exists='append', index=False)
            pass


    def _update_meta(self,df, pool, apartment='Arkadia'):
        try:
            df_meta = pd.read_sql("select * from room_meta where apartment = '{}';".format(apartment), pool)
            existing_metas = df_meta.room_number.to_list()
        except:
            existing_metas = []
        out_dfs = []
        for room_number, df_group in df.groupby('room_number'):
            if room_number not in existing_metas:
                out_dfs.append(df_group[['room_number', 'apartment', 'Avaliable_date', 'Sq.Ft', 'Floor_Plan', 'num_bedroom',
                                         'num_bathroom']])
        if len(out_dfs) > 0:
            with pool.connect() as conn:
                pd.concat(out_dfs).to_sql('room_meta', conn, if_exists='append', index=False)
            print(len(out_dfs), 'new rooms entered the market')