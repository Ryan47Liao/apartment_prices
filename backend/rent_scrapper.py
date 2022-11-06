import sys

sys.path.append("./..")

import pymysql
pymysql.install_as_MySQLdb()

from bs4 import BeautifulSoup
import requests
import re
import tqdm
import pytz
import datetime
import pandas as pd
from sqlalchemy import create_engine
from backend.database_manager import DataBaseManager
class Arkadia_scrapper:
    def __init__(self):
        pass

    def get_listings(self, ):
        url = "https://arkadiawestloop.securecafe.com/onlineleasing/arkadia-west-loop/floorplans.aspx?_ga=2.145610804.1012139687.1667357315-788274914.1665976718&_gac=1.92130024.1667426277.Cj0KCQjwqoibBhDUARIsAH2OpWhB1oR2tXM_4IY7UscPCBpVLsM7CjsKpNxpW66yiKUX7ql6Prf6F60aAoajEALw_wcB"
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        target_html = [i for i in soup.find_all('script') if str(i).__contains__('var pageData = {')][0]
        listings = [i.split('location.href=')[1].strip("'") for i in
                    re.findall("""availableUnitsURL: "location\.href='https:.+'""", target_html.text)]
        return listings

    def _fetch_info(self, tag):
        # 1. Find unit number
        unit_number = re.findall('#\d{4}', tag.text)[0]
        # 2. Find
        try:
            price_range = re.findall('\$.+-\$.+\d', tag.text)[0]
        except IndexError:
            price_range = None
        # 3. foot-sqr
        if price_range is None:
            delim = 'Call'
        else:
            delim = '$'
        foot_sqr = int(tag.text.split(delim)[0][5:])
        # 4. avaliable date
        try:
            date = re.findall('\d+\/\d+\/\d{4}', tag.text)[0]
        except IndexError:
            date = None
        return unit_number, foot_sqr, price_range, date

    def fetch_info(self, url):
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        # Find all listings nested under the same plan
        floor_plans = soup.find_all('h3')
        if len(floor_plans) > 1:
            return
        floor_plan = floor_plans[0].text.strip('<h3>').rstrip('</h3>')
        infos = [self._fetch_info(tag) for tag in soup.find_all("tr", {"class": "AvailUnitRow"})]
        df = pd.DataFrame(infos, columns=['room_number', 'Sq.Ft', 'price_range', 'Avaliable_date'])
        df['floor_plan'] = floor_plan
        # df['url'] = url
        return df

    def clean_data(self, df):
        df = df.copy()
        df.drop_duplicates(subset='room_number', inplace=True)
        df['price_floor'] = df.price_range.apply(
            lambda x: int(x.split('-')[0].replace(',', '').strip('$')) if (
                    x is not None and not pd.isnull(x)) else None)
        df['price_ceil'] = df.price_range.apply(
            lambda x: int(x.split('-')[1].replace(',', '').strip('$')) if (
                    x is not None and not pd.isnull(x)) else None)
        df['Floor_Plan'] = df.floor_plan.apply(lambda x: x.split(':')[1].split(' - ')[0])
        df['num_bedroom'] = df.floor_plan.apply(
            lambda x: x.split(':')[1].split(' - ')[1].split(',')[0].split(' ')[0])
        df['num_bathroom'] = df.floor_plan.apply(
            lambda x: x.split(':')[1].split(' - ')[1].split(',')[1].split(' ')[1])
        df.drop(columns=['price_range', 'floor_plan'], inplace=True)
        if 'date_update' not in df.columns:
            current_time = datetime.datetime.now(pytz.timezone('US/Central'))
            print("Updated {} rows at {}".format(df.shape[0],current_time))
            df['date_update'] = pd.to_datetime(str(current_time).split('.')[0])
        df['apartment'] = 'Arkadia'
        df['Avaliable_date'] = pd.to_datetime(df.Avaliable_date)
        return df.reset_index(drop=True)

    def main(self):
        url_lists = self.get_listings()
        dfs = [self.fetch_info(url) for url in tqdm.tqdm(url_lists)]
        df_final = self.clean_data(pd.concat(dfs))
        return df_final


    # pool = create_engine(url=create_url(**config),
    #                      pool_size=20, max_overflow=0,
    #                      )
    # _update_meta(df, pool, 'Arkadia')
    # df = df[['room_number', 'apartment', 'date_update', 'price_floor', 'price_ceil']]
    # with pool.connect() as conn:
    #     df.to_sql('prices', conn, if_exists='append', index=False)
    #     pass


# def _update_meta(df, pool, apartment='Arkadia'):
#     try:
#         df_meta = pd.read_sql("select * from room_meta where apartment = '{}';".format(apartment), pool)
#         existing_metas = df_meta.room_number.to_list()
#     except:
#         existing_metas = []
#     out_dfs = []
#     for room_number, df_group in df.groupby('room_number'):
#         if room_number not in existing_metas:
#             out_dfs.append(df_group[['room_number', 'apartment', 'Avaliable_date', 'Sq.Ft', 'Floor_Plan', 'num_bedroom',
#                                      'num_bathroom']])
#     if len(out_dfs) > 0:
#         with pool.connect() as conn:
#             pd.concat(out_dfs).to_sql('room_meta', conn, if_exists='append', index=False)
#         print(len(out_dfs), 'new rooms entered the market')


if __name__ == '__main__':
    import time
    try:
        scarpper_west_arkadia = Arkadia_scrapper()
        db = DataBaseManager()
        df = scarpper_west_arkadia.main()
        db.push_newest_data(df)
    except Exception as e:
        print(e)
    # for i in range(5):
    #     print(i)
    #     time.sleep(1)
