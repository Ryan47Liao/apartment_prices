from backend.database_manager import DataBaseManager
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

class DataAnalyzer:
    def __init__(self, config=None, local=True):
        self.db = DataBaseManager(config=config,local=local)
        self.data_raw = self.db.get_all_data()

    def fetch_new_data(self):
        self.data_raw = self.db.get_all_data()

    def process_data(self):
        df = self.data_raw.copy()
        # Change dtypes
        df.num_bedroom = df.num_bedroom.apply(lambda x:int(x) if not np.isnan(x) else None)
        df.num_bathroom = df.num_bedroom.apply(lambda x: int(x) if not np.isnan(x) else None)
        #
        df = df.merge(df.groupby(['Sq.Ft', 'date_update']).price_floor.mean().reset_index().rename(
            {'price_floor': 'price_mean_horizontal'}, axis=1),
            on=['Sq.Ft', 'date_update'])
        df = df.merge(
            df.groupby('room_number').price_floor.mean().reset_index().rename({'price_floor': 'price_mean_vertical'},
                                                                              axis=1),
            on='room_number')
        df['deviation_from_cohort'] = df.eval('price_floor - price_mean_horizontal')
        df['deviation_from_history'] = df.eval('price_floor - price_mean_vertical')
        df['price_sqft'] = df.eval('price_floor / `Sq.Ft`')
        df.date_update = pd.DatetimeIndex(df.date_update)
        return df

    def show_history(self, df_floor_group, title, by='price_floor'):
        legends = []
        fig, ax = plt.subplots()
        fig.set_size_inches(20,10)
        #plt.figure(figsize=(20, 10))
        for group, df_group in df_floor_group.groupby('room_number'):
            legends.append(group + '|' + str(df_group['Sq.Ft'].to_list()[-1]))
            df_group = df_group.set_index('date_update')
            ax.plot(df_group.index, df_group[by])
        for group, df_group in df_floor_group.groupby('room_number'):
            # legends.append(group+'|'+str(df_group['Sq.Ft'].to_list()[-1]))
            df_group = df_group.set_index('date_update')
            ax.scatter(df_group.index, df_group[by])
        ax.legend(legends, loc='lower left', bbox_to_anchor=(1.01, 0., 0.5, 0.5))
        ax.set_title(title)
        return fig

if __name__ == '__main__':
    da = DataAnalyzer()
    df = da.process_data()
    fig = da.show_history(df, 'ALL')
    plt.show()

