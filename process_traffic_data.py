from configparser import ConfigParser
import sys
import pandas as pd
import threading
import time

import data_processing.process_utils as utils
from omnisci_connector.omni_connect import OmnisciConnect



def apply_custom_transformations(df, interest_col, threshold, grouper):

    # drop nas
    df = utils.grouped_drop_na(df, threshold, grouper=grouper, col=interest_col)

    # lower all column names

    df = utils.lower_col_names(df)

    df['state_pm'] = utils.state_pm_to_numeric(df['state_pm'])
    # drop all rows with na in absolute postmarker field
    df = df.dropna(subset=['abs_pm'])

    df['timestamp_'] = pd.to_datetime(df['timestamp_'], infer_datetime_format=True)

    # add a rounded timestamp for grouping later on: (e.g. 12:46 --> 13:00)
    df = utils.rounded_timestamp(df=df, name_current_ts='timestamp_', name_rounded_ts='timestamp_rounded', round_by='H')

    df = utils.add_day_of_week(df, 'timestamp_')

    # add column with hour of day for each data point
    df['hour_of_day'] = df['timestamp_'].dt.hour

    # add column with day of year for each data point
    df['day_of_year'] = df['timestamp_'].dt.dayofyear

    # downcast all ints to save memory
    df = utils.downcast_int(df, ['station', 'freeway', 'total_flow', 'lanes', 'county'])

    # downcast all floats to save memory
    df = utils.downcast_type(df)

    return df


table_name = "test_2"

# initial parameters for reading in traffic data
threshold = 0.1
interest_col = 'speed'
grouper = 'station'
batch_limit = 1
file_ext = '.txt'
thread_num = 1
DEBUG = True


class TrafficData:
    data_columns = ['timestamp_',
                    'station',
                    'district',
                    'freeway',
                    'direction',
                    'lane_type',
                    'station_length',
                    'samples',
                    'pct_observed',
                    'total_flow',
                    'occupancy',
                    'speed'
                    ]

    meta_columns = ['id',
                    'county',
                    'state_pm',
                    'abs_pm',
                    'latitude',
                    'longitude',
                    'lanes']

    def __init__(self, files, meta_file):
        self.files = files
        self.meta_file = meta_file
        self.metadata = None
        self.data = None
        self.df = None

        # Read in traffic data
        self.read_data()

        # Read in metatable
        self.read_meta()

        # Join metatable and data
        self.create_df()

    def read_meta(self):
        self.metadata = (pd.read_csv(self.meta_file, sep=',',usecols=self.meta_columns)
                         .rename(str.lower, axis='columns')
                         .set_index('id'))

    def read_data(self):
        no_data_cols = list(range(len(self.data_columns)))
        l_df = []
        for f in self.files:
            print("Processing file: ", f)
            temp = (pd.read_csv(f, header=None, names=self.data_columns, usecols=no_data_cols)
                    .drop('district', axis=1)
                    .drop('samples', axis=1)
                    .drop('pct_observed',axis=1))
            l_df.append(temp)

        self.data = pd.concat(l_df, ignore_index=True)

    def create_df(self):
        self.df = self.data.join(self.metadata, on='station')


def transform_and_load(config_path, paths, meta_path, thread):

    # Send data to omnisci:
    print("connect to omnisci with thread: ", thread)
    connection = OmnisciConnect(config_path)
    connection.start_connection()

    df_batch = []

    try:
        data = TrafficData(paths, meta_path)


        df_transformed = apply_custom_transformations(df=data.df,
                                                          interest_col=interest_col,
                                                          threshold=threshold,
                                                          grouper=grouper)
    except ValueError as ex:
        print(ex)
        print("could not process ", paths[0])
        return

    connection.load_data(table_name=table_name,
                         df=df_transformed,
                         method='infer',
                         create='infer')

    connection.close_connection()


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

t0 = time.time()
if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(len(sys.argv))
        raise TypeError("ERROR: need to provide path to config file.")

    # Configuration file reader
    config_path = sys.argv[1]
    config = ConfigParser()
    config.read(config_path)
    print("Configuration file read.")

    traffic_meta_path = config.get('Paths', 'meta_path')
    csv_files = config.get('Paths', 'data_path')
    print(csv_files)
    file_paths = utils.get_file_names(csv_files, extension=file_ext)




    print("Number of traffic files found: ", len(file_paths))

    if DEBUG:
        end = batch_limit
    else:
        end = len(file_paths)

    for i in range(0, end, batch_limit):
        files = file_paths[i:i + batch_limit]

        #transform_and_load(end, batch_limit, file_paths, traffic_meta_path)

        threads = []
        for j in range(thread_num):
            n = batch_limit / thread_num
            assert batch_limit % thread_num == 0
            chunked_files = list(chunks(files, int(n)))
            t = threading.Thread(target=transform_and_load, args=(config_path, chunked_files[j], traffic_meta_path,j,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

print("Exiting Main Thread")

print("total time: " , time.time()-t0)
