from configparser import ConfigParser
import sys
import pandas as pd
import threading
import time

import data_processing.process_utils as utils
from omnisci_connector.omni_connect import OmnisciConnect
from data_processing.MetaData import MetaData
from data_processing.TrafficData import TrafficData

table_name = "test_2"

# initial parameters for reading in traffic data
threshold = 0.01
interest_col = 'speed'
grouper = 'station'
batch_limit = 1
file_ext = '.txt'
thread_num = 1
DEBUG = False


def transform_and_load(config_path, paths, meta_path, thread):

    # Send data to omnisci:
    print("connect to omnisci with thread: ", thread)
    connection = OmnisciConnect(config_path)
    connection.start_connection()

    df_batch = []

    try:
        traffic_data = TrafficData(paths)

        meta_data = MetaData(meta_path)

        data = traffic_data.join_meta(meta_df=meta_data.df)

        df_transformed = utils.apply_custom_transformations(df=data,
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
