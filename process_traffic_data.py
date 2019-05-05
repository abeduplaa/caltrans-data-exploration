from configparser import ConfigParser
import sys
import pandas as pd

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
    df = utils.downcast_int(df, ['station', 'freeway', 'samples', 'total_flow', 'lanes', 'county'])

    # downcast all floats to save memory
    df = utils.downcast_type(df)

    return df


send_traffic = True
table_name = "historic_test"

# initial parameters for reading in traffic data
threshold = 0.1
interest_col = 'speed'
grouper = 'station'
batch_limit = 1
file_ext = '.txt'


# traffic information for loading in data
traffic_data_columns = ['timestamp_',
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

traffic_meta_columns = ['ID',
                        'County',
                        'State_PM',
                        'Abs_PM',
                        'Latitude',
                        'Longitude',
                        'Lanes',
                        'Name']

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(len(sys.argv))
        raise TypeError("ERROR: need to provide path to config file.")

    # Configuration file reader
    config_path = sys.argv[1]
    config = ConfigParser()
    config.read(config_path)
    print("Configuration file read.")

    ### metadata section ###
    traffic_meta_path = config.get('Paths', 'meta_path')

    # read in the traffic metadata to pandas:
    df_traffic_metadata = pd.read_csv(traffic_meta_path, sep='\t', usecols=traffic_meta_columns).set_index('ID')
    df_traffic_metadata = df_traffic_metadata.rename(str.lower, axis='columns')
    print("traffic metadata file read.")

    ### Traffic Section ###

    # get the paths of relevant files for the data
    csv_files = config.get('Paths', 'data_path')

    # Send data to omnisci:
    print("connect to omnisci")
    connection = OmnisciConnect(config_path)
    connection.start_connection()


    # send traffic metadata:
    if send_traffic:
        # get file paths:
        file_paths = utils.get_file_names(csv_files, extension=file_ext)
        print("Number of traffic files found: ", len(file_paths))

        # extract and traffic data in batches:
        no_data_cols = list(range(len(traffic_data_columns)))
        for i in range(0, len(file_paths), batch_limit):
        #for i in range(0, batch_limit, batch_limit):
            df_batch = []
            for f in file_paths[i:i + batch_limit]:
                print("Processing file: ", f)
                temp = pd.read_csv(f, header=None, names=traffic_data_columns, usecols=no_data_cols)
                df_batch.append(temp)

            df_extracted_traffic = pd.concat(df_batch, ignore_index=True)

            df_extracted_traffic = df_extracted_traffic.drop('district', axis=1)

            df_extracted_traffic = df_extracted_traffic.join(df_traffic_metadata, on='station')

            df_transformed_traffic = apply_custom_transformations(df=df_extracted_traffic,
                                                                  interest_col=interest_col,
                                                                  threshold=threshold,
                                                                  grouper=grouper)

            connection.load_data(table_name=table_name,
                                 df=df_transformed_traffic,
                                 method='infer',
                                 create='infer')

    connection.close_connection()
