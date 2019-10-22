from configparser import ConfigParser
import sys
import pandas as pd

import data_processing.process_utils as utils
from omnisci_connector.omni_connect import OmnisciConnect
from noaa_weather_tool.noaa_api_v2 import NOAAData

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
    df['day_of_year'] = df['timestamp w_'].dt.dayofyear

    # downcast all ints to save memory
    df = utils.downcast_int(df, ['station', 'freeway', 'total_flow', 'lanes', 'county'])

    # downcast all floats to save memory
    df = utils.downcast_type(df)

    return df


def using_reset_index(df):
    df = df.reset_index(level=0)
    return df.groupby('STATION').resample('60min').max()


def add_rounded_hour_column(df, name_current_ts='timestamp_', name_rounded_ts='timestamp_rounded', round_by='H'):
    df[name_rounded_ts] = df[name_current_ts].dt.round(round_by)
    return df


if __name__ == "__main__":

    send_weather = False
    send_traffic = True
    send_joined = True
    send_meta_traffic = False
    send_meta_weather = False

    traffic_table_name = "joined_traffic_weather_janfeb_correcttypes"
    weather_table_name = "ncdc_weather_janfeb_dictstrkey_test"

    traffic_meta_table_name = "caltrans_traffic_janfeb_d07_metatable"
    weather_meta_table_name = "ncdc_weather_janfeb_sanfrancisco_metatable"

    if len(sys.argv) != 2:
        print(len(sys.argv))
        raise TypeError("ERROR: need to provide path to config file.")

    # Configuration file reader
    config_path = sys.argv[1]
    config = ConfigParser()
    config.read(config_path)
    print("Configuration file read.")

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

    # weather information for loading in data
    weather_counties = ['Alameda County',
                        'Contra Costa County',
                        'Marin County',
                        'Napa County',
                        'San Benito County',
                        'San Francisco County',
                        'San Mateo County',
                        'Santa Clara County',
                        'Santa Cruz County',
                        'Solano County',
                        'Sonoma County'
                        ]

    weather_state = 'CA'

    weather_startdate = '2019-01-01'
    weather_enddate = '2019-02-20'
    weather_dataset = 'LCD'

    # initial parameters for reading in traffic data
    threshold = 0.1
    interest_col = 'speed'
    grouper = 'station'
    batch_limit = 1
    file_ext = '.txt'

    ### metadata section ###
    traffic_meta_path = config.get('Paths', 'meta_path')

    # read in the traffic metadata to pandas:
    df_traffic_metadata = pd.read_csv(traffic_meta_path, sep='\t', usecols=traffic_meta_columns).set_index('ID')
    df_traffic_metadata = df_traffic_metadata.rename(str.lower, axis='columns')
    print("traffic metadata file read.")

    # Weather metadata section

    token = config.get('NCDC', 'key')

    ncdc = NOAAData(token)

    weather_locations = ncdc.locations(datasetid='LCD',
                                       locationcategoryid='CNTY',
                                       startdate=weather_startdate,
                                       enddate=weather_enddate,
                                       limit=1000
                                       )

    df_ncdc_locations = pd.DataFrame(weather_locations['results'])

    df_ncdc_locations = df_ncdc_locations.join(pd.DataFrame(df_ncdc_locations['name'].str.split(', ', ).tolist(),
                                                            columns=['county', 'state']))

    location_ids = df_ncdc_locations.loc[(df_ncdc_locations['county'].isin(weather_counties)) &
                                         (df_ncdc_locations['state'] == 'CA')]['id']

    stations = []
    for w_id in location_ids:
        fetch_stations = ncdc.stations(datasetid='LCD',
                                    limit=1000,
                                    locationid=w_id,
                                    startdate=weather_startdate,
                                    enddate=weather_enddate,
                                    )
        stations.append(fetch_stations['results'])
    stations = [item for sublist in stations for item in sublist]

    df_weather_metadata = pd.DataFrame(stations)
    df_weather_metadata = df_weather_metadata.rename(str.lower, axis='columns')
    df_weather_metadata['weather_id'] = df_weather_metadata['id'].str.split(':').str[1]

    # calculate distance between weather stations and traffic stations.
    # then add column to traffic metadata to know which weather station corresponds to traffic station
    weather_station_id = 'weather_station_id'
    df_traffic_metadata[weather_station_id] = utils.calculate_longlat_distance(df_traffic_metadata,
                                                                                 df_weather_metadata,
                                                                                 'weather_id')

    df_traffic_metadata[weather_station_id] = pd.to_numeric(df_traffic_metadata[weather_station_id])
    print(df_traffic_metadata.dtypes)
    print(df_traffic_metadata.isna().sum())
    print("weather metadata file read.")

    # Extract weather data
    ncdc_data_path = config.get('Paths', 'ncdc_data_path')
    raw_weather_data = pd.read_csv(ncdc_data_path, low_memory=False)

    weather_hourly_columns = [col for col in raw_weather_data if col.startswith('Hourly')]
    weather_hourly_columns.append('STATION')
    weather_hourly_columns.append('DATE')

    raw_weather_data['timestamp_'] = pd.to_datetime(raw_weather_data['DATE'])

    raw_weather_data = raw_weather_data.set_index(['STATION', 'timestamp_'])

    weather_hourly_columns.remove('DATE')
    weather_hourly_columns.remove('STATION')
    weather_hourly_columns.remove('HourlyPresentWeatherType')
    weather_hourly_columns.remove('HourlySkyConditions')

    raw_weather_data = raw_weather_data[weather_hourly_columns]

    for col in weather_hourly_columns:
        raw_weather_data[col] = pd.to_numeric(raw_weather_data[col], errors='coerce')

    weather_data = raw_weather_data.fillna(0)

    weather_data = using_reset_index(weather_data)

    weather_data[weather_station_id] = weather_data['STATION'].str[-5:]
    weather_data = weather_data.drop(columns='STATION')
    weather_data = weather_data.reset_index(level=0)
    weather_data = weather_data.loc[weather_startdate:weather_enddate]
    weather_data = raw_weather_data.fillna(0)
    weather_data = weather_data.reset_index()
    weather_data = weather_data.rename(str.lower, axis='columns')
    print("weather data extracted and transformed.")

    ### Traffic Section ###

    # get the paths of relevant files for the data
    csv_files = config.get('Paths', 'data_path')

    # Send data to omnisci:
    print("connect to omnisci")
    connection = OmnisciConnect(config_path)
    connection.start_connection()

    # send traffic metadata:
    if send_meta_traffic:
        print("sending %s to omnisci" % traffic_meta_table_name)
        connection.load_data(table_name=traffic_meta_table_name, df=df_traffic_metadata, method='infer', create='infer')

    # send weather metadata:
    if send_meta_weather:
        print("sending %s to omnisci" % weather_meta_table_name)
        connection.load_data(table_name=weather_meta_table_name, df=df_weather_metadata, method='infer', create='infer')

    # send weather data
    if send_weather:
        print("sending weather data to omnisci")
        #weather_data[weather_station_id] = weather_data['station'].str[-5:]
        #weather_data[weather_station_id] = pd.to_numeric(weather_data[weather_station_id])

        weather_data = utils.downcast_type(weather_data)
        print(weather_data.dtypes)
        connection.load_data(table_name=weather_table_name, df=weather_data, method='infer', create='infer')

    ts_join = 'timestamp_rounded'
    if send_joined:
        weather_data[weather_station_id] = weather_data['station'].str[-5:]
        # weather_data = weather_data.rename(index=str, columns={"station": weather_station_id})
        # weather_data = utils.downcast_type(weather_data)

        join_key = [weather_station_id, ts_join]
        weather_data = utils.downcast_type(weather_data)
        weather_data = add_rounded_hour_column(weather_data, name_rounded_ts=ts_join)
        grouped_weather_data = weather_data.groupby(join_key).mean()
        grouped_weather_data = grouped_weather_data.reset_index()

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

            print("applying transformations to traffic data")
            df_transformed_traffic = apply_custom_transformations(df=df_extracted_traffic,
                                                                  interest_col=interest_col,
                                                                  threshold=threshold,
                                                                  grouper=grouper)

            df_transformed_traffic[weather_station_id] = df_transformed_traffic[weather_station_id].astype(int)
            df_transformed_traffic[weather_station_id] = df_transformed_traffic[weather_station_id].astype(str)

            if send_joined:
                # df_transformed_traffic = add_rounded_hour_column(df_transformed_traffic, name_rounded_ts=ts_join)
                df_transformed_traffic[ts_join] = df_transformed_traffic['timestamp_']
                df_transformed_traffic[weather_station_id] = df_transformed_traffic[weather_station_id].astype(str)
                df_transformed_traffic = pd.merge(left=df_transformed_traffic,
                                                  right=grouped_weather_data,
                                                  how='inner',
                                                  on=join_key)

            connection.load_data(table_name=traffic_table_name,
                                 df=df_transformed_traffic,
                                 method='infer',
                                 create='infer')

    connection.close_connection()

