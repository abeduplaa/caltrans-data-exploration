import pandas as pd
import numpy as np
import os
from geopy.distance import vincenty
from ipywidgets import IntProgress
from functools import partial
import multiprocessing


def grouped_drop_na(df, threshold, grouper, col):

    g_count = df[col].groupby(df[grouper]).count()

    g_na = df[col].isna().groupby(df[grouper]).sum()

    g_pct_na = g_na / g_count

    in_tolerance = list(g_pct_na.loc[g_pct_na <= threshold].index)

    df = df.loc[df[grouper].isin(in_tolerance)]

    return df


def grouped_fill_na(df, grouper, method=None):
    if method is None:
        method = 'ffill'

    grouped = df.groupby(grouper)
    transformed = grouped.transform(lambda x: x.fillna(method=method))
    return transformed


def rounded_timestamp(df, name_current_ts='timestamp_', name_rounded_ts='timestamp_rounded', round_by='H'):
    df[name_rounded_ts] = df[name_current_ts].dt.round(round_by)
    return df


def grouped_resample(df, grouper, sample_size, time_col):

    grouped = df.groupby(grouper).resample(sample_size, on=time_col).mean()

    return grouped


def lower_col_names(df):
    df = df.rename(str.lower, axis='columns')

    return df


def state_pm_to_numeric(s):
    s = s.astype(str)
    s = [x.strip('R') for x in s]
    s = pd.to_numeric(s, errors='coerce')

    return s


def start_from_0(s):
    s = s - s.min()

    return s


def downcast_type(df):

    # pymapd does not allow int8 to be sent! had to add in line to upcast int8 to int16
    for col in df:

        if df[col].dtype == int:
            df[col] = pd.to_numeric(df[col], downcast='integer')
            df[col] = df[col].astype(np.int16) # upcast because of pymapd issue
        elif df[col].dtype == float:
            df[col] = pd.to_numeric(df[col], downcast='float')

    return df


def downcast_int(df, cols):
    for col in cols:
        df[col] = pd.to_numeric(df[col], downcast='integer')
        df[col] = df[col].astype(np.int16) # upcast because of pymapd issue
    return df


def get_na(df):
    print(df.isna().sum())


def add_day_of_week(df, timestamp_col):

    df['day_of_week'] = df[timestamp_col].dt.day_name()
    df['day_of_week_num'] = df[timestamp_col].dt.dayofweek
    return df


def apply_custom_transformations(df, interest_col, threshold, grouper):

    # drop nas
    df = grouped_drop_na(df, threshold, grouper=grouper, col=interest_col)

    # lower all column names

    df = lower_col_names(df)

    df['state_pm'] = state_pm_to_numeric(df['state_pm'])
    # drop all rows with na in absolute postmarker field
    df = df.dropna(subset=['abs_pm'])

    df['timestamp_'] = pd.to_datetime(df['timestamp_'], infer_datetime_format=True)

    # add a rounded timestamp for grouping later on: (e.g. 12:46 --> 13:00)
    df = rounded_timestamp(df=df, name_current_ts='timestamp_', name_rounded_ts='timestamp_rounded', round_by='H')

    df = add_day_of_week(df, 'timestamp_')

    # add column with hour of day for each data point
    df['hour_of_day'] = df['timestamp_'].dt.hour

    # add column with day of year for each data point
    df['day_of_year'] = df['timestamp_'].dt.dayofyear

    # downcast all ints to save memory
    df = downcast_int(df, ['station', 'freeway', 'total_flow', 'lanes', 'county'])

    # downcast all floats to save memory
    df = downcast_type(df)

    return df



def get_file_names(csv_path, extension):

    file_paths = [os.path.join(csv_path, f) for f in os.listdir(csv_path) if (os.path.isfile(os.path.join(csv_path, f)) & (os.path.splitext(f)[1] == extension))]
    return file_paths


def calculate_longlat_distance(df1, df2, key_col):
    """
    Calculates the distance between two longitude and latitude dataframe columns
    :param df1:
    :param df2:
    :param key_col: the column with the id
    :return: list containing the id which is closest for each row of df1
    """
    df1 = df1.rename(str.lower, axis='columns')
    df2 = df2.rename(str.lower, axis='columns')
    labels = []
    f = IntProgress(min=0, max=len(df1.index)) # instantiate the bar
    display(f) # display the bar
    for i in df1.index:
        f.value += 1 # signal to increment the progress bar
        for j in df2.index:
            temp_distance = vincenty((df1['latitude'].loc[i], df1['longitude'].loc[i]),
                                                    (df2['latitude'].loc[j], df2['longitude'].loc[j]))

            if j == df2.index[0]:
                closest = temp_distance
            elif temp_distance < closest:
                closest = temp_distance
                idx = j

        labels.append(df2[key_col].loc[idx])

    return labels


def calc_distances(df1, df2, zipped):
    i = zipped[0]
    results = zipped[1]
    
    df2 = df2.loc[ df2['direction'] == df1['direction'][i] ]
    
    for j in df2.index:
        temp_distance = vincenty((df1['latitude'].loc[i], df1['longitude'].loc[i]),
                                                (df2['latitude'].loc[j], df2['longitude'].loc[j]))

        if j == df2.index[0]:
            closest = temp_distance
            idx = j
        elif temp_distance < closest:
            closest = temp_distance
            idx = j

    results[df1['location'].loc[i]] = df2['id'].loc[idx]



def longlat_distance_parallel(df1, df2, key_col, num_processes=8):
    """
    Calculates the distance between two longitude and latitude dataframe columns
    :param df1:
    :param df2:
    :param key_col: the column with the id
    :return: list containing the id which is closest for each row of df1
    """
    df1 = df1.rename(str.lower, axis='columns')
    df2 = df2.rename(str.lower, axis='columns')
    
    df1_index = list(df1.index)
    labels = []
       
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    import itertools
    with multiprocessing.Pool(processes=num_processes) as pool:
        func = partial(calc_distances, df1, df2)
        pool.map(func, zip(df1_index, itertools.repeat(return_dict) ) )
    
    return return_dict
                 