import pandas as pd
import numpy as np
import os


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


def lower_col_names(df):
    df = df.rename(str.lower, axis='columns')

    return df


def convert_state_pm(s):
    s = s.astype(str)
    s = [x.strip('R') for x in s]
    s = pd.to_numeric(s, errors='coerce')

    return s


def start_from_0(s):
    s = s - s.min()

    return s


def downcast_type(df):

    # TODO: find out why mapd won't accept int8!
    for col in df:

        if df[col].dtype == int:
            df[col] = pd.to_numeric(df[col], downcast='integer')
            df[col] = df[col].astype(np.int16)
        elif df[col].dtype == float:
            df[col] = pd.to_numeric(df[col], downcast='float')

    return df


def downcast_int(df, cols):
    for col in cols:
        df[col] = pd.to_numeric(df[col], downcast='integer')
        df[col] = df[col].astype(np.int16)
    return df


def get_na(df):
    print(df.isna().sum())


def add_day_of_week(df, timestamp_col):

    df['day_of_week'] = df[timestamp_col].dt.day_name()
    return df


def apply_transformations(df, interest_col, threshold, grouper):

    df = grouped_drop_na(df, threshold, grouper=grouper, col=interest_col)

    df = lower_col_names(df)

    df['state_pm'] = convert_state_pm(df['state_pm'])

    df['station'] = start_from_0(df['station'])

    df = df.dropna(subset=['abs_pm'])

    df = downcast_type(df)

    df = downcast_int(df, ['lanes','county'])

    df['timestamp_'] = pd.to_datetime(df['timestamp_'], infer_datetime_format=True)

    df = add_day_of_week(df, 'timestamp_')

    return df


def get_file_names(csv_path):

    file_paths = [os.path.join(csv_path, f) for f in os.listdir(csv_path) if os.path.isfile(os.path.join(csv_path, f))]

    return file_paths


def load_dfs(data_paths, meta_path, limit, data_columns, meta_columns, con, table_name, grouper='station',threshold=0.1, interest_col='speed'):

    no_data_cols = list(range(len(data_columns)))

    meta_df = pd.read_csv(meta_path, sep='\t', usecols=meta_columns).set_index('ID')

    for i in range(0, len(data_paths), limit):
        df_0 =[]
        for f in data_paths[i:i + limit]:
            print(f)
            df_temp = pd.read_csv(f, header=None, names=data_columns, usecols=no_data_cols)
            df_0.append(df_temp)

        data_df = pd.concat(df_0, ignore_index=True)

        # data_df = pd.concat(
        #    [pd.read_csv(f, header=None, names=data_columns, usecols=no_data_cols) for f in data_paths[i:i+limit]]
        #    , ignore_index=True)

        data_df = data_df.drop('district', axis=1)

        joined_df = data_df.join(meta_df, on='station')

        ready_df = apply_transformations(df=joined_df, interest_col=interest_col, threshold=threshold, grouper=grouper)

        if i == 0:
            create = True
        else:
            create = False

        print("loading data to omnisci")
        con.load_table(table_name, ready_df, method='infer', create=create)