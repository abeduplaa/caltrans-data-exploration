import pandas as pd

#TODO: add in more functions, complete the processing framework

data_columns = ['timestamp',
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

no_cols = list(range(len(data_columns)))

meta_columns = ['ID',
                'County',
                'State_PM',
                'Abs_PM',
                'Latitude',
                'Longitude',
                'Lanes',
                'Name']


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


threshold = 0.1
interest_col = 'speed'
grouper = 'station'

data_df = pd.read_csv('/Users/abrahamduplaa/Desktop/OmniSci/Caltrans_Project/play_data/d04_text_station_5min_2019_01_03.txt',
                      header=None, names=data_columns, usecols=no_cols)

meta_df = pd.read_csv('/Users/abrahamduplaa/Desktop/OmniSci/Caltrans_Project/meta_files/d04_text_meta_2019_01_26.txt',
                      sep='\t', usecols=meta_columns).set_index('ID')

joined_df = data_df.join(meta_df, on='station')


cleaned_df = grouped_drop_na(joined_df, threshold, grouper='station', col=interest_col)

# cleaned_df.to_csv(path_or_buf='./test_data')
# don't do fill_na, takes too long
# cleaned_df = grouped_fill_na(cleaned_df, grouper='station')
