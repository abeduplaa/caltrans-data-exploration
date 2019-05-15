from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error
import pandas as pd
import numpy as np

def data_index(df, n_lag, n_steps):
    
    steps = n_lag + n_steps - 1
    
    return df.iloc[steps:].index

def format_model_data(df, n_lag, n_steps):
    df_out = []
    for station, new_df in df.groupby(level=0):
        key, scaled, scaler1 = scale_data(new_df)
        reframed_ = prepare_data_for_network(scaled,n_lag,n_steps)
        df_out.append(reframed_)
        
    return pd.concat(df_out, ignore_index=True), key, scaled, scaler1


def scale_data(df):
    # process data
    values = df.values
    key = data_key(df)

    # normalize features
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaler1 = MinMaxScaler(feature_range=(0, 1))
    scaled = scaler.fit_transform(values)

    scaled1 = scaler1.fit_transform(values[:,1].reshape(-1, 1))
    # print(key)
    
    return key, scaled, scaler1


def data_key(df):
    key = dict()
    for i,col in enumerate(list(df.columns)):
        var_ = 'var'+str(i+1)
        key[col] = var_
    return key

def prepare_data_for_network(data, n_in=1, n_out=1, dropnan=True):
    """
    Frame a time series as a supervised learning dataset.
    Arguments:
        data: Sequence of observations as a list or NumPy array.
        n_in: Number of lag observations as input (X).
        n_out: Number of observations as output (y).
        dropnan: Boolean whether or not to drop rows with NaN values.
    Returns:
        Pandas DataFrame of series framed for supervised learning.
    """
    n_vars = 1 if type(data) is list else data.shape[1]
    df = pd.DataFrame(data)
    cols, names = list(), list()
    # input sequence (t-n, ... t-1)
    for i in range(n_in, 0, -1):
        cols.append(df.shift(i))
        names += [('var%d(t-%d)' % (j+1, i)) for j in range(n_vars)]
    # forecast sequence (t, t+1, ... t+n)
    for i in range(0, n_out):
        cols.append(df.shift(-i))
        if i == 0:
            names += [('var%d(t)' % (j+1)) for j in range(n_vars)]
        else:
            names += [('var%d(t+%d)' % (j+1, i)) for j in range(n_vars)]
    # put it all together
    agg = pd.concat(cols, axis=1)
    agg.columns = names
    # drop rows with NaN values
    if dropnan:
        agg.dropna(inplace=True)
    return agg


