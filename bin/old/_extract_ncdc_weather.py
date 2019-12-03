# Do not use
### TODO:
# read in token from file
# auto load data from NCDC



import sys
sys.path.append('./src')

import pandas as pd
from noaa_weather_tool.noaa_api_v2 import NOAAData


def using_grouper(df):
    level_values = df.index.get_level_values
    return df.groupby(level_values(0) + pd.Grouper(freq='60min', level=-1)).max()


def using_reset_index(df):
    df = df.reset_index(level=0)
    return df.groupby('STATION').resample('60min').max()


ncdc_data_path = '/Users/abrahamduplaa/Desktop/OmniSci/ncdc_data.csv'


counties = ['Alameda County',
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

state = 'CA'


startdate = '2019-01-01'
enddate = '2019-02-20'
dataset = 'LCD'

n = NOAAData(token)

locations = n.locations(datasetid='LCD',
                        locationcategoryid='CNTY',
                        startdate=startdate,
                        enddate=enddate,
                        limit=1000
                        )

df = pd.DataFrame(locations['results'])

df2 =pd.DataFrame(df['name'].str.split(', ',).tolist(),columns = ['county','state'])

df = df.join(df2)

location_ids = df.loc[(df.county.isin(counties)) & (df.state == 'CA')]

ids = location_ids['id']

stations = []

for id in ids:
    fetch_stations = n.stations(datasetid='LCD',
                                limit=1000,
                                locationid=id,
                                startdate=startdate,
                                enddate=enddate,
                                )
    stations.append(fetch_stations['results'])

stations = [item for sublist in stations for item in sublist]

# data = n.fetch_data(datasetid='lcd',
#                      startdate='2019-01-01',
#                      enddate='2019-02-01',
#                     #locationid='FIPS:06097',
#                      stationid='WBAN:23230',
#                      limit=1000)

#datasetid=GSOM&stationid=GHCND:USC00010008&units=standard&startdate=2010-05-01&enddate=2010-05-31

metadata = pd.DataFrame(stations)


weather_data = pd.read_csv(ncdc_data_path, low_memory=False)

filter_cols =  [col for col in weather_data if col.startswith('Hourly')]

filter_cols.append('STATION')
filter_cols.append('DATE')

weather_data = weather_data[filter_cols]

weather_data['timestamp'] = pd.to_datetime(weather_data['DATE'])


weather_data = weather_data.set_index(['STATION', 'timestamp'])

weather_data = weather_data.drop(columns=['DATE', 'HourlyPresentWeatherType', 'HourlySkyConditions'])

for col in weather_data:
    weather_data[col] = pd.to_numeric(weather_data[col], errors='coerce')

weather_data = weather_data.fillna(0)


weather_data = using_reset_index(weather_data)

weather_data = weather_data.drop(columns='STATION')
weather_data = weather_data.reset_index(level=0)
weather_data = weather_data.loc[startdate:enddate]

weather_data = weather_data.fillna(0)

weather_data['id'] = weather_data['STATION'].str[-5:]

metadata['id'] = metadata['id'].str[5:]

weather_data = weather_data.reset_index()

weather_data = weather_data.rename(index=str, columns={'timestamp':'timestamp_'})

final_weather_data = pd.merge(weather_data, metadata, on='id')

table_name = 'ncdc_weather_test'

#connection = OmnisciConnect(config_path)
#connection.start_connection()
#connection.load_data(table_name=table_name, df=final, method='infer', create='infer')
#connection.close_connection()
