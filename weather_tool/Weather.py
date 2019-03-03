import datetime

import darksky
import pandas as pd



class Weather:
    def __init__(self, key):
        self.key = key

    def get_hourly_weather_1day(self, city, time):
        city_forecast = darksky.forecast(key=self.key, latitude=city.latitude, longitude=city.longitude, time=time)

        return city_forecast['hourly']['data']

    def get_hourly_weather(self, city, start, end, to_pandas=True):

        delta = end - start
        no_days = delta.days
        date = start

        weather_data = []

        for i in range(no_days):
            unix_date = int(date.timestamp())

            _data = self.get_hourly_weather_1day(city, unix_date)
            weather_data.append(_data)

            date += datetime.timedelta(days=1)

        weather_data = [item for sublist in weather_data for item in sublist]

        if to_pandas is True:
            weather_data = pd.DataFrame(weather_data)
            weather_data['time'] = pd.to_datetime(weather_data['time'], unit='s')

        return weather_data





