from weather_tool.Weather import Weather
from weather_tool.City import City
from configparser import ConfigParser
from omnisci_connector.omni_connect import OmnisciConnect

import datetime
import sys


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(len(sys.argv))
        raise TypeError("ERROR: need to provide path to config file.")

    config_path = sys.argv[1]

    city_name = 'San Francisco'
    state_name = 'California'

    start_date = datetime.datetime(2019, 1, 1)
    end_date = datetime.datetime(2019, 1, 10)

    config = ConfigParser()
    config.read(config_path)

    api_key = config.get('Darksky', 'key')

    sanfran = City('San Francisco', 'California')

    forecast_handle = Weather(api_key)

    weather_forecast = forecast_handle.get_hourly_weather(sanfran, start_date, end_date, True)

    # Define info for omnisci
    table_name = "weather_test"

    # todo: this column renaming is temporary. remove this once check function is in place
    weather_forecast = weather_forecast.rename(index=str, columns={'time': 'time_'})

    connection = OmnisciConnect(config_path)
    connection.connect()
    connection.load_data(table_name=table_name, df=weather_forecast,method='infer', create='infer')

