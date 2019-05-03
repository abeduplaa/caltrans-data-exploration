from darksky_weather_tool.Weather import Weather
from darksky_weather_tool.City import City
from configparser import ConfigParser
from omnisci_connector.omni_connect import OmnisciConnect

import datetime
import sys


#INPUTS:
city_name = 'San Francisco'
state_name = 'California'

start_date = datetime.datetime(2019, 1, 1)
end_date = datetime.datetime(2019, 2, 18)

to_omnisci = False

if to_omnisci:
    table_name = "SanFrancisco_Weather_JanFeb"


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(len(sys.argv))
        raise TypeError("ERROR: need to provide path to config file.")

    config_path = sys.argv[1]

    config = ConfigParser()
    config.read(config_path)

    api_key = config.get('Darksky', 'key')

    city = City(city_name, state_name)

    forecast_handle = Weather(api_key)

    weather_forecast = forecast_handle.get_hourly_weather(city, start_date, end_date, True)

    if to_omnisci:
        connection = OmnisciConnect(config_path)
        connection.start_connection()
        connection.load_data(table_name=table_name,
                             df=weather_forecast,
                             method='infer',
                             create='infer')
        connection.close_connection()
