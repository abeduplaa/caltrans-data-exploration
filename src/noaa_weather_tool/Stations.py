from noaa_weather_tool.noaa_api_v2 import NOAAData


class Stations:
    def __init__(self, counties):
        self.counties = counties

    def _get_locationlist(self):
        NOAAData.locations
