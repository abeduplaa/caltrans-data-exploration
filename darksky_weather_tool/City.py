from geopy import geocoders


class City:
    def __init__(self, city_name, state):
        self.name = city_name
        self.state = state
        self.longitude = None
        self.latitude = None

        self.get_lat_long()

    def get_lat_long(self):
        geolocator = geocoders.Nominatim(user_agent='city_finder')
        city_state = self.name + ', ' + self.state
        print(city_state)
        location = geolocator.geocode(city_state)

        try:
            self.latitude = location.latitude
            self.longitude = location.longitude
        except Exception as ex:
            template = "An exception of type {0} occurred. Did you make sure to enter city and state correctly? " \
                       "Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            raise ex

