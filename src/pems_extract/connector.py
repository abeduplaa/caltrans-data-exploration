#TODO: add error function and logging and output
import requests 


class Connector:
    def __init__(self, login, url_base=None):
        if url_base is None:
            url_base = 'http://pems.dot.ca.gov/'

        self.url_base = url_base
        self.connect_config = login
        self._test_login_auth()

    def _test_login_auth(self):
        response = requests.post(self.url_base, data=self.connect_config)
        if ("Incorrect username or password" in response.content.decode("utf-8")):
            raise requests.ConnectionError("Incorrect username and password for caltrans PeMS")

    def start_connection(self):

        self.connect_config['action'] = 'login'
        conn = requests.session()
        conn.post(self.url_base, data=self.connect_config)
        return conn
        # returns handle to download files from caltrans
