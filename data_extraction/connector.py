#TODO: add error function and logging and output
from requests import session, ConnectionError


class Connector:
    def __init__(self, login, url_base=None):
        if url_base is None:
            url_base = 'http://pems.dot.ca.gov/'

        self.url_base = url_base
        self.connect_config = login
        self._test_connection()

    def _test_connection(self):
        # add tester to make sure connection worked.
        pass

    def start_connection(self):

        self.connect_config['action'] = 'login'
        conn = session()

        conn.post(self.url_base, data=self.connect_config)

        return conn
        # returns handle to download things

    def _connection_error(self):
        pass
