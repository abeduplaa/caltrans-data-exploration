from configparser import ConfigParser
import pymapd


class OmnisciConnect:
    def __init__(self, config_path):
        self.config_path = config_path
        self._parse_config()
        self.con = None

    def _parse_config(self):
        config = ConfigParser()
        config.read(self.config_path)

        # get OmniSci login information from config file
        self.user = config.get('OmniSci-Connection', 'user')
        self.password = config.get('OmniSci-Connection', 'password')
        self.dbname = config.get('OmniSci-Connection', 'dbname')
        self.host = config.get('OmniSci-Connection', 'host')
        self.port = config.get('OmniSci-Connection', 'port')
        self.protocol = config.get('OmniSci-Connection', 'protocol')

    def connect(self):
        self.con = pymapd.connect(user=self.user,
                                  password=self.password,
                                  dbname=self.dbname,
                                  host=self.host,
                                  port=self.port,
                                  protocol=self.protocol)

    def load_data(self, table_name, df, method='infer', create='infer'):
        print("loading data to omnisci")

        # TODO: add function that checks all the columns and makes sure that col names are not reserved by omnisci.
        # if so then function should add an underscore or something.
        # self._check_columns(df)

        self.con.load_table(table_name, df, method=method, create=create)

    def close_connect(self):
        self.con.close()

