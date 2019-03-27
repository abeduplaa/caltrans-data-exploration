from configparser import ConfigParser
import pymapd
import os

class ReservedWordsException(Exception):
    """
    Class for same column exception
    """
    pass


def read_reserved_words(file_path=None):
    if file_path is None:
        file_path = os.path.dirname(os.path.realpath(__file__)) + '/RESERVED_WORDS'
    # check if file exists
    try:
        with open(file_path, 'r') as f:
            content = f.readlines()
            content = [x.strip() for x in content]
    except FileNotFoundError as ex:
        print(ex)
        raise Exception('RESERVED_WORDS file not found. is RESERVED_WORDS located in omnisci_connector folder?')

    return content


class OmnisciConnect:

    _RESERVED_WORDS = read_reserved_words()

    def __init__(self, config_path):
        self.config_path = config_path
        self._parse_config()
        self.con = None

    def _check_col_names(self, df):

        cols = list(df.columns)

        cols = [col.upper() for col in cols]

        if any(elem in cols for elem in self._RESERVED_WORDS):
            new_cols = {}
            bools = [elem in self._RESERVED_WORDS for elem in cols]
            bools = [i for i, x in enumerate(bools) if x]

            for b in bools:
                new_cols[cols[b]] = cols[b]+'_'

            df = df.rename(new_cols,axis='columns')
            print(df.dtypes)
            #raise ReservedWordsException("name of column in dataframe is same as omnisci reserved words")

        return df

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

    def start_connection(self):
        self.con = pymapd.connect(user=self.user,
                                  password=self.password,
                                  dbname=self.dbname,
                                  host=self.host,
                                  port=self.port,
                                  protocol=self.protocol)

    def load_data(self, table_name, df, method='infer', create='infer'):

        df = self._check_col_names(df)

        print("loading data to omnisci.")
        self.con.load_table(table_name, df, method=method, create=create)
        print("successfully loaded data to omnisci.")
    def close_connection(self):
        self.con.close()
        print("connection closed.")




