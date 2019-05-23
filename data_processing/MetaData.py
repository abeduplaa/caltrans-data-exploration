import pandas as pd


class MetaData:
    meta_columns = ['id',
                    'county',
                    'state_pm',
                    'abs_pm',
                    'latitude',
                    'longitude',
                    'lanes']

    def __init__(self, meta):
        self.meta_file = meta
        self.df = None
        # Read in metatable
        self.read_meta()

    def read_meta(self):
        self.df = (pd.read_csv(self.meta_file, sep=',', usecols=self.meta_columns)
                   .rename(str.lower, axis='columns')
                   .set_index('id'))

