import pandas as pd


class TrafficData:
    data_columns = ['timestamp_',
                    'station',
                    'district',
                    'freeway',
                    'direction',
                    'lane_type',
                    'station_length',
                    'samples',
                    'pct_observed',
                    'total_flow',
                    'occupancy',
                    'speed'
                    ]

    def __init__(self, files):
        self.files = files
        self.df = None

        # Read in traffic data
        self.read_data()

    def read_data(self):
        no_data_cols = list(range(len(self.data_columns)))
        l_df = []
        for f in self.files:
            print("Processing file: ", f)
            temp = pd.read_csv(f, header=None, names=self.data_columns, usecols=no_data_cols)
            l_df.append(temp)

        self.df = pd.concat(l_df, ignore_index=True)

    def join_meta(self, meta_df):
        joined = self.df.join(meta_df, on='station')
        return joined
