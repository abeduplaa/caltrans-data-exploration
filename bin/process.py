import pymapd
from configparser import ConfigParser
import sys

from data_processing.process_utils import get_file_names, transform_and_load


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(len(sys.argv))
        raise TypeError("ERROR: need to provide path to config file.")

    config_path = sys.argv[1]

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

    meta_columns = ['ID',
                    'County',
                    'State_PM',
                    'Abs_PM',
                    'Latitude',
                    'Longitude',
                    'Lanes',
                    'Name']

    threshold = 0.1
    interest_col = 'speed'
    grouper = 'station'
    limit = 1
    file_ext = '.txt'

    config = ConfigParser()
    config.read(config_path)

    # get the paths of relevant files for the data
    csv_files = config.get('Paths', 'data_path')
    meta_path = config.get('Paths', 'meta_path')

    # get OmniSci login information from config file
    user = config.get('OmniSci-Connection', 'user')
    password = config.get('OmniSci-Connection', 'password')
    dbname = config.get('OmniSci-Connection', 'dbname')
    host = config.get('OmniSci-Connection', 'host')
    port = config.get('OmniSci-Connection', 'port')
    protocol = config.get('OmniSci-Connection', 'protocol')

    # get file paths:
    file_paths = get_file_names(csv_files, extension=file_ext)

    print("Number of files found: ", len(file_paths))

    # connect to OmniSci:
    con = pymapd.connect(user=user,
                         password=password,
                         dbname=dbname,
                         host=host,
                         port=port,
                         protocol=protocol)

    transform_and_load(data_paths=file_paths,
                       meta_path=meta_path,
                       limit=limit,
                       data_columns=data_columns,
                       meta_columns=meta_columns,
                       con=con,
                       grouper=grouper,
                       threshold=threshold,
                       interest_col='speed',
                       table_name='text_enc_test')

    con.close()

