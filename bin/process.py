from pymapd import connect
from configparser import ConfigParser
import sys

from data_processing.process_utils import get_file_names, load_dfs


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
    limit = 20

    config = ConfigParser()
    config.read(config_path)

    csv_files = config.get('Paths', 'data_path')
    meta_path = config.get('Paths', 'meta_path')

    # get file paths:
    file_paths = get_file_names(csv_files)

    print("Number of files found: ", len(file_paths))

    # connect to mapd:
    con = connect(user="abraham",
                  password="abraham",
                  dbname='abraham',
                  host="13.90.129.165",
                  port=6273,
                  protocol='http')

    load_dfs(data_paths=file_paths,
             meta_path=meta_path,
             limit=limit,
             data_columns=data_columns,
             meta_columns=meta_columns,
             con=con,
             grouper=grouper,
             threshold=threshold,
             interest_col='speed',
             table_name='January_to_midFeb')



# read csv files
# data_df = pd.read_csv('/Users/abrahamduplaa/Desktop/OmniSci/Caltrans_Project/play_data/d04_text_station_5min_2019_01_03.txt',
#                      header=None, names=data_columns, usecols=no_data_cols)



# meta_df = pd.read_csv('/Users/abrahamduplaa/Desktop/OmniSci/Caltrans_Project/meta_files/d04_text_meta_2019_01_26.txt',
#                      sep='\t', usecols=meta_columns).set_index('ID')

# data_df = data_df.drop('district', axis=1)

# join data
# joined_df = data_df.join(meta_df, on='station')

# process data


# cleaned_df.to_csv(path_or_buf='./test_data')
# don't do fill_na, takes too long
# cleaned_df = grouped_fill_na(cleaned_df, grouper='station')

# con = connect(user="abraham", password="abraham",dbname='abraham', host="13.90.129.165", port=6273 , protocol='http' )

# con.load_table("pymapd_test", cleaned_df,method='infer',create='infer')