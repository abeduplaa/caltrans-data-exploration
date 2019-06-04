OmniSci Data exploration of Caltrans Data
==============================================

Note: Use python 3.6 and make sure to install the requirements. (`pip install -r requirements.txt`)



# 1. Downloading Caltrans Data

At the moment, the following pre-steps are needed to download the data:
1. copy the links from the caltrans website html for the year wanted and place the file in the html directory
2. make sure the directories and login info in the config file is correct

Once all the links have been placed in the proper place and config file is properly set, run the following command:

```
python extract.py PATH/TO/CONFIG.INI
```

This will save the caltrans text files to the directory specified in config.ini

# 2. Processing and Loading Caltrans Data in to OmiSci

(Make sure file directory is correct in config.ini file)

To process and transform the downloaded text files, run process_traffic_data.py to transform and then load the data in to your OmniSci instance.

```
python process_traffic_data.py PATH/TO/CONFIG.INI
```

# 3. Jupyter notebooks

With the data either provided from the S3 bucket or loaded by the user to OmniSci, you can now use the notebooks to try to predict traffic flow. Play around and try different architectures and models with the notebooks.


Final note: The weather part will also be described in this README later on.

## Pre-cleaned data on S3

If you just want to analyze the data yourself without downloading and processing it, the dataset is located at:
s3://mapd-cloud/DataSets/caltrans_2015_2019
