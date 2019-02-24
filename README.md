OmniSciData exploration with Caltrans Data
==============================================

**under progress**

Use python 3+

At the moment, the following steps are needed to download the data:
1. copy the html from the caltrans website for the year wanted and place in the html directory
2. make sure the directories and login info in the config file is correct

## to extract data from caltrans
you'll need to first make sure everything is correct in the config.ini file.

Then run the command:
```
python /bin/extract.py /path/to/your/config.ini/file
```

## after extraction process the data by running
```
python /bin/process.py /path/to/your/config.ini/file
```
In process.py, there are some parameters that can be adjusted

___

Columns of importance from Caltrans:

| Columns |
| ------------- |
 Timestamp |
Station |
District |
Freeway # |
Direction of Travel | 
Lane Type |
Station Length |
Samples |
% Observed |
Total Flow |
Avg Occupancy |
Avg Speed |
