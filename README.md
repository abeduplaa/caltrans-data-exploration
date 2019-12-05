Analyzing San Francisco's traffic with python and OmniSci
==============================================

Note: Follow the instructions step by step to extract the data from the sources. However, if you just want to try the notebooks, then go straight there (however, you'll still need to load data from somewhere).

## Table of Contents


* [General Info](#general-info)
* [setup](#setup)
* [Extracting traffic data from Caltrans](#extracting-traffic-data)
* [Extracting weather](#extracting-weather)
* [Blog posts](#blog-posts)

## General Info

The state of California provides a an enormous database containing years of traffic sensor data. In this repo, there is code to:
* Extract weather data from skylab
* Extract traffic data from PeMS
* Extract weather data from noaa
* Transform and load data to OmniSci

**But, the best way to start is to go through the jupyter notebooks!**

## Setup

1. Preferably, use python 3.6
2. Install the requirements in requirements.txt: `pip install -r requirements.txt`
3. Create accounts at the appropriate places to be able to download the data.
4. Fill in the fields in `config.ini`. The code reads critical information, such as your login to Caltrans  from this file. **You will not be able to extract data without creating a free account.**
5. Download the correct html files with the appropriate links for data extraction (read below in [extracting traffic data...] (#extracting-traffic-data-from-caltrans))

Once everything is ready, you'll only need to run the files in `bin/` to extract data and load to OmniSci.

Order to run the files in:

1. `python bin/extract.py`
2. `python bin/extract_darksky_weather.py`
3. `python bin/transform_traffic_data_load_omnisci.py`

## Extracting traffic data

The data is provided by California Department of Transportation (CalTrans) and found in their Performance Measurement System (PeMS) database. 

CalTrans collects data in realtime from around 40,000 sensors!

To extract CalTrans traffic data, follow these steps:

0. Follow the setup steps
1. Set up the login info, paths, etc. in `config.ini`
2. Go to CalTrans PeMS website (http://pems.dot.ca.gov/) and login. 
3. Once in the website, navigate to the Data Clearinghouse (http://pems.dot.ca.gov/?dnode=Clearinghouse)
4. The Data Clearinghouse has the data you need. Unfortunately, scrapy hasn't been implemented yet for this project, so you'll need to download the html for the desired Traffic data type and district from the website and place it in `./html_files/`. I've already placed some sample files in there. 
5. Also important! Make sure to download the meta files for your district. These are necessary as they contain meta data regarding the stations. When transforming/loading to OmniSci, the code will read all meta files in the folder and join them together. All meta files for district 04 from 2015 to 2019 can be found in `data/meta/`.
6. You're ready to run: `python bin/extract.py`

## Extracting Weather

0. Follow the setup steps
1. Set up the login info, paths, etc. in `config.ini`
2. Create an API key at [darksky](https://darksky.net/dev) and add it to the `config.ini`. 
3. Open `bin/extract_darksky_weather.py` and configure the location, dates, etc
5. You're ready to run: `python bin/extract_darksky_weather.py `

**Note:** There is already data from NOAA included in `data/weather_noaa`. The script to download this data is also included but there are still some bugs.

## Transforming and loading to OmniSci

In order to load the data in, make sure to have OmniSci running and have put in your OmniSci credentials in `config.ini`.

1. Make sure you have all the data correctly downloaded and ready.
2. Open `transform_traffic_data_load_omnisci.py` and set the table name and other input parameters.
2. Run `python bin/transform_traffic_data_load_omnisci.py`

The data should now be in OmniSci and ready to visualize!

## Notebooks

The notebooks all require reading from OmniSci. Check them out to see how we created a model to: 

1. forecast traffic: `notebooks/Train_Models.ipynb` and `Prediction.ipynb`
2. Identify the severity of an accident: `notebooks/IncidentClassification.ipynb`

Try them out and also try some new ideas with the data!

## Blog posts

If you want to check out some of the insights we've found from the traffic data, you can read the blog posts here: 

1. [Blog post 1](https://www.omnisci.com/blog/analyzing-historical-traffic-flow-in-real-time-with-omnisci)
2. Blog post 2 on its way!
3. Blog post 3 on its way!


Feel free to contact me for any questions or to get in touch with OmniSci.

