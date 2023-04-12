Reference Note:
  Following is the main source of the air quality datasets from where the raw datasets were downloaded.
  On these downloaded datasets, various ETL were performed:
  https://catalogue.data.gov.bc.ca/dataset/air-quality-monitoring-verified-hourly-data/

--------------------------------------------------------------------------------------------------------------------------------

* spark scripts and redshift defs
  - This folder has the scripts and commands that are used to perform extraction and transformation on the raw datasets.
  -  It contains following files and further info about these files can be found in its Readme.txt file:
      air_extract_df.py
      air_transform_sql.py
      complile-run.txt
      redshift_table_defs_final.sql
      Readme.txt

* spark transformed files with geo coords
  - This folder has the final extracted, transformed and cleaned datasets
      that also includes the lat and long cols in the csv files.
    These lat and long geocords are that of (air quality monitoring) stations
      needed to plot the stations on the air quality dashboards.
  - It contains the following .csv files that have monthly and annual 1-hr averages:
      CO_1980_2008_cleaned_stats.csv
      CO_2009_2020_cleaned_stats.csv

      NO2_1980_2008_cleaned_stats.csv
      NO2_2009_2020_cleaned_stats.csv

      NO_1980_2008_cleaned_stats.csv
      NO_2009_2020_cleaned_stats.csv

      O3_1980_2008_cleaned_stats.csv
      O3_2009_2020_cleaned_stats.csv

      PM10_1980_2008_cleaned_stats.csv
      PM10_2009_2020_cleaned_stats.csv

      PM25_1980_2008_cleaned_stats.csv
      PM25_2009_2020_cleaned_stats.csv

      SO2_1980_2008_cleaned_stats.csv
      SO2_2009_2020_cleaned_stats.csv


* spark transformed files
  - This folder has the extracted, transformed and cleaned datasets excluding the latitudes in the csv files.
  - It contains the following .csv files that have monthly and annual 1-hr averages:
      CO_1980_2008_cleaned_stats.csv
      CO_2009_2020_cleaned_stats.csv

      NO2_1980_2008_cleaned_stats.csv
      NO2_2009_2020_cleaned_stats.csv

      NO_1980_2008_cleaned_stats.csv
      NO_2009_2020_cleaned_stats.csv

      O3_1980_2008_cleaned_stats.csv
      O3_2009_2020_cleaned_stats.csv

      PM10_1980_2008_cleaned_stats.csv
      PM10_2009_2020_cleaned_stats.csv

      PM25_1980_2008_cleaned_stats.csv
      PM25_2009_2020_cleaned_stats.csv

      SO2_1980_2008_cleaned_stats.csv
      SO2_2009_2020_cleaned_stats.csv

* Air Quality Monitoring Stations
  - This folder contains the following subdirectories and all these subdirectories have the CSV files that have been downloaded from:
      https://catalogue.data.gov.bc.ca/dataset/01867404-ba2a-470e-94b7-0604607cfa30
  - Its subdirectories contains the data regarding the
    geo-coordinates (lat and long) of the air quality monitoring stations of BC
    from 1980 - 2008 and from 2009 - 2020.
