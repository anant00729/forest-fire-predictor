Reference Note:
  Following is the main source of the air quality datasets from where the raw datasets were downloaded.
  On these downloaded datasets, various ETL were performed:
  https://catalogue.data.gov.bc.ca/dataset/air-quality-monitoring-verified-hourly-data/

--------------------------------------------------------------------------------------------------------------------------------
Other Note:
 This directory contains all the files, codes, queries etc to get monthly and annual averages by the stations.

 Since we are now using the monthly and annual averages by the region as well,
  these files are not actively being used.

 That said, the code in these files all work and would have been used if region summaries wouldn't have been needed.
--------------------------------------------------------------------------------------------------------------------------------


* spark scripts and redshift defs
  - This folder has the scripts and commands that are used to perform extraction and transformation on the raw datasets.
  -  It contains following files and further info about these files can be found in its Readme.txt file:
      air_extract_df.py
      air_transform_sql.py
      complile-run.txt
      redshift_table_defs_final.sql
      Readme.txt

* spark transformed files for stations only
  -  This directory contains the datasets that were the output of the ETL (.py) scripts written in PySpark.
     In other words the output files created by the spark scripts in the "spark scripts and redshift defs" directory.

* Redshift queries
  -  It contains all the Redshift queries that were run on AWS Redshift to clean and extract the monthly and annual averages.

* sqlite queries
  -  It contains all the SQL queries that were run on Sqlite DB to clean and extract the monthly and annual averages.
     Since this is a project for a Big Data course converted all these queries into Redshift queries and were put separately in the "Redshift queries" directory.




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
