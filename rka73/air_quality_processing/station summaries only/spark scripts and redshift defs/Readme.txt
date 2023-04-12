Files in this directory (spark scripts and redshift defs)
------------------------------------------------------------------------------------------------------------------------------------------
1:  air_extract_df.py

  * air_extract_df.py reads a big raw csv file as a dataframe and removes the unnecessary rows containing NA values.
    This itself reduced the file size. Example, CO.csv file from 1980-2008 directory was of about 850 MB. This got reduced to about 500 MB (CO_1980_2008_cleaned.csv)
    It then saves it back in the .csv format.
    The output files are saved in this format air_year_range_cleaned.csv (eg: CO_1980_2008_cleaned.csv).

  * This file takes 2 command line inputs
    - input_file (the big raw csv file containing all rows from either 1980-2008 or 2009-2020 )
    - output_file (without .csv extension)

  * example command to run this file:
    spark-submit air_extract_df.py "/Volumes/RK - T7/big data project data/Annual_Summary/1980-2008/CO.csv" "/Volumes/RK - T7/big data project data/raw cleaned files/CO_1980_2008_cleaned"

------------------------------------------------------------------------------------------------------------------------------------------
2: air_transform_sql.py

  * air_transform_sql.py calculates the monthly and annual averages of different kinds of air.
    It takes the big cleaned csv file i.e. the output file of air_extract_df.py as it input.
    Then it uses SQL queries to transform and reduce those big file to only contain monthly and annual averages instead of daily hourly data.
    The output files are saved in this format air_year_range_cleaned_stats.csv (eg: CO_1980_2008_cleaned_stats.csv).

  * This file takes 2 command line inputs
    - input_file (the big cleaned csv file i.e. the output file of air_extract_df.py. It contains all rows from either 1980-2008 or 2009-2020 without NA values)
    - output_file (without .csv extension.)

  * example command to run this file:
    spark-submit air_transform_sql.py "/Volumes/RK - T7/big data project data/raw cleaned files/CO_1980_2008_cleaned.csv" "/Volumes/RK - T7/big data project data/spark transformed files/CO_1980_2008_cleaned_stats"

------------------------------------------------------------------------------------------------------------------------------------------
3: complile-run.txt

  * This file contains all the exact commands for air_extract_df.py and air_transform_sql.py used to extract and transform the air quality data.

------------------------------------------------------------------------------------------------------------------------------------------
4: redshift_table_defs_final.sql

  * Once the all the commands in complile-run.txt have been run, the transformed files are loaded into an S3 bucket

  * This file contains all the queries need to load the data from S3 bucket to Redshift.
    The queries are divided into 3 sections:

    -  1: (To start from the scratch) DROP all the TABLES to clean up the DB

    -  2: Table definitions for Redshift
          Tables need to be defined and created first in redshift before csv files could be imported into the redshift DB.

    -  3: Once the tables have been defined and created in redshift, load data into them from S3
    
------------------------------------------------------------------------------------------------------------------------------------------
