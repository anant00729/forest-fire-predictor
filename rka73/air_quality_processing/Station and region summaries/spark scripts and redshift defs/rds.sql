-- The following queries are divided into 3 sections:

--  1: (To start from the scratch) DROP all the TABLES to clean up the DB

--  2: Table definitions for Postgres
--    tables need to be defined and created first in Postgres before csv files could be imported into the Postgres DB

--  3: once the tables have been defined and created in Postgres,
--    load data into them from S3

------------------------------------------------------------------------------
--- To start from the scratch DROP all the TABLES to clean up the DB
DROP TABLE public."CO_1980_2008_stats";
DROP TABLE public."CO_2009_2020_stats";

DROP TABLE public."NO2_1980_2008_stats";
DROP TABLE public."NO2_2009_2020_stats";

DROP TABLE public."NO_1980_2008_stats";
DROP TABLE public."NO_2009_2020_stats";

DROP TABLE public."O3_1980_2008_stats";
DROP TABLE public."O3_2009_2020_stats";

DROP TABLE public."PM10_1980_2008_stats";
DROP TABLE public."PM10_2009_2020_stats";

DROP TABLE public."PM25_1980_2008_stats";
DROP TABLE public."PM25_2009_2020_stats";

DROP TABLE public."SO2_1980_2008_stats";
DROP TABLE public."SO2_2009_2020_stats";
------------------------------------------------------------------------------
-- table definitions for Postgres DB
-- tables need to be defined and created first in Postgres before csv files could be imported into the Postgres DB


CREATE TABLE IF NOT EXISTS "CO_1980_2008_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE"	TIMESTAMP,
	"MONTH"	INTEGER,
  "YEAR"	INTEGER,
	"MONTHLY1-HR_AVG-STATION"	FLOAT,
	"ANNUAL1-HR_AVG-STATION"	FLOAT,
  "MONTHLY1-HR_AVG-REGION"	FLOAT,
	"ANNUAL1-HR_AVG-REGION"		FLOAT,
	"LAT"  FLOAT,
	"LONG" FLOAT
);
CREATE TABLE IF NOT EXISTS "CO_2009_2020_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE"	TIMESTAMP,
	"MONTH"	INTEGER,
  "YEAR"	INTEGER,
	"MONTHLY1-HR_AVG-STATION"	FLOAT,
	"ANNUAL1-HR_AVG-STATION"	FLOAT,
  "MONTHLY1-HR_AVG-REGION"	FLOAT,
	"ANNUAL1-HR_AVG-REGION"		FLOAT,
	"LAT"  FLOAT,
	"LONG" FLOAT
);


CREATE TABLE IF NOT EXISTS "NO2_1980_2008_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE"	TIMESTAMP,
	"MONTH"	INTEGER,
  "YEAR"	INTEGER,
	"MONTHLY1-HR_AVG-STATION"	FLOAT,
	"ANNUAL1-HR_AVG-STATION"	FLOAT,
  "MONTHLY1-HR_AVG-REGION"	FLOAT,
	"ANNUAL1-HR_AVG-REGION"		FLOAT,
	"LAT"  FLOAT,
	"LONG" FLOAT
);
CREATE TABLE IF NOT EXISTS "NO2_2009_2020_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE"	TIMESTAMP,
	"MONTH"	INTEGER,
  "YEAR"	INTEGER,
	"MONTHLY1-HR_AVG-STATION"	FLOAT,
	"ANNUAL1-HR_AVG-STATION"	FLOAT,
  "MONTHLY1-HR_AVG-REGION"	FLOAT,
	"ANNUAL1-HR_AVG-REGION"		FLOAT,
	"LAT"  FLOAT,
	"LONG" FLOAT
);



CREATE TABLE IF NOT EXISTS "NO_1980_2008_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE"	TIMESTAMP,
	"MONTH"	INTEGER,
  "YEAR"	INTEGER,
	"MONTHLY1-HR_AVG-STATION"	FLOAT,
	"ANNUAL1-HR_AVG-STATION"	FLOAT,
  "MONTHLY1-HR_AVG-REGION"	FLOAT,
	"ANNUAL1-HR_AVG-REGION"		FLOAT,
	"LAT"  FLOAT,
	"LONG" FLOAT
);
CREATE TABLE IF NOT EXISTS "NO_2009_2020_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE"	TIMESTAMP,
	"MONTH"	INTEGER,
  "YEAR"	INTEGER,
	"MONTHLY1-HR_AVG-STATION"	FLOAT,
	"ANNUAL1-HR_AVG-STATION"	FLOAT,
  "MONTHLY1-HR_AVG-REGION"	FLOAT,
	"ANNUAL1-HR_AVG-REGION"		FLOAT,
	"LAT"  FLOAT,
	"LONG" FLOAT
);



CREATE TABLE IF NOT EXISTS "O3_1980_2008_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE"	TIMESTAMP,
	"MONTH"	INTEGER,
  "YEAR"	INTEGER,
	"MONTHLY1-HR_AVG-STATION"	FLOAT,
	"ANNUAL1-HR_AVG-STATION"	FLOAT,
  "MONTHLY1-HR_AVG-REGION"	FLOAT,
	"ANNUAL1-HR_AVG-REGION"		FLOAT,
	"LAT"  FLOAT,
	"LONG" FLOAT
);
CREATE TABLE IF NOT EXISTS "O3_2009_2020_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE"	TIMESTAMP,
	"MONTH"	INTEGER,
  "YEAR"	INTEGER,
	"MONTHLY1-HR_AVG-STATION"	FLOAT,
	"ANNUAL1-HR_AVG-STATION"	FLOAT,
  "MONTHLY1-HR_AVG-REGION"	FLOAT,
	"ANNUAL1-HR_AVG-REGION"		FLOAT,
	"LAT"  FLOAT,
	"LONG" FLOAT
);



CREATE TABLE IF NOT EXISTS "PM10_1980_2008_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE"	TIMESTAMP,
	"MONTH"	INTEGER,
  "YEAR"	INTEGER,
	"MONTHLY1-HR_AVG-STATION"	FLOAT,
	"ANNUAL1-HR_AVG-STATION"	FLOAT,
  "MONTHLY1-HR_AVG-REGION"	FLOAT,
	"ANNUAL1-HR_AVG-REGION"		FLOAT,
	"LAT"  FLOAT,
	"LONG" FLOAT
);
CREATE TABLE IF NOT EXISTS "PM10_2009_2020_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE"	TIMESTAMP,
	"MONTH"	INTEGER,
  "YEAR"	INTEGER,
	"MONTHLY1-HR_AVG-STATION"	FLOAT,
	"ANNUAL1-HR_AVG-STATION"	FLOAT,
  "MONTHLY1-HR_AVG-REGION"	FLOAT,
	"ANNUAL1-HR_AVG-REGION"		FLOAT,
	"LAT"  FLOAT,
	"LONG" FLOAT
);




CREATE TABLE IF NOT EXISTS "PM25_1980_2008_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE"	TIMESTAMP,
	"MONTH"	INTEGER,
  "YEAR"	INTEGER,
	"MONTHLY1-HR_AVG-STATION"	FLOAT,
	"ANNUAL1-HR_AVG-STATION"	FLOAT,
  "MONTHLY1-HR_AVG-REGION"	FLOAT,
	"ANNUAL1-HR_AVG-REGION"		FLOAT,
	"LAT"  FLOAT,
	"LONG" FLOAT
);
CREATE TABLE IF NOT EXISTS "PM25_2009_2020_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE"	TIMESTAMP,
	"MONTH"	INTEGER,
  "YEAR"	INTEGER,
	"MONTHLY1-HR_AVG-STATION"	FLOAT,
	"ANNUAL1-HR_AVG-STATION"	FLOAT,
  "MONTHLY1-HR_AVG-REGION"	FLOAT,
	"ANNUAL1-HR_AVG-REGION"		FLOAT,
	"LAT"  FLOAT,
	"LONG" FLOAT
);





CREATE TABLE IF NOT EXISTS "SO2_1980_2008_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE"	TIMESTAMP,
	"MONTH"	INTEGER,
  "YEAR"	INTEGER,
	"MONTHLY1-HR_AVG-STATION"	FLOAT,
	"ANNUAL1-HR_AVG-STATION"	FLOAT,
  "MONTHLY1-HR_AVG-REGION"	FLOAT,
	"ANNUAL1-HR_AVG-REGION"		FLOAT,
	"LAT"  FLOAT,
	"LONG" FLOAT
);
CREATE TABLE IF NOT EXISTS "SO2_2009_2020_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE"	TIMESTAMP,
	"MONTH"	INTEGER,
  "YEAR"	INTEGER,
	"MONTHLY1-HR_AVG-STATION"	FLOAT,
	"ANNUAL1-HR_AVG-STATION"	FLOAT,
  "MONTHLY1-HR_AVG-REGION"	FLOAT,
	"ANNUAL1-HR_AVG-REGION"		FLOAT,
	"LAT"  FLOAT,
	"LONG" FLOAT
);
------------------------------------------------------------------------------
-- once the tables have been defined and created in Postgres,
-- load data into them from S3

-- Syntax for importing csv files into Postgres tables:

-- SELECT aws_s3.table_import_from_s3(
-- 'POSTGRES_TABLE_NAME', 'event_id,event_name,event_value', '(format csv, header true)',
-- 'BUCKET_NAME',
-- 'FOLDER_NAME(optional)/FILE_NAME',
-- 'REGION',
-- 'AWS_ACCESS_KEY', 'AWS_SECRET_KEY', 'OPTIONAL_SESSION_TOKEN'
-- )


SELECT aws_s3.table_import_from_s3(
'"CO_1980_2008_stats"', '"NAPS_ID", "EMS_ID", "STATION_NAME", "REGION", "OWNER", "DATE", "MONTH", "YEAR", "MONTHLY1-HR_AVG-STATION", "ANNUAL1-HR_AVG-STATION", "MONTHLY1-HR_AVG-REGION", "ANNUAL1-HR_AVG-REGION", "LAT", "LONG"',
'(format csv, header true)',
'rishabh-kaushal-unique',
'big data 732 project/spark transformed files with geo coords/CO_1980_2008_cleaned_stats.csv',
'us-west-2',
'AKIAWT54ZGDVOCQGBBUE', '37wHgtnA/LvymPqijahbQoGTi6fKxwzO98tTBNb5'
);
SELECT aws_s3.table_import_from_s3(
'"CO_2009_2020_stats"', '"NAPS_ID", "EMS_ID", "STATION_NAME", "REGION", "OWNER", "DATE", "MONTH", "YEAR", "MONTHLY1-HR_AVG-STATION", "ANNUAL1-HR_AVG-STATION", "MONTHLY1-HR_AVG-REGION", "ANNUAL1-HR_AVG-REGION", "LAT", "LONG"',
'(format csv, header true)',
'rishabh-kaushal-unique',
'big data 732 project/spark transformed files with geo coords/CO_2009_2020_cleaned_stats.csv',
'us-west-2',
'AKIAWT54ZGDVOCQGBBUE', '37wHgtnA/LvymPqijahbQoGTi6fKxwzO98tTBNb5'
);


SELECT aws_s3.table_import_from_s3(
'"NO2_1980_2008_stats"', '"NAPS_ID", "EMS_ID", "STATION_NAME", "REGION", "OWNER", "DATE", "MONTH", "YEAR", "MONTHLY1-HR_AVG-STATION", "ANNUAL1-HR_AVG-STATION", "MONTHLY1-HR_AVG-REGION", "ANNUAL1-HR_AVG-REGION", "LAT", "LONG"',
'(format csv, header true)',
'rishabh-kaushal-unique',
'big data 732 project/spark transformed files with geo coords/NO2_1980_2008_cleaned_stats.csv',
'us-west-2',
'AKIAWT54ZGDVOCQGBBUE', '37wHgtnA/LvymPqijahbQoGTi6fKxwzO98tTBNb5'
);
SELECT aws_s3.table_import_from_s3(
'"NO2_2009_2020_stats"', '"NAPS_ID", "EMS_ID", "STATION_NAME", "REGION", "OWNER", "DATE", "MONTH", "YEAR", "MONTHLY1-HR_AVG-STATION", "ANNUAL1-HR_AVG-STATION", "MONTHLY1-HR_AVG-REGION", "ANNUAL1-HR_AVG-REGION", "LAT", "LONG"',
'(format csv, header true)',
'rishabh-kaushal-unique',
'big data 732 project/spark transformed files with geo coords/NO2_2009_2020_cleaned_stats.csv',
'us-west-2',
'AKIAWT54ZGDVOCQGBBUE', '37wHgtnA/LvymPqijahbQoGTi6fKxwzO98tTBNb5'
);


SELECT aws_s3.table_import_from_s3(
'"NO_1980_2008_stats"', '"NAPS_ID", "EMS_ID", "STATION_NAME", "REGION", "OWNER", "DATE", "MONTH", "YEAR", "MONTHLY1-HR_AVG-STATION", "ANNUAL1-HR_AVG-STATION", "MONTHLY1-HR_AVG-REGION", "ANNUAL1-HR_AVG-REGION", "LAT", "LONG"',
'(format csv, header true)',
'rishabh-kaushal-unique',
'big data 732 project/spark transformed files with geo coords/NO_1980_2008_cleaned_stats.csv',
'us-west-2',
'AKIAWT54ZGDVOCQGBBUE', '37wHgtnA/LvymPqijahbQoGTi6fKxwzO98tTBNb5'
);
SELECT aws_s3.table_import_from_s3(
'"NO_2009_2020_stats"', '"NAPS_ID", "EMS_ID", "STATION_NAME", "REGION", "OWNER", "DATE", "MONTH", "YEAR", "MONTHLY1-HR_AVG-STATION", "ANNUAL1-HR_AVG-STATION", "MONTHLY1-HR_AVG-REGION", "ANNUAL1-HR_AVG-REGION", "LAT", "LONG"',
'(format csv, header true)',
'rishabh-kaushal-unique',
'big data 732 project/spark transformed files with geo coords/NO_2009_2020_cleaned_stats.csv',
'us-west-2',
'AKIAWT54ZGDVOCQGBBUE', '37wHgtnA/LvymPqijahbQoGTi6fKxwzO98tTBNb5'
);



SELECT aws_s3.table_import_from_s3(
'"O3_1980_2008_stats"', '"NAPS_ID", "EMS_ID", "STATION_NAME", "REGION", "OWNER", "DATE", "MONTH", "YEAR", "MONTHLY1-HR_AVG-STATION", "ANNUAL1-HR_AVG-STATION", "MONTHLY1-HR_AVG-REGION", "ANNUAL1-HR_AVG-REGION", "LAT", "LONG"',
'(format csv, header true)',
'rishabh-kaushal-unique',
'big data 732 project/spark transformed files with geo coords/O3_1980_2008_cleaned_stats.csv',
'us-west-2',
'AKIAWT54ZGDVOCQGBBUE', '37wHgtnA/LvymPqijahbQoGTi6fKxwzO98tTBNb5'
);
SELECT aws_s3.table_import_from_s3(
'"O3_2009_2020_stats"', '"NAPS_ID", "EMS_ID", "STATION_NAME", "REGION", "OWNER", "DATE", "MONTH", "YEAR", "MONTHLY1-HR_AVG-STATION", "ANNUAL1-HR_AVG-STATION", "MONTHLY1-HR_AVG-REGION", "ANNUAL1-HR_AVG-REGION", "LAT", "LONG"',
'(format csv, header true)',
'rishabh-kaushal-unique',
'big data 732 project/spark transformed files with geo coords/O3_2009_2020_cleaned_stats.csv',
'us-west-2',
'AKIAWT54ZGDVOCQGBBUE', '37wHgtnA/LvymPqijahbQoGTi6fKxwzO98tTBNb5'
);


SELECT aws_s3.table_import_from_s3(
'"CO_1980_2008_stats"', '"NAPS_ID", "EMS_ID", "STATION_NAME", "REGION", "OWNER", "DATE", "MONTH", "YEAR", "MONTHLY1-HR_AVG-STATION", "ANNUAL1-HR_AVG-STATION", "MONTHLY1-HR_AVG-REGION", "ANNUAL1-HR_AVG-REGION", "LAT", "LONG"',
'(format csv, header true)',
'rishabh-kaushal-unique',
'big data 732 project/spark transformed files with geo coords/CO_1980_2008_cleaned_stats.csv',
'us-west-2',
'AKIAWT54ZGDVOCQGBBUE', '37wHgtnA/LvymPqijahbQoGTi6fKxwzO98tTBNb5'
);
SELECT aws_s3.table_import_from_s3(
'"PM10_2009_2020_stats"', '"NAPS_ID", "EMS_ID", "STATION_NAME", "REGION", "OWNER", "DATE", "MONTH", "YEAR", "MONTHLY1-HR_AVG-STATION", "ANNUAL1-HR_AVG-STATION", "MONTHLY1-HR_AVG-REGION", "ANNUAL1-HR_AVG-REGION", "LAT", "LONG"',
'(format csv, header true)',
'rishabh-kaushal-unique',
'big data 732 project/spark transformed files with geo coords/PM10_2009_2020_cleaned_stats.csv',
'us-west-2',
'AKIAWT54ZGDVOCQGBBUE', '37wHgtnA/LvymPqijahbQoGTi6fKxwzO98tTBNb5'
);


SELECT aws_s3.table_import_from_s3(
'"PM25_1980_2008_stats"', '"NAPS_ID", "EMS_ID", "STATION_NAME", "REGION", "OWNER", "DATE", "MONTH", "YEAR", "MONTHLY1-HR_AVG-STATION", "ANNUAL1-HR_AVG-STATION", "MONTHLY1-HR_AVG-REGION", "ANNUAL1-HR_AVG-REGION", "LAT", "LONG"',
'(format csv, header true)',
'rishabh-kaushal-unique',
'big data 732 project/spark transformed files with geo coords/PM25_1980_2008_cleaned_stats.csv',
'us-west-2',
'AKIAWT54ZGDVOCQGBBUE', '37wHgtnA/LvymPqijahbQoGTi6fKxwzO98tTBNb5'
);
SELECT aws_s3.table_import_from_s3(
'"PM25_2009_2020_stats"', '"NAPS_ID", "EMS_ID", "STATION_NAME", "REGION", "OWNER", "DATE", "MONTH", "YEAR", "MONTHLY1-HR_AVG-STATION", "ANNUAL1-HR_AVG-STATION", "MONTHLY1-HR_AVG-REGION", "ANNUAL1-HR_AVG-REGION", "LAT", "LONG"',
'(format csv, header true)',
'rishabh-kaushal-unique',
'big data 732 project/spark transformed files with geo coords/PM25_2009_2020_cleaned_stats.csv',
'us-west-2',
'AKIAWT54ZGDVOCQGBBUE', '37wHgtnA/LvymPqijahbQoGTi6fKxwzO98tTBNb5'
);


SELECT aws_s3.table_import_from_s3(
'"SO2_1980_2008_stats"', '"NAPS_ID", "EMS_ID", "STATION_NAME", "REGION", "OWNER", "DATE", "MONTH", "YEAR", "MONTHLY1-HR_AVG-STATION", "ANNUAL1-HR_AVG-STATION", "MONTHLY1-HR_AVG-REGION", "ANNUAL1-HR_AVG-REGION", "LAT", "LONG"',
'(format csv, header true)',
'rishabh-kaushal-unique',
'big data 732 project/spark transformed files with geo coords/SO2_1980_2008_cleaned_stats.csv',
'us-west-2',
'AKIAWT54ZGDVOCQGBBUE', '37wHgtnA/LvymPqijahbQoGTi6fKxwzO98tTBNb5'
);
SELECT aws_s3.table_import_from_s3(
'"SO2_2009_2020_stats"', '"NAPS_ID", "EMS_ID", "STATION_NAME", "REGION", "OWNER", "DATE", "MONTH", "YEAR", "MONTHLY1-HR_AVG-STATION", "ANNUAL1-HR_AVG-STATION", "MONTHLY1-HR_AVG-REGION", "ANNUAL1-HR_AVG-REGION", "LAT", "LONG"',
'(format csv, header true)',
'rishabh-kaushal-unique',
'big data 732 project/spark transformed files with geo coords/SO2_2009_2020_cleaned_stats.csv',
'us-west-2',
'AKIAWT54ZGDVOCQGBBUE', '37wHgtnA/LvymPqijahbQoGTi6fKxwzO98tTBNb5'
);

------------------------------------------------------------------------------
