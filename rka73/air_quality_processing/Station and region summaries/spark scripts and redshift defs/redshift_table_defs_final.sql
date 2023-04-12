-- The following queries are divided into 3 sections:

--  1: (To start from the scratch) DROP all the TABLES to clean up the DB

--  2: Table definitions for Redshift
--    tables need to be defined and created first in redshift before csv files could be imported into the redshift DB

--  3: once the tables have been defined and created in redshift,
--    load data into them from S3

------------------------------------------------------------------------------
--- To start from the scratch DROP all the TABLES to clean up the DB
DROP TABLE CO_1980_2008_stats;
DROP TABLE CO_2009_2020_stats;

DROP TABLE NO2_1980_2008_stats;
DROP TABLE NO2_2009_2020_stats;

DROP TABLE NO_1980_2008_stats;
DROP TABLE NO_2009_2020_stats;

DROP TABLE O3_1980_2008_stats;
DROP TABLE O3_2009_2020_stats;

DROP TABLE PM10_1980_2008_stats;
DROP TABLE PM10_2009_2020_stats;

DROP TABLE PM25_1980_2008_stats;
DROP TABLE PM25_2009_2020_stats;

DROP TABLE SO2_1980_2008_stats;
DROP TABLE SO2_2009_2020_stats;
------------------------------------------------------------------------------
-- table definitions for Redshift
-- tables need to be defined and created first in redshift before csv files could be imported into the redshift DB

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
	"ANNUAL1-HR_AVG-REGION"		FLOAT
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
	"ANNUAL1-HR_AVG-REGION"		FLOAT
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
	"ANNUAL1-HR_AVG-REGION"		FLOAT
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
	"ANNUAL1-HR_AVG-REGION"		FLOAT
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
	"ANNUAL1-HR_AVG-REGION"		FLOAT
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
	"ANNUAL1-HR_AVG-REGION"		FLOAT
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
	"ANNUAL1-HR_AVG-REGION"		FLOAT
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
	"ANNUAL1-HR_AVG-REGION"		FLOAT
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
	"ANNUAL1-HR_AVG-REGION"		FLOAT
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
	"ANNUAL1-HR_AVG-REGION"		FLOAT
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
	"ANNUAL1-HR_AVG-REGION"		FLOAT
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
	"ANNUAL1-HR_AVG-REGION"		FLOAT
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
	"ANNUAL1-HR_AVG-REGION"		FLOAT
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
	"ANNUAL1-HR_AVG-REGION"		FLOAT
);
------------------------------------------------------------------------------
-- once the tables have been defined and created in redshift,
-- load data into them from S3
COPY CO_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/spark transformed files/CO_1980_2008_cleaned_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY CO_2009_2020_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/spark transformed files/CO_2009_2020_cleaned_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;




COPY NO_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/spark transformed files/NO_1980_2008_cleaned_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY NO_2009_2020_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/spark transformed files/NO_2009_2020_cleaned_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;




COPY NO2_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/spark transformed files/NO2_1980_2008_cleaned_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY NO2_2009_2020_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/spark transformed files/NO2_2009_2020_cleaned_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;





COPY O3_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/spark transformed files/O3_1980_2008_cleaned_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY O3_2009_2020_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/spark transformed files/O3_2009_2020_cleaned_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;





COPY PM10_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/spark transformed files/PM10_1980_2008_cleaned_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY PM10_2009_2020_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/spark transformed files/PM10_2009_2020_cleaned_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;



COPY PM25_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/spark transformed files/PM25_1980_2008_cleaned_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY PM25_2009_2020_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/spark transformed files/PM25_2009_2020_cleaned_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;



COPY SO2_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/spark transformed files/SO2_1980_2008_cleaned_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY SO2_2009_2020
FROM 's3://rishabh-kaushal-unique/big data 732 project/spark transformed files/SO2_2009_2020.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;
------------------------------------------------------------------------------
-- check errors
SELECT * FROM stl_load_errors ORDER BY starttime DESC;

------------------------------------------------------------------------------
