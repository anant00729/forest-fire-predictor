
-- check errors
SELECT * FROM stl_load_errors ORDER BY starttime DESC;

---------------------------------------
-- create tables in redshift before copying data from CSVs in S3

CREATE TABLE IF NOT EXISTS "CO_1980_2008" (
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"TIME"	TEXT,
	"STATION_NAME"	TEXT,
	"STATION_NAME_FULL"	TEXT,
	"EMS_ID"	TEXT,
	"NAPS_ID"	INTEGER,
	"RAW_VALUE"	REAL,
	"ROUNDED_VALUE"	REAL,
	"UNIT"	TEXT,
	"INSTRUMENT"	TEXT,
	"PARAMETER"	TEXT,
	"OWNER"	TEXT,
	"REGION"	TEXT,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"DAY"	INTEGER
);
CREATE TABLE IF NOT EXISTS "CO_1980_2008_stats_monthly" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "CO_1980_2008_stats_annual" (
	"NAPS_ID"	INTEGER,
	"EMSID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "CO_1980_2008_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "CO_2009_2020" (
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"TIME"	TEXT,
	"STATION_NAME"	TEXT,
	"STATION_NAME_FULL"	TEXT,
	"EMS_ID"	TEXT,
	"NAPS_ID"	INTEGER,
	"RAW_VALUE"	TEXT,
	"ROUNDED_VALUE"	TEXT,
	"UNIT"	TEXT,
	"INSTRUMENT"	TEXT,
	"PARAMETER"	TEXT,
	"OWNER"	TEXT,
	"REGION"	TEXT,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"DAY"	INTEGER
);
CREATE TABLE IF NOT EXISTS "CO_2009_2020_stats_monthly" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "CO_2009_2020_stats_annual" (
	"NAPS_ID"	INTEGER,
	"EMSID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "CO_2009_2020_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "NO_1980_2008" (
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"TIME"	TEXT,
	"STATION_NAME"	TEXT,
	"STATION_NAME_FULL"	TEXT,
	"EMS_ID"	TEXT,
	"NAPS_ID"	INTEGER,
	"RAW_VALUE"	TEXT,
	"ROUNDED_VALUE"	TEXT,
	"UNIT"	TEXT,
	"INSTRUMENT"	TEXT,
	"PARAMETER"	TEXT,
	"OWNER"	TEXT,
	"REGION"	TEXT,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"DAY"	INTEGER
);
CREATE TABLE IF NOT EXISTS "NO_1980_2008_stats_monthly" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "NO_1980_2008_stats_annual" (
	"NAPS_ID"	INTEGER,
	"EMSID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "NO_1980_2008_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "NO_2009_2020" (
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"TIME"	TEXT,
	"STATION_NAME"	TEXT,
	"STATION_NAME_FULL"	TEXT,
	"EMS_ID"	TEXT,
	"NAPS_ID"	INTEGER,
	"RAW_VALUE"	TEXT,
	"ROUNDED_VALUE"	TEXT,
	"UNIT"	TEXT,
	"INSTRUMENT"	TEXT,
	"PARAMETER"	TEXT,
	"OWNER"	TEXT,
	"REGION"	TEXT,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"DAY"	INTEGER
);
CREATE TABLE IF NOT EXISTS "NO_2009_2020_stats_monthly" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "NO_2009_2020_stats_annual" (
	"NAPS_ID"	INTEGER,
	"EMSID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "NO_2009_2020_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "SO2_1980_2008" (
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"TIME"	TEXT,
	"STATION_NAME"	TEXT,
	"STATION_NAME_FULL"	TEXT,
	"EMS_ID"	TEXT,
	"NAPS_ID"	INTEGER,
	"RAW_VALUE"	TEXT,
	"ROUNDED_VALUE"	TEXT,
	"UNIT"	TEXT,
	"INSTRUMENT"	TEXT,
	"PARAMETER"	TEXT,
	"OWNER"	TEXT,
	"REGION"	TEXT,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"DAY"	INTEGER
);
CREATE TABLE IF NOT EXISTS "SO2_1980_2008_stats_monthly" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "SO2_1980_2008_stats_annual" (
	"NAPS_ID"	INTEGER,
	"EMSID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "SO2_1980_2008_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "SO2_2009_2020" (
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"TIME"	TEXT,
	"STATION_NAME"	TEXT,
	"STATION_NAME_FULL"	TEXT,
	"EMS_ID"	TEXT,
	"NAPS_ID"	INTEGER,
	"RAW_VALUE"	TEXT,
	"ROUNDED_VALUE"	TEXT,
	"UNIT"	TEXT,
	"INSTRUMENT"	TEXT,
	"PARAMETER"	TEXT,
	"OWNER"	TEXT,
	"REGION"	TEXT,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"DAY"	INTEGER
);
CREATE TABLE IF NOT EXISTS "SO2_2009_2020_stats_monthly" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "SO2_2009_2020_stats_annual" (
	"NAPS_ID"	INTEGER,
	"EMSID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "SO2_2009_2020_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "O3_1980_2008" (
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"TIME"	TEXT,
	"STATION_NAME"	TEXT,
	"STATION_NAME_FULL"	TEXT,
	"EMS_ID"	TEXT,
	"NAPS_ID"	INTEGER,
	"RAW_VALUE"	TEXT,
	"ROUNDED_VALUE"	TEXT,
	"UNIT"	TEXT,
	"INSTRUMENT"	TEXT,
	"PARAMETER"	TEXT,
	"OWNER"	TEXT,
	"REGION"	TEXT,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"DAY"	INTEGER
);
CREATE TABLE IF NOT EXISTS "O3_2009_2020" (
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"TIME"	TEXT,
	"STATION_NAME"	TEXT,
	"STATION_NAME_FULL"	TEXT,
	"EMS_ID"	TEXT,
	"NAPS_ID"	INTEGER,
	"RAW_VALUE"	TEXT,
	"ROUNDED_VALUE"	TEXT,
	"UNIT"	TEXT,
	"INSTRUMENT"	TEXT,
	"PARAMETER"	TEXT,
	"OWNER"	TEXT,
	"REGION"	TEXT,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"DAY"	INTEGER
);
CREATE TABLE IF NOT EXISTS "O3_1980_2008_stats_monthly" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "O3_1980_2008_stats_annual" (
	"NAPS_ID"	INTEGER,
	"EMSID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "O3_1980_2008_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "O3_2009_2020_stats_monthly" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "O3_2009_2020_stats_annual" (
	"NAPS_ID"	INTEGER,
	"EMSID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"ANNUAL1-HR_AVG"	FLOAT
);
CREATE TABLE IF NOT EXISTS "O3_2009_2020_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT,
	"ANNUAL1-HR_AVG"	FLOAT
);
COMMIT;


----------------------------------------------------
CREATE TABLE IF NOT EXISTS "NO2_1980_2008_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT,
	"ANNUAL1-HR_AVG"	FLOAT
);

CREATE TABLE IF NOT EXISTS "NO2_2009_2020_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT,
	"ANNUAL1-HR_AVG"	FLOAT
);

CREATE TABLE IF NOT EXISTS "PM10_1980_2008_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT,
	"ANNUAL1-HR_AVG"	FLOAT
);

CREATE TABLE IF NOT EXISTS "PM10_2009_2020_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT,
	"ANNUAL1-HR_AVG"	FLOAT
);

CREATE TABLE IF NOT EXISTS "PM25_1980_2008_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT,
	"ANNUAL1-HR_AVG"	FLOAT
);

CREATE TABLE IF NOT EXISTS "PM25_2009_2020_stats" (
	"NAPS_ID"	INTEGER,
	"EMS_ID"	TEXT,
	"STATION_NAME"	TEXT,
	"REGION"	TEXT,
	"OWNER"	TEXT,
	"DATE_PST"	TEXT,
	"DATE"	DATE,
	"YEAR"	INTEGER,
	"MONTH"	INTEGER,
	"MONTHLY1-HR_AVG"	FLOAT,
	"ANNUAL1-HR_AVG"	FLOAT
);
----------------------------------------------------

-- copy data from CSV files into redshift tables
COPY CO_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/CO_1980_2008_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY CO_2009_2020
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/CO_2009_2020.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;


COPY NO_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/NO_1980_2008_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY NO_2009_2020
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/NO_2009_2020.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY O3_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/O3_1980_2008_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY O3_2009_2020
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/O3_2009_2020.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY SO2_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/SO2_1980_2008_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY SO2_2009_2020
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/SO2_2009_2020.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;
---------------------------------------


COPY CO_1980_2008_stats_monthly
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/CO_1980_2008_stats_monthly.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY CO_1980_2008_stats_annual
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/CO_1980_2008_stats_annual.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY CO_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/CO_1980_2008_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY CO_2009_2020
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/CO_2009_2020.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;


COPY CO_2009_2020_stats_monthly
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/CO_2009_2020_stats_monthly.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;


COPY CO_2009_2020_stats_annual
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/CO_2009_2020_stats_annual.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;


COPY CO_2009_2020_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/CO_2009_2020_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

---------------------------------------

COPY NO_1980_2008_stats_monthly
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/NO_1980_2008_stats_monthly.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY NO_1980_2008_stats_annual
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/NO_1980_2008_stats_annual.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY NO_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/NO_1980_2008_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY NO_2009_2020
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/NO_2009_2020.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;


COPY NO_2009_2020_stats_monthly
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/NO_2009_2020_stats_monthly.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;


COPY NO_2009_2020_stats_annual
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/NO_2009_2020_stats_annual.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;


COPY NO_2009_2020_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/NO_2009_2020_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

---------------------------------------

COPY SO2_1980_2008
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/SO2_1980_2008.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;


COPY SO2_1980_2008_stats_monthly
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/SO2_1980_2008_stats_monthly.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY SO2_1980_2008_stats_annual
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/SO2_1980_2008_stats_annual.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY SO2_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/SO2_1980_2008_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY SO2_2009_2020
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/SO2_2009_2020.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY SO2_2009_2020_stats_monthly
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/SO2_2009_2020_stats_monthly.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY SO2_2009_2020_stats_annual
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/SO2_2009_2020_stats_annual.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY SO2_2009_2020_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/SO2_2009_2020_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

---------------------------------------
COPY O3_1980_2008
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/O3_1980_2008.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;


COPY O3_1980_2008_stats_monthly
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/O3_1980_2008_stats_monthly.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY O3_1980_2008_stats_annual
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/O3_1980_2008_stats_annual.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY O3_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/O3_1980_2008_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY O3_2009_2020
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/O3_2009_2020.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY O3_2009_2020_stats_monthly
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/O3_2009_2020_stats_monthly.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY O3_2009_2020_stats_annual
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/O3_2009_2020_stats_annual.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY O3_2009_2020_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/O3_2009_2020_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

---------------------------------------

COPY NO2_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/NO2_1980_2008_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY NO2_2009_2020_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/NO2_2009_2020_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY PM10_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/PM10_1980_2008_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY PM10_2009_2020_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/PM10_2009_2020_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY PM25_1980_2008_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/PM25_1980_2008_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;

COPY PM25_2009_2020_stats
FROM 's3://rishabh-kaushal-unique/big data 732 project/final sql cleaned files/PM25_2009_2020_stats.csv'
IAM_ROLE 'arn:aws:iam::455125577962:role/RedshiftS3Full'
IGNOREHEADER 1
CSV;
