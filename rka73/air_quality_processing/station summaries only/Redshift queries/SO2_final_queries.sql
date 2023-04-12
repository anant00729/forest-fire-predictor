-- TABLE SO2_1980_2008 - imported SO2.csv from 1980-2008 folder in Annual Summary
-- -- SO2ntains All the hourly data for SO2 from 1980 - 2008

CREATE TABLE "SO2_1980_2008_stats_annual" AS
  SELECT NAPS_ID, STATION_NAME, REGION,
    date_part(year, date) as YEAR,
    round(AVG(ROUNDED_VALUE), 3) as "ANNUAL1-HR_AVG"
  from SO2_1980_2008
  GROUP By NAPS_ID, EMS_ID, STATION_NAME, REGION, OWNER, YEAR
  ORDER BY YEAR;

CREATE TABLE "SO2_1980_2008_stats_monthly" AS
  SELECT NAPS_ID, STATION_NAME, REGION,
    date_part(year, date) as YEAR, date_part(month, date) as MONTH,
    round(AVG(ROUNDED_VALUE), 3) as "MONTHLY1-HR_AVG"
  from SO2_1980_2008
  GROUP By NAPS_ID, STATION_NAME, REGION, YEAR, MONTH
  ORDER BY YEAR, MONTH;


CREATE TABLE "SO2_1980_2008_stats" AS
  select m.NAPS_ID, m.STATION_NAME, m.REGION, m.YEAR, m.MONTH, m."MONTHLY1-HR_AVG", a."ANNUAL1-HR_AVG"
  from "SO2_1980_2008_stats_monthly" m
  inner join "SO2_1980_2008_stats_annual" a on m. "STATION_NAME" = a."STATION_NAME" and m. "YEAR" = a."YEAR"
  Group by m.NAPS_ID, m.STATION_NAME, m.REGION, m.YEAR, m.MONTH, m."MONTHLY1-HR_AVG", a."ANNUAL1-HR_AVG"
  order by m.STATION_NAME, m.YEAR, m.MONTH;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- TABLE SO2_2009_2020 - imported SO2.csv from 2009-LatestVerified folder in Annual Summary
-- -- SO2ntains All the hourly data for SO2 from 2009 - 2020

CREATE TABLE "SO2_2009_2020_stats_annual" AS
  SELECT NAPS_ID, STATION_NAME, REGION,
    date_part(year, date) as YEAR,
    round(AVG(ROUNDED_VALUE), 3) as "ANNUAL1-HR_AVG"
  from SO2_1980_2008
  GROUP By NAPS_ID, EMS_ID, STATION_NAME, REGION, OWNER, YEAR
  ORDER BY YEAR;

CREATE TABLE "SO2_2009_2020_stats_monthly" AS
  SELECT NAPS_ID, STATION_NAME, REGION,
    date_part(year, date) as YEAR, date_part(month, date) as MONTH,
    round(AVG(ROUNDED_VALUE), 3) as "MONTHLY1-HR_AVG"
  from SO2_1980_2008
  GROUP By NAPS_ID, STATION_NAME, REGION, YEAR, MONTH
  ORDER BY YEAR, MONTH;


CREATE TABLE "SO2_2009_2020_stats" AS
  select m.NAPS_ID, m.STATION_NAME, m.REGION, m.YEAR, m.MONTH, m."MONTHLY1-HR_AVG", a."ANNUAL1-HR_AVG"
  from "SO2_2009_2020_stats_monthly" m
  inner join "SO2_2009_2020_stats_annual" a on m. "STATION_NAME" = a."STATION_NAME" and m. "YEAR" = a."YEAR"
  Group by m.NAPS_ID, m.STATION_NAME, m.REGION, m.YEAR, m.MONTH, m."MONTHLY1-HR_AVG", a."ANNUAL1-HR_AVG"
  order by m.STATION_NAME, m.YEAR, m.MONTH;
