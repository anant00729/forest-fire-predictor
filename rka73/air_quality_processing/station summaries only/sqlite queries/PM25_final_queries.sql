-- TABLE PM25_1980_2008 - imported PM25.csv from 1980-2008 folder in Annual Summary
-- -- Contains All the hourly data for PM25 from 1980 - 2008

-- add Year column
select strftime('%Y', DATE_PST) from `PM25_1980_2008`;
ALTER TABLE `PM25_1980_2008` ADD YEAR INT(4);
UPDATE `PM25_1980_2008` SET `YEAR` = strftime('%Y', `DATE`);
SELECT `DATE`, `YEAR` from `PM25_1980_2008`;

-- add Month column
select strftime('%m', DATE), DATE from `PM25_1980_2008`;
ALTER TABLE `PM25_1980_2008` ADD `MONTH` INT(2);
UPDATE `PM25_1980_2008` SET `MONTH` = strftime('%m', `DATE`);
SELECT `DATE`, `MONTH` from `PM25_1980_2008`;

-- add Date column
select strftime('%d', DATE), DATE from `PM25_1980_2008`;
ALTER TABLE `PM25_1980_2008` ADD `DAY` INT(2);
UPDATE `PM25_1980_2008` SET `DAY` = strftime('%d', `DATE`);
SELECT `DATE`, `DAY` from `PM25_1980_2008`;

--------------------------------------------------------------------------------
-- create stats table with a column containing MONTHLY 1 hr averages
CREATE TABLE `PM25_1980_2008_stats_monthly` AS
SELECT NAPS_ID, EMS_ID, STATION_NAME, REGION, OWNER, DATE_PST, DATE, YEAR, MONTH,
round(avg(ROUNDED_VALUE), 3) as `MONTHLY1-HR_AVG`
from `PM25_1980_2008`  GROUP By STATION_NAME, YEAR,MONTH;


-- create annual stats table like above but with a column containing ANNUAL 1 hr averages
CREATE TABLE `PM25_1980_2008_stats_annual` AS
SELECT NAPS_ID, EMS_ID as EMSID, STATION_NAME, REGION, OWNER, DATE_PST, DATE, YEAR,
round(avg(ROUNDED_VALUE), 3) as `ANNUAL1-HR_AVG`
from `PM25_1980_2008`  GROUP By STATION_NAME, YEAR;


-- created a separate table for monthly avg to make the join faster,
-- because self join was taking a real long time, possibly because the table is big.
-- Haven't tried WITH subqueries (temp tables), maybe because they'll also take long time similar to self joins

-- CREATE a final table after joining monthly and yearly tables
CREATE TABLE `PM25_1980_2008_stats` AS
select m.NAPS_ID, m.EMS_ID, m.STATION_NAME, m.REGION, m.OWNER, m.DATE_PST, m.DATE, m.YEAR, m.MONTH, `MONTHLY1-HR_AVG`, a.`ANNUAL1-HR_AVG`
from "PM25_1980_2008_stats_monthly" m
join "PM25_1980_2008_stats_annual" a on m. "STATION_NAME" = a."STATION_NAME" and m. "YEAR" = a."YEAR";
--------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- TABLE PM25_2009_2020 - imported PM25.csv from 2009-LatestVerified folder in Annual Summary
-- -- Contains All the hourly data for PM25 from 2009 - 2020


-- add Year column
select strftime('%Y', DATE_PST) from `PM25_2009_2020`;
ALTER TABLE `PM25_2009_2020` ADD YEAR INT(4);
UPDATE `PM25_2009_2020` SET `YEAR` = strftime('%Y', `DATE`);
SELECT `DATE`, `YEAR` from `PM25_2009_2020`;

-- add Month column
select strftime('%m', DATE), DATE from `PM25_2009_2020`;
ALTER TABLE `PM25_2009_2020` ADD `MONTH` INT(2);
UPDATE `PM25_2009_2020` SET `MONTH` = strftime('%m', `DATE`);
SELECT `DATE`, `MONTH` from `PM25_2009_2020`;

-- add Date column
select strftime('%d', DATE), DATE from `PM25_2009_2020`;
ALTER TABLE `PM25_2009_2020` ADD `DAY` INT(2);
UPDATE `PM25_2009_2020` SET `DAY` = strftime('%d', `DATE`);
SELECT `DATE`, `DAY` from `PM25_2009_2020`;


-- create stats table with a column containing MONTHLY 1 hr averages
CREATE TABLE `PM25_2009_2020_stats_monthly` AS
SELECT NAPS_ID, EMS_ID, STATION_NAME, REGION, OWNER, DATE_PST, DATE, YEAR, MONTH,
round(avg(ROUNDED_VALUE), 3) as `MONTHLY1-HR_AVG`
from `PM25_2009_2020`  GROUP By STATION_NAME, YEAR,MONTH;

-- create annual stats table like above but with a column containing ANNUAL 1 hr averages
CREATE TABLE `PM25_2009_2020_stats_annual` AS
SELECT NAPS_ID, EMS_ID as EMSID, STATION_NAME, REGION, OWNER, DATE_PST, DATE, YEAR,
round(avg(ROUNDED_VALUE), 3) as `ANNUAL1-HR_AVG`
from `PM25_2009_2020`  GROUP By STATION_NAME, YEAR;

-- created a separate table for monthly avg to make the join faster,
-- because self join was taking a real long time, possibly because the table is big.
-- Haven't tried WITH subqueries (temp tables), maybe because they'll also take long time similar to self joins

-- CREATE a final table after joining monthly and yearly tables
CREATE TABLE `PM25_2009_2020_stats` AS
select m.NAPS_ID, m.EMS_ID, m.STATION_NAME, m.REGION, m.OWNER, m.DATE_PST, m.DATE, m.YEAR, m.MONTH, `MONTHLY1-HR_AVG`, a.`ANNUAL1-HR_AVG`
from "PM25_2009_2020_stats_monthly" m
join "PM25_2009_2020_stats_annual" a on m. "STATION_NAME" = a."STATION_NAME" and m. "YEAR" = a."YEAR";
