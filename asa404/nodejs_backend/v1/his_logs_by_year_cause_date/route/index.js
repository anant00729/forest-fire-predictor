const express = require("express");
const {
  get_all_history_data_by_year,
  get_find_fire_history,
  firefindFireCauseVsCountByYear,
  findBCReginalAreas,
  findFireHistory,
  firefindFireCauseVsCount,
  findBCReginalAreasNew,
  getFireStation,
  createGsonFile,
  getCleanedFireStation,
} = require("../controller");

const {
  get_annual_hr_avg,
  get_monthly_hr_avg,
  get_station_location,
  get_all_air_quality_data,
} = require("../controller/air-quality");

const _r = express.Router();

// wildfire
_r.get("/findFireHistory", get_find_fire_history);
_r.get("/findFireHistoryByYear", get_all_history_data_by_year);
_r.get("/firefindFireCauseVsCountByYear", firefindFireCauseVsCountByYear);
_r.get("/findFireHistoryNew/:selected_year", findFireHistory);
_r.get("/firefindFireCauseVsCount/:selected_year", firefindFireCauseVsCount);
_r.get("/findBCReginalAreas", findBCReginalAreas);
_r.get("/regionalPartitionBC", findBCReginalAreasNew);
_r.get("/getFireStation/:center_name", getFireStation);
_r.get("/createGsonFile", createGsonFile);
_r.get("/getCleanedFireStation/:center_name", getCleanedFireStation);

// air quality
_r.get("/getAnnualHrAvg/:air_type/:region_name", get_annual_hr_avg);
_r.get("/getMonthlyHrAvg/:air_type/:region_name/:year", get_monthly_hr_avg);
_r.get("/getStationLocation/:air_type/:region_name", get_station_location);
_r.get("/get_all_air_quality_data/:air_type", get_all_air_quality_data);
module.exports = _r;
