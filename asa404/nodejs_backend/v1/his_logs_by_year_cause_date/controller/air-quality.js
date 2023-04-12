db = require("../../../database/connect-redshift");

// http://localhost:5443/v1/fire/getAnnualHrAvg/CO_1980_2008_stats/01 - Vancouver Island
// http://fire-forrest-maps.herokuapp.com/v1/fire/getAnnualHrAvg/CO_1980_2008_stats/01 - Vancouver Island
https: exports.get_annual_hr_avg = async (req, res) => {
  let air_type = req.params.air_type || "CO_1980_2008_stats";
  let region_name = req.params.region_name || "01 - Vancouver Island";

  let q1 = `SELECT * from public."${air_type}" WHERE "REGION"=(:region_name);`;
  try {
    let res_d = await db.query(q1, {
      replacements: { air_type, region_name },
    });
    if (res_d[0].length === 0) {
      res.json({ status: false, message: "Data not Found" });
    } else {
      res.json({ status: true, message: "Data Found", data: res_d[0] });
    }
  } catch (error) {
    res.json({ status: false, message: error.message });
  }
};

// http://localhost:5443/v1/fire/getMonthlyHrAvg/CO_1980_2008_stats/01 - Vancouver Island/1980
// http://fire-forrest-maps.herokuapp.com/v1/fire/getMonthlyHrAvg/CO_1980_2008_stats/01 - Vancouver Island/1980
exports.get_monthly_hr_avg = async (req, res) => {
  let air_type = req.params.air_type || "CO_1980_2008_stats";
  let region_name = req.params.region_name || "01 - Vancouver Island";
  let year = req.params.year || "1980";

  let q1 = `SELECT * from public."${air_type}" WHERE "REGION"=(:region_name) AND "YEAR"=(:year);`;
  try {
    let res_d = await db.query(q1, {
      replacements: { air_type, region_name, year },
    });
    if (res_d[0].length === 0) {
      res.json({ status: false, message: "Data not Found" });
    } else {
      res.json({ status: true, message: "Data Found", data: res_d[0] });
    }
  } catch (error) {
    res.json({ status: false, message: error.message });
  }
};

// http://localhost:5443/v1/fire/getStationLocation/CO_1980_2008_stats/01 - Vancouver Island
// http://fire-forrest-maps.herokuapp.com/v1/fire/getStationLocation/CO_1980_2008_stats/01 - Vancouver Island
exports.get_station_location = async (req, res) => {
  let air_type = req.params.air_type || "CO_1980_2008_stats";
  let region_name = req.params.region_name || "01 - Vancouver Island";

  let q1 = `SELECT * from public."${air_type}" WHERE "REGION"=(:region_name);`;
  try {
    let res_d = await db.query(q1, { replacements: { air_type, region_name } });
    if (res_d[0].length === 0) {
      res.json({ status: false, message: "Data not Found" });
    } else {
      res.json({ status: true, message: "Data Found", data: res_d[0] });
    }
  } catch (error) {
    res.json({ status: false, message: error.message });
  }
};

// http://localhost:5443/v1/fire/get_all_air_quality_data/CO_1980_2008_stats
// http://fire-forrest-maps.herokuapp.com/v1/fire/get_all_air_quality_data/CO_1980_2008_stats
exports.get_all_air_quality_data = async (req, res) => {
  let air_type = req.params.air_type || "CO_1980_2008_stats";
  let q1 = `SELECT * from public."${air_type}" ORDER BY "YEAR", "MONTH"`;
  try {
    let res_d = await db.query(q1);
    if (res_d[0].length === 0) {
      res.json({ status: false, message: "Data not Found" });
    } else {
      // setTimeout(() => {
      res.json({ status: true, message: "Data Found", data: res_d[0] });
      // }, 2000);
    }
  } catch (error) {
    res.json({ status: false, message: error.message });
  }
};
