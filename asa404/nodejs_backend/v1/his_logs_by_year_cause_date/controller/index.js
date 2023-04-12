const { getClient } = require("../../../database/connect-database");
const fire_station = require("../../../public/fire_station.json");
const cleaned_fire_station = require("../../../public/cleaned_fire_station.json");
const { main_temp, main_obj } = require("../../../template_gson");
const { cleaned_data } = require("../../../test");
const fs = require("fs");

/*
 * @route  v1/get_find_fire_history
 * @type   POST
 * @access public
 */
exports.get_find_fire_history = async (req, res) => {
  const rs = await getClient().execute(
    "select * from his_logs_by_year_cause_date limit 2;"
  );
  res.json({ status: true, result: rs.rows });
};

// SELECT fire_date from his_logs_by_year_cause_date WHERE fire_date >= '2013-01-01 00:00:00+0200' AND  fire_date <= '2015-01-01 23:59:00+0200'

function compare(a, b) {
  if (a.year < b.year) {
    return -1;
  }
  if (a.year > b.year) {
    return 1;
  }
  return 0;
}
//http:localhost:5444/v1/fire/findFireHistoryByYear
exports.get_all_history_data_by_year = async (req, res) => {
  const rs = await getClient().execute("SELECT * FROM history_count_by_year");

  result = rs.rows.sort(compare);

  let min = parseInt(result[0].year);
  let max = parseInt(result[result.length - 1].year);

  let rangeList = partitionTheYears(min, max);

  rangeList = rangeList.map((rangeObj) => {
    rangeObj.yearsGroup = [];
    result.forEach((data) => {
      let cyear = parseInt(data.year);
      if (cyear >= rangeObj.min && cyear <= rangeObj.max) {
        rangeObj.yearsGroup.push(data);
      }
    });
    return rangeObj;
  });

  res.json({
    status: true,
    result,
    firstYear: result[0].year,
    lastYear: result[result.length - 1].year,
    rangeList,
    // year_gp,
    rangeList,
  });
  // /res.json({ recordCount: rs.rows });
};

//http:localhost:5444/v1/fire/findFireHistoryNew
exports.findFireHistory = async (req, res) => {
  selected_year = req.params.selected_year;

  console.log(`selected_year`, selected_year);

  const rs = await getClient().execute("SELECT * FROM history_count_by_year");

  result = rs.rows.sort(compare);

  let min = parseInt(result[0].year);
  let max = parseInt(result[result.length - 1].year);

  let rangeList = partitionTheYears(min, max);

  rangeList = rangeList.map((rangeObj) => {
    rangeObj.yearsGroup = [];
    result.forEach((data) => {
      let cyear = parseInt(data.year);
      if (cyear >= rangeObj.min && cyear <= rangeObj.max) {
        rangeObj.yearsGroup.push(data);
      }
    });
    return rangeObj;
  });

  let year_gp = rangeList.find((r) => r.rangeTitle == selected_year);

  if (!year_gp) {
    year_gp = rangeList.find((r) => r.rangeTitle == "1917 - 1927");
  }

  let x_axis_years = year_gp.yearsGroup.map((y) => y.year.toString());
  let y_axis_count = year_gp.yearsGroup.map((y) => y.count);

  rangeTitles = rangeList.map((r) => r.rangeTitle);
  res.json({
    status: true,
    // result,
    // firstYear: result[0].year,
    // lastYear: result[result.length - 1].year,
    // rangeList,
    // year_gp,
    rangeTitles,
    x_axis_years,
    y_axis_count,
  });
  // /res.json({ recordCount: rs.rows });
};

const partitionTheYears = (min, max) => {
  let range = 10;
  let rangeList = [];
  for (let i = min; i <= max; ++i) {
    if (range == 10 && i != range) {
      range = range + i;
      let rangeObj = {
        min: i,
        max: range,
        rangeTitle: `${i} - ${range >= 2021 ? "Present" : range}`,
      };
      rangeList.push(rangeObj);
    }
    if (i == range) {
      range = 10;
    }
  }
  return rangeList;
};

exports.firefindFireCauseVsCountByYear = async (req, res) => {
  selected_year = req.params.selected_year;

  const rs = await getClient().execute(
    "SELECT * FROM his_year_fire_cause_vs_count"
  );

  result = rs.rows.sort(compare);

  let min = parseInt(result[0].year);
  let max = parseInt(result[result.length - 1].year);

  let rangeList = partitionTheYears(min, max);

  rangeList = rangeList.map((rangeObj) => {
    rangeObj.yearsGroup = [];
    result.forEach((data) => {
      let cyear = parseInt(data.year);
      if (cyear >= rangeObj.min && cyear <= rangeObj.max) {
        rangeObj.yearsGroup.push(data);
      }
    });
    return rangeObj;
  });

  res.json({
    status: true,
    result,
    firstYear: result[0].year,
    lastYear: result[result.length - 1].year,
    rangeList,
  });
  // /res.json({ recordCount: rs.rows });
};

exports.firefindFireCauseVsCount = async (req, res) => {
  selected_year = req.params.selected_year;

  const rs = await getClient().execute(
    "SELECT * FROM his_year_fire_cause_vs_count"
  );

  result = rs.rows.sort(compare);

  let min = parseInt(result[0].year);
  let max = parseInt(result[result.length - 1].year);

  let rangeList = partitionTheYears(min, max);

  rangeList = rangeList.map((rangeObj) => {
    rangeObj.yearsGroup = [];
    result.forEach((data) => {
      let cyear = parseInt(data.year);
      if (cyear >= rangeObj.min && cyear <= rangeObj.max) {
        rangeObj.yearsGroup.push(data);
      }
    });
    return rangeObj;
  });

  let year_gp = rangeList.find((r) => r.rangeTitle == selected_year);

  if (!year_gp) {
    year_gp = rangeList.find((r) => r.rangeTitle == "1917 - 1927");
  }

  person_count = [];
  lightening_count = [];
  years = [];

  if (year_gp.max >= 2021) {
    year_gp.max = 2021;
  }
  for (let ij = year_gp.min; ij <= year_gp.max; ++ij) {
    // for (let yo of year_gp.yearsGroup) {

    // }
    person = year_gp.yearsGroup.find(
      (y) => y.fire_cause == "Person" && ij == y.year
    );
    lightening = year_gp.yearsGroup.find(
      (y) => y.fire_cause == "Lightning" && ij == y.year
    );
    if (!person) {
      person = { count: 0 };
    }
    if (!lightening) {
      lightening = { count: 0 };
    }
    person_count.push(person.count);
    lightening_count.push(lightening.count);
    years.push(ij);
  }

  // years = year_gp.yearsGroup
  //   .filter((r) => r.fire_cause == "Person")
  //   .map((r) => r.year.toString());

  // person_count = year_gp.yearsGroup
  //   .filter((r) => r.fire_cause == "Person")
  //   .map((r) => r.count);

  // lightening_count = year_gp.yearsGroup
  //   .filter((r) => r.fire_cause == "Lightning")
  //   .map((r) => r.count);

  rangeTitles = rangeList.map((r) => r.rangeTitle);

  res.json({
    status: true,
    // result,
    // firstYear: result[0].year,
    // lastYear: result[result.length - 1].year,
    // rangeList,
    // year_gp,
    rangeTitles,
    // persons,
    // lightenings,
    years,
    person_count,
    lightening_count,
  });
  // /res.json({ recordCount: rs.rows });
};

exports.findBCReginalAreas = async (req, res) => {
  const rs = await getClient().execute(
    "select * from bc_regional_area limit 25"
  );
  res.json({ status: true, result: rs.rows });
};

exports.findBCReginalAreasNew = async (req, res) => {
  const rs = await getClient().execute(
    "select objectid,admin_area_name from regional_partition_bc WHERE id = 1"
  );
  res.json({ status: true, result: rs.rows });
};

exports.getFireStation = async (req, res) => {
  let center_name = req.params.center_name || "Northwest Fire Centre";
  let f_sta = { ...fire_station };
  f_sta.features = f_sta.features.filter(
    (f) => f.properties.MOF_FIRE_CENTRE_NAME == center_name
  );
  let size = f_sta.features.length;
  // delete fire_station.features[0].geometry.coordinates;
  // keys = [...Object.keys(fire_station)];
  // delete fire_station.features;
  res.json({ ...f_sta, size });
};

exports.getCleanedFireStation = async (req, res) => {
  let center_name = req.params.center_name || "Northwest Fire Centre";
  let f_sta = { ...cleaned_fire_station };
  f_sta.features = f_sta.features.filter((f) => {
    console.log(`f`, f);
    return f.properties.mof_fire_centre_name == center_name;
  });
  let size = f_sta.features.length;
  // delete fire_station.features[0].geometry.coordinates;
  // keys = [...Object.keys(fire_station)];
  // delete fire_station.features;
  res.json({ ...f_sta, size });
};

exports.createGsonFile = async (req, res) => {
  let main_t = { ...main_temp };
  // main_o = { ...main_obj };

  cleaned_data.forEach((c) => {
    let m = {
      type: "Feature",
      geometry: {
        type: "Polygon",
        coordinates: [],
      },
      properties: {},
    };
    // let main_o = { ...main_obj };
    m.geometry.coordinates = c.reduced_coordinates;
    m.properties.centeroid_lat = c.centeroid_lat;
    m.properties.centeroid_lng = c.centeroid_lng;
    m.properties.feature_area_sqm = c.feature_area_sqm;
    m.properties.feature_length_m = c.feature_length_m;
    m.properties.mof_fire_centre_id = c.mof_fire_centre_id;
    m.properties.mof_fire_centre_name = c.mof_fire_centre_name;
    m.properties.objectid = c.objectid;
    m.properties.r_coordinates_count = c.r_coordinates_count;
    main_t.features.push(m);
  });

  // main_o = { ...main_obj };
  // res.json(main_t);

  fs.writeFile("cleaned.json", JSON.stringify(main_t), function (err) {
    if (err) {
      return console.log(err);
    }
    console.log("The file was saved!");
    res.json({ message: "The file was saved!", ...main_t });
  });
};
