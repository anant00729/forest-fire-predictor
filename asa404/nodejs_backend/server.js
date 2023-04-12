const express = require("express");
const path = require("path");
// const map_sample = require("./map_sample.json");
const bc_map = require("./bc_map_partitions.json");
const cors = require("cors");
var bodyParser = require("body-parser");
const app = express();
const db = require("./database/connect-redshift");

const mongoose = require("mongoose");
const { connectToCassandra } = require("./database/connect-database.js");

// Authenticate DB
db.authenticate()
  .then(() =>
    console.log(
      "^^%&%^&^%^$%^&$%%^$%^$^ Wolla Connected to DB ^^%&%^&^%^$%^&$%%^$%^$^"
    )
  )
  .catch((err) => {
    console.log(`DB Connection failed ${err.message}`);
  });

app.use(cors());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

const FireForest = require("./database/FireForest");

connectToCassandra();

const DB_URL =
  "<mongo_db_uri>";

mongoose
  .connect(DB_URL)
  .then(() => console.log("successfully connected to mongo db"))
  .catch((err) => console.log(`error in DB connection : ${err.message}`));

app.use(express.static("public/build"));

app.use("/v1/fire", require("./v1/his_logs_by_year_cause_date/route"));

app.get("/insertFireForest", (req, res) => {
  // let sampleFeatures = map_sample.features.splice(0, 100);
  let sampleFeatures = map_sample.features.splice(-100);
  sampleFeatures = sampleFeatures.map((f_t) => {
    const {
      FIRE_NUMBER,
      FIRE_CAUSE,
      FIRE_YEAR,
      FIRE_LABEL,
      FIRE_SIZE_HECTARES,
      SOURCE,
      GPS_TRACK_DATE,
      LOAD_DATE,
      CREATION_METHOD,
      FEATURE_CODE,
      OBJECTID,
      SE_ANNO_CAD_DATA,
      FEATURE_AREA_SQM,
      FEATURE_LENGTH_M,
    } = f_t.properties;

    let coordinates = f_t.geometry.coordinates[0].map((coors) => {
      return coors.reverse();
    });

    const fireData = {
      coordinates,
      fire_number: FIRE_NUMBER,
      fire_cause: FIRE_CAUSE,
      fire_year: FIRE_YEAR,
      fire_label: FIRE_LABEL,
      fire_size_hectares: FIRE_SIZE_HECTARES,
      source: SOURCE,
      gps_track_date: GPS_TRACK_DATE,
      load_date: LOAD_DATE,
      creation_method: CREATION_METHOD,
      feature_code: FEATURE_CODE,
      object_id: OBJECTID,
      se_anno_cad_data: SE_ANNO_CAD_DATA,
      feature_area_sqm: FEATURE_AREA_SQM,
      feature_length_m: FEATURE_LENGTH_M,
      shape_area: f_t.properties["SHAPE.AREA"],
      shape_len: f_t.properties["SHAPE.LEN"],
    };
    return fireData;
  });

  FireForest.create(sampleFeatures, (err, instertedData) => {
    if (err) res.json(err);
    res.json({ status: true, instertedData });
  });
});

app.get("/getAllTheLocationData", async (req, res) => {
  try {
    const ffs = await FireForest.find().sort({ _id: -1 });
    res.json({ status: true, fireForests: ffs, length: ffs.length });
  } catch (error) {
    res.json({ status: false, error: error.message });
  }
});

app.get("/testApp", (req, res) => {
  let data = ["iqttt,0077", "obvhd,0093", "flohd,0075"];

  let data_kvs = [];

  data_kvs = data.map((ele, i) => {
    ele_p = ele.split(",");
    return {
      device_id: ele_p[0],
      usage_in_min: ele_p[1],
    };
  });

  resu = data_kvs.reduce((acc, cv, i) => {
    if (parseInt(acc.usage_in_min) < parseInt(cv.usage_in_min)) {
      acc = cv.usage_in_min;
    } else {
      acc = acc.usage_in_min;
    }
    return acc;
  });
  res.json({ result: resu });
});

app.get("/getFireForrestRecYearWise", async (req, res) => {
  try {
    const result = await FireForest.aggregate([
      {
        $group: {
          _id: "$fire_year",
          total_count: { $sum: 1 },
        },
      },
      { $sort: { _id: 1 } },
    ]);

    let max_count = Math.max.apply(
      Math,
      result.map(function (o) {
        return o.total_count;
      })
    );

    let min = parseInt(result[0]._id);
    let max = parseInt(result[result.length - 1]._id);

    let rangeList = partitionTheYears(min, max);

    rangeList = rangeList.map((rangeObj) => {
      rangeObj.yearsGroup = [];
      result.forEach((data) => {
        let cyear = parseInt(data._id);
        if (cyear >= rangeObj.min && cyear <= rangeObj.max) {
          rangeObj.yearsGroup.push(data);
        }
      });
      return rangeObj;
    });

    res.json({
      status: true,
      result,
      max_count,
      firstYear: result[0]._id,
      lastYear: result[result.length - 1]._id,
      rangeList,
    });
  } catch (error) {
    res.json({ status: false, error: error.message });
  }
});

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

app.get("/getBCCoordinates", (req, res) => {
  res.json(bc_map);
});

app.get("/tets", (req, res) => {
  res.json(bc_map);
});

app.get("*", (req, res) => {
  res.sendFile(path.resolve(__dirname, "public", "build", "index.html"));
});

const PORT = process.env.PORT || 5443;

app.listen(PORT, () => console.log(`The App is running on PORT ${PORT}`));
