const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const fireForestSchema = new Schema({
  coordinates: [[{ type: Number }, { type: Number }]],
  fire_number: {
    type: String,
  },
  fire_cause: {
    type: String,
  },
  fire_year: {
    type: String,
  },
  fire_label: {
    type: String,
  },
  fire_size_hectares: {
    type: String,
  },
  source: {
    type: String,
  },
  gps_track_date: {
    type: String,
  },
  load_date: {
    type: String,
  },
  creation_method: {
    type: String,
  },
  feature_code: {
    type: String,
  },
  object_id: {
    type: String,
  },
  se_anno_cad_data: {
    type: String,
  },
  feature_area_sqm: {
    type: String,
  },
  feature_length_m: {
    type: String,
  },
  shape_area: {
    type: String,
  },
  shape_len: {
    type: String,
  },
});

module.exports = Post = mongoose.model(
  "fire_forest_collection",
  fireForestSchema
);
