const Sequelize = require("sequelize");
url = "<db_url>";
const dbURI = url || "";
module.exports = new Sequelize(dbURI);
