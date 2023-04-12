const { Client } = require("cassandra-driver");
const cassandra = require("cassandra-driver");

const distance = cassandra.types.distance;
// heroku run bash -a fire-forrest-maps

const options = {
  cloud: {
    secureConnectBundle: "<base_url>",
  },
  credentials: {
    username: "<username>",
    password: "<password>",
  },
  keyspace: "asa404",
  pooling: {
    coreConnectionsPerHost: {
      [distance.local]: 2,
      [distance.remote]: 1,
    },
  },
};

const client = new Client(options);

exports.connectToCassandra = async () => {
  const rs = await client.execute(
    "select count(*) from his_logs_by_year_cause_date"
  );

  console.log(`Your cluster returned ${rs.rows[0].count.low} row(s)`);
};

exports.getClient = () => {
  return client;
};
