const path = require("path");
const { Firebolt } = require("firebolt-sdk");

require("dotenv").config({ path: path.resolve("./.env") });

const connectionParams = {
  auth: {
    username: process.env.FIREBOLT_USERNAME,
    password: process.env.FIREBOLT_PASSWORD,
  },
  database: process.env.FIREBOLT_DATABASE,
  engineName: process.env.FIREBOLT_ENGINE_NAME,
};

const main = async () => {
  const queries = ["SELECT 1", "SELECT 2", "SELECT 3"];

  const clients = 300;
  const resultPromises = [];

  const firebolt = Firebolt({
    apiEndpoint: process.env.FIREBOLT_API_ENDPOINT,
  });

  const now = Date.now();

  const connection = await firebolt.connect(connectionParams);
  console.log(
    "----",
    "time spent connecting(ms):",
    Date.now() - now,
    "----------"
  );

  const executeQuery = async (query) => {
    const now = Date.now();
    const statement = await connection.execute(query);
    const { data, statistics } = await statement.fetchResult();
    console.log(
      `Query: ${query} returned with: ${data} in ${
        statistics.duration
      } s statistics time and ${
        (Date.now() - now) / 1000
      }s with fetching results time`
    );
    return data;
  };

  for (let i = 0; i < clients; i++) {
    const query = queries[i % queries.length];
    resultPromises.push(executeQuery(query));
  }

  const results = await Promise.all(resultPromises);

  console.log("----", "total time(ms):", Date.now() - now, "----------");
  return results;
};

main()
  .then((results) => {
    console.log(results);
  })
  .catch((error) => {
    console.log(error);
  });
