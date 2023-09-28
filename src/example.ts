import { External, $, ply } from "plywood";
import { druidRequesterFactory } from "plywood-druid-requester";
import * as config from "../config.json";

async function runExample() {
  let counter = 1;

  const requester = druidRequesterFactory({
    // Sets the hostname via "locator" and "urlBuilder" instead of "host" to circumvent the default
    // URL building logic inside of plywood-druid-requester that would try to change the port.
    //
    // See https://docs.imply.io/api/polaris/api-reference for hostname format
    locator: () => Promise.resolve({ hostname: config.full_host_url }),
    urlBuilder: () => config.full_host_url,
    protocol: "tls",

    // Polaris API tokens are provided as basic auth tokens with an empty password
    authToken: {
      type: "basic-auth",
      username: config.api_key,
      password: "",
    },

    getQueryId: () =>
      `${config.name || "example"}-${Math.floor(
        Date.now() / 1000
      )}-${counter++}`,

    // The list of endpoint overrides. By using the provided list, you are able to use both native queries
    // and SQL queries, so you are able to slowly migrate away from the native queries and test them on SQL.
    endpointOverrides: {
      native: "/query/native",
      sql: "/query/sql",
      introspect: "/query/native/datasources",
      sourceList: "/query/native/datasources",
    },
  });

  const wikiDataset = External.fromJS(
    {
      engine: "druid", // use druid for native queries or use `druidsql` for sql querires
      version: "0.28.0", // hard coded to avoid the need for `/status` api calls which are not supported in Polaris
      source: "wikiticker", // The table name in Polaris
      context: {
        timeout: 10000, // The Druid context
      },
    },
    requester
  );

  const ex = ply()
    .apply(
      "wiki",
      $("wiki").filter(
        $("__time").overlap({
          start: new Date("2022-06-02T17:14:49.656Z"),
          end: new Date("2023-06-02T17:14:49.656Z"),
        })
      )
    )
    .apply("Count", $("wiki").count())
    .apply("TotalAdded", "$wiki.sum($added)")
    .apply(
      "Pages",
      $("wiki")
        .split("$page", "Page")
        .apply("Count", $("wiki").count())
        .sort("$Count", "descending")
        .limit(6)
    );

  const result = await ex.compute({ wiki: wikiDataset });

  console.log(JSON.stringify(result, null, 2));
}

runExample().catch((e) => {
  console.error(e);
});
