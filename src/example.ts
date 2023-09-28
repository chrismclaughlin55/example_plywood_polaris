
import {External, $, ply} from 'plywood';
import {druidRequesterFactory } from 'plywood-druid-requester'
import * as config from "../config.json";

async function runExample(){
    let counter = 1;
    const requester = druidRequesterFactory({
        host: config.full_host_url,
        protocol: 'plain',
        urlBuilder: () => config.full_host_url,
        authToken: {
                    type: 'basic-auth', username: config.api_key, password:''
                },
        getQueryId: () => `${config.name || 'example'}-${Math.floor(Date.now()/1000)}-${counter++}`,
    endpointOverrides: {
      native: '/query/native',
      sql: '/query/native',
      introspect: '/query/native/datasources',
      sourceList: '/query/native/datasources',

  },

    })
    const wikiDataset = External.fromJS({
        engine: 'druid',
        version: "0.28.0",
        source: 'wikiticker',  // The datasource name in Druid
        timeAttribute: '__time',  // Druid's anonymous time attribute will be called 'time',
        context: {
          timeout: 10000 // The Druid context
        }
      }, requester);
    const ex = ply()
    .apply("wiki",
      $('wiki').filter($("__time").overlap({
        start: new Date("2022-06-02T17:14:49.656Z"),
        end: new Date("2023-06-02T17:14:49.656Z")
      }))
    )
    .apply('Count', $('wiki').count())
    .apply('TotalAdded', '$wiki.sum($added)')
    .apply('Pages',
      $('wiki').split('$page', 'Page')
        .apply('Count', $('wiki').count())
        .sort('$Count', 'descending')
        .limit(6)
    );
   
     const result = await ex.compute({wiki:wikiDataset});

    console.log(JSON.stringify(result, null, 2))

  }
  runExample();