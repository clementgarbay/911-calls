var elasticsearch = require('elasticsearch');
var csv = require('csv-parser');
var fs = require('fs');

var esClient = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'error'
});

esClient.indices.create({ index: 'calls' }, (err, resp) => {
  if (err) console.trace(err.message);
  if (!err) {
    esClient.indices.putMapping({
      index: 'calls',
      type: 'call',
      body: {
        call: {
          properties: {
            location: {
              type: "geo_point"
            }
          }
        }
      }
    }, (err, resp) => {
      if (err) console.trace(err.message);
    });
  }
});

let calls = [];

fs.createReadStream('../911.csv')
    .pipe(csv())
    .on('data', data => {
      calls.push({
        lat: data.lat,
        lng: data.lng,
        desc: data.desc,
        zip: data.zip,
        title_cat: data.title.split(':')[0].trim(),
        title_descr: data.title.split(':')[1].trim(),
        timeStamp: data.timeStamp,
        twp: data.twp,
        addr: data.addr,
        e: data.e
      });
    })
    .on('end', () => {
      esClient.bulk(createBulkInsertQuery(calls), (err, resp) => {
        if (err) console.trace(err.message);
        else console.log(`Inserted ${resp.items.length} 911 calls`);
        esClient.close();
      });
});

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(calls) {
  const body = calls.reduce((acc, call) => {
    const { lat,lng,desc,zip,title_cat,title_descr,timeStamp,twp,addr,e } = call;
    acc.push({ index: { _index: 'calls', _type: 'call'} });
    acc.push({ 
      location: {
        lat: lat,
        lon: lng
      },
      desc,
      zip,
      title_cat,
      title_descr,
      timeStamp,
      twp,
      addr,
      e });
    return acc;
  }, []);

  return { body };
}
