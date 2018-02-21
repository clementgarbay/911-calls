var mongodb = require('mongodb');
var csv = require('csv-parser');
var fs = require('fs');

var MongoClient = mongodb.MongoClient;
var mongoUrl = 'mongodb://localhost:27017/911-calls';

var insertCalls = function(db, callback) {
    var collection = db.collection('calls');

    var calls = [];
    fs.createReadStream('../911.csv')
        .pipe(csv())
        .on('data', data => {
            calls.push({
                lat: data.lat.trim(),
                lng: data.lng.trim(),
                desc: data.desc.trim(),
                zip: data.zip.trim(),
                title_cat: data.title.split(':')[0].trim(),
                title_descr: data.title.split(':')[1].trim(),
                timeStamp: data.timeStamp.trim(),
                twp: data.twp.trim(),
                addr: data.addr.trim(),
                e: data.e.trim()
            });
        })
        .on('end', () => {
          collection.insertMany(calls, (err, result) => {
            callback(result)
          });
        });
}

MongoClient.connect(mongoUrl, (err, db) => {
    insertCalls(db, result => {
        console.log(`${result.insertedCount} calls inserted`);
        db.close();
    });
});
