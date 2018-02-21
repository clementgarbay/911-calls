const mongodb = require('mongodb');
const csv = require('csv-parser');
const fs = require('fs');

const MongoClient = mongodb.MongoClient;
const mongoUrl = 'mongodb://localhost:27017/911-calls';

const insertCalls = function(callsCollection, callback) {
    const calls = [];

    fs.createReadStream('../911.csv')
        .pipe(csv())
        .on('data', data => {
            calls.push({
                loc : {
                    type: "Point",
                    coordinates: [parseFloat(data.lng.trim()), parseFloat(data.lat.trim())]
                },
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
            callsCollection.insertMany(calls, (err, result) => {
                callback(result)
            });
        });
}

MongoClient.connect(mongoUrl, (err, db) => {
    const callsCollection = db.collection('calls');

    callsCollection.createIndex({ loc : "2dsphere" });
    callsCollection.createIndex({ title_descr : "text" });

    insertCalls(callsCollection, result => {
        console.log(`${result.insertedCount} calls inserted`);
        db.close();
    });
});
