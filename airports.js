function loadAirports(callback) {

  /** Setup our config */
  var config = {
    mongodb: {
      host: '127.0.0.1',
      port: 27017,
      database: 'travelbank'
    },
    collection: 'Stations',
    skipFields: [
      'lat',
      'lon',
      'runway_length',
      'elev',
      'icao',
      'direct_flights',
      'carriers',
      'woeid'
    ]
  };

  Object.keys(config.mongodb).forEach(function (key) {
    if (process && process.env && process.env["MONGODB_" + key])
      config.mongodb[key] = process.env["MONGODB_" + key]
  })
  if (process && process.env && process.env["AIRPORTS_collection"])
    config.collection = process.env["AIRPORTS_collection"]

  /** Pull in dependencies */
  var mongodb = require('mongodb');
  var async = require('async');
  var csv = require('csv');
  var assert = require('assert');
  var fs = require('fs');
  var _ = require('lodash');

  /** Connect to mongo */
  mongodb.MongoClient.connect('mongodb://' + config.mongodb.host + ':' + config.mongodb.port + '/' + config.mongodb.database, function (err, db) {
    assert.equal(null, err);
    console.log('Connected to mongodb...');
    /** Read our CSV file */
    rawData = fs.readFileSync(__dirname + '/airports.csv', 'utf8');
    console.log('Read CSV file, converting to objects...');
    csv.parse(rawData, {columns: true}, function (err, data) {
      assert.equal(null, err);
      console.log('We have our data, setting up our collection...');
      /** Create our collection, then drop it */
      var collection = db.collection(config.collection);
      /** Drop it */
      collection.drop(function (err) {
        console.log('Dropped our collection...');
        collection.ensureIndex({'location': '2dsphere'}, function (err) {
          assert.equal(null, err);
          console.log('Created our index, now we are going to insert our documents...');
          newData = [];
          for (var x in data) {
            row = data[x];
            /** If our row has no name */
            // if( row[ 'name' ].trim() == '' ){
            //   continue;
            // }
            /** Setup our location field */
            row['location'] = {
              type: 'Point',
              coordinates: [
                parseFloat(row['lon']),
                parseFloat(row['lat'])
              ]
            };
            /** Unset our skipped fields */
            for (var xx in config.skipFields) {
              delete row[config.skipFields[xx]];
            }
            newData.push(row);
          }
          console.log('Transformed data, inserting into the collection...');
          data = _.chunk(newData, 500);
          async.each(data, function (data, cb) {
            collection.insert(data, function (err) {
              if (err) {
                console.log(err);
                //process.exit();
                callback()
                cb(err);
              } else {
                cb();
              }
            });
          }, function (err) {
            assert.equal(null, err);
            console.log('We\'re done, bailing!');
            callback()
            //process.exit();
          });
        });
      });
    });
  });
}
if(require.main == module) loadAirports(function(){
  process.exit()
})
module.exports = loadAirports
