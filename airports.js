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
  ],
  geocoder: {
    baseUrl: 'https://maps.googleapis.com/maps/api/geocode/json?address=',
    keyUrl: '&key=AIzaSyCTFKDtVcvBiBK_KM6Y8uT2vNwBlIuvPSs',
    limit: 3,
    delay: 300
  },
  rail: {
    type: 'Railway Stations',
    csvInPath: '/amtrak_stations.csv',
    csvOutPath: 'rail.csv',
    headerRow: [ 'code','lat','lon','name','city','state','country','woeid','tz','phone','type','email','url','runway_length','elev','icao','direct_flights','carriers' ]
  },
  inputFiles: ['/airports.csv', '/rail.csv']
};

/** Pull in dependencies */
var mongodb = require('mongodb');
var async = require('async');
var csv = require('csv');
var assert = require('assert');
var fs = require('fs');
var _ = require('lodash');
var request = require('request');

function loadAirports(callback) {
  Object.keys(config.mongodb).forEach(function (key) {
    if (process && process.env && process.env["MONGODB_" + key])
      config.mongodb[key] = process.env["MONGODB_" + key]
  })
  if (process && process.env && process.env["AIRPORTS_collection"])
    config.collection = process.env["AIRPORTS_collection"]

  /** Connect to mongo */
  mongodb.MongoClient.connect('mongodb://' + config.mongodb.host + ':' + config.mongodb.port + '/' + config.mongodb.database, function (err, db) {
    assert.equal(null, err);
    console.log('Connected to mongodb...');
    /** Read our CSV file */
    _.each(config.inputFiles, function(inputFile) {
      rawData = fs.readFileSync(__dirname + inputFile, 'utf8');
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
  });
}

function regenerateCsv(callback) {
  // File from http://www.rita.dot.gov/bts/sites/rita.dot.gov.bts/files/AdditionalAttachmentFiles/amtrak_sta.zip
  // Converted from txt -> dbf (via rename) -> csv (via http://dbfconv.com/ then pipe it thru the unix 'strings' function to clean it up)
  rawRail = fs.readFileSync(__dirname + config.rail.csvInPath, 'utf8');
  allRailRows = [];
  allRailRows.push(config.rail.headerRow);
  console.log('Re-geocoding rail stations...');
  csv.parse(rawRail, {columns: true}, function (err, railData) {
    assert.equal(null, err);

    // Using the limit to throttle request rate so as not to exceed our query limit to google
    async.eachLimit (railData, config.geocoder.limit, function(railRow, cb) {
      var fullAddress = railRow['ADDRESS1'] + ' ' + railRow['CITY'] + ', ' + railRow['STATE'] + ' ' + railRow['ZIP'];
      var fullUrl = config.geocoder.baseUrl + encodeURIComponent(fullAddress) + config.geocoder.keyUrl;
      request(fullUrl, function(error, response, bodyString) {
        body = JSON.parse(bodyString);
        if (body.results != null) {
          newRail = [ railRow['STNCODE'],
            body.results[0].geometry.location.lat,
            body.results[0].geometry.location.lng,
            railRow['STNNAME'],
            railRow['CITY'],
            railRow['STATE'],
            'United States',
            '',
            '',
            '',
            config.rail.type,
            '',
            '',
            '',
            '',
            '',
            0,
            0
          ];

          allRailRows.push(newRail);
        }
      });
      setTimeout(cb, config.geocoder.delay);
    }, function () {
      csv.stringify(allRailRows, function(err, data) {
        fs.writeFileSync(config.rail.csvOutPath, data);
      });
    });
  });
}


if(require.main == module) {
  if (process.argv.length >= 3) {
    if (process.argv[2] === 'rail' && process.argv.length == 3) {
      regenerateCsv(loadAirports(function(){
        process.exit();
      }));
    }
    else {
      console.log('Error: invalid argument \"' + process.argv[2] + '\" - only valid argument is \'rail\' (to re-generate the rail geocoding before running) or nothing (to run as normal)');
      process.exit();
    }
  }
  else {
    loadAirports(function(){
      process.exit();
    });
  }
}

module.exports = loadAirports
