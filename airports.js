/** Pull in dependencies */
const _ = require('lodash');
const assert = require('assert');
const async = require('async');
const csv = require('csv');
const fs = require('fs');
const jsonStringify = require('json-pretty');
const mongodb = require('mongodb');
const Promise = require("bluebird");
const request = require('request');
const sds = require('sabre-dev-studio');

/** Setup our config */
const config = {
  mongodb: {
    host: '127.0.0.1',
    port: 27017,
    database: 'travelbank',
    uri: process.env.MONGOLAB_URI
  },
  collection: process.env.AIRPORTS_collection || 'Stations',
  skipFields: [
    'lat',
    'lon',
    'runway_length',
    'elev',
    'icao',
    'direct_flights',
    'carriers',
    'woeid',
    'url',
    'email',
    'phone'
  ],
  geocoder: {
    baseUrl: 'https://maps.googleapis.com/maps/api/geocode/json?address=',
    keyUrl: '&key=AIzaSyCTFKDtVcvBiBK_KM6Y8uT2vNwBlIuvPSs',
    limit: 45,
    delay: 1000
  },
  rail: {
    type: 'Railway Stations',
    csvInPath: './sources/amtrak_stations.csv',
    csvOutPath: './sources/rail.csv',
    headerRow: [ 'code','lat','lon','name','city','state','country','woeid','tz','phone','type','email','url','runway_length','elev','icao','direct_flights','carriers' ]
  },
  multiAirportCity: {
    macCodeMapFile: './sources/mac_codes.json',
    macCodesToIgnore: [
      'QDF',
      'QHO',
      'QPH'
    ]
  },
  inputFiles: [
    './sources/airports.csv',
    './sources/rail.csv'
  ],
  stationsFile: './stations.json',
  sabreCredentials: {
    client_id: 'V1:7971:V0ZH:AA',
    client_secret: 'WS042614',
    uri: 'https://api.sabre.com'
  },
  sabreLimits: {
    limit: 20,
    delay: 5000
  }
};
/** Modify some configs based on the env */
for(var key of Object.keys(config.mongodb)){
  if (process.env[`MONGODB_${key}`]){
    config.mongodb[key] = process.env[`MONGODB_${key}`];
  }
}
/** Setup the connection to mongo */
if(!config.mongodb.uri){
  config.mongodb.uri = `mongodb://${config.mongodb.host}:${config.mongodb.port}/${config.mongodb.database}`;
}

/**
 * This function loads the JSON into the database, and gets run by default
 */
function loadToDb(cb) {
  return new Promise.resolve().then(function(db) {
    /** Connect to mongo */
    return mongodb.MongoClient.connect(config.mongodb.uri).then(function(db){
      console.log('Connected to mongodb...');
      console.log('Setting up our collection...');
      /** Create our collection, then drop it */
      const collection = db.collection(config.collection);
      /** Clear it */
      console.log('Clearing our collection...');
      return collection.deleteMany({}).then(function(){
        console.log('Ensuring indexes...');
        return collection.createIndex({location: '2dsphere'}).then(function(){
          console.log('Putting data back in...');
          /** Load the json file into the DB now */
          const stationsData = require(__dirname + '/' + config.stationsFile);
          return collection.insertMany(stationsData);
        });
      });
    });
  }).asCallback(cb);
};

/**
 * This function regenerates the data from the CSV to the JSON file
 */
function regenJson(cb) {
  console.log('Loading our CSVs...');
  /** Get our multi-airport city map to dynamically extend data */
  var macCodesMap = require(config.multiAirportCity.macCodeMapFile).map;
  /** Setup our data */
  const newData = [];
  /** Read our CSV file */
  async.eachSeries(config.inputFiles, function(inputFile, cb) {
    console.log('Read CSV file, converting to objects...');
    rawData = fs.readFileSync(__dirname + '/' + inputFile, 'utf8');
    csv.parse(rawData, {columns: true}, function (err, data) {
      let totalRows = data.length;
      let count = 0;
      assert.equal(null, err);
      async.eachLimit(data, config.geocoder.limit, function (row, cb) {
        count++;
        console.log(`Working ${count}/${totalRows} rows...`);
        /** If our row has no name */
        // if( row[ 'name' ].trim() == '' ){
        //   continue;
        // }
        // If our airport doesn't have enough carriers/flights, skip it
        if ((row['type'] === 'Airports' || row['type'] === 'Other Airport') && row['direct_flights'] < 5 && row['carriers'] < 2) {
          return cb(null);
        }
        // Gelocate
        var fullAddress = row['city'] + ', ' + row['state'] + ', ' + row['country'];
        var fullUrl = config.geocoder.baseUrl + encodeURIComponent(fullAddress) + config.geocoder.keyUrl;
        request(fullUrl, function (err, response, bodyString) {
          assert.equal(null, err);
          body = JSON.parse(bodyString);
          switch (body.status) {
            case 'OK':
            case 'ZERO_RESULTS':
              break;
            default:
              console.log('------------------------------')
              console.log(body);
              console.log('------------------------------')
              return cb('There was an error geocoding...');
          }
          row['stateCode'] = '';
          row['countryCode'] = '';
          if (body.results[0] != undefined) {
            for (var component of body.results[0].address_components) {
              // country -> country
              // administrative_area_level_1 -> state
              if (component.types.indexOf('country') !== -1) {
                if (component.short_name != undefined) {
                  row['countryCode'] = component.short_name;
                } else {
                  row['countryCode'] = component.long_name;
                }
              }
              if (component.types.indexOf('administrative_area_level_1') !== -1) {
                if (component.short_name != undefined) {
                  row['stateCode'] = component.short_name;
                } else {
                  row['stateCode'] = component.long_name;
                }
              }
            }
          }
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
          /** Add in multi-airport code if applicable */
          if ((row['type'] === 'Airports' || row['type'] === 'Other Airport') && macCodesMap[row['code']] != null) {
            row['macCode'] = macCodesMap[row['code']];
          }
          newData.push(row);
          setTimeout(cb, config.geocoder.delay);
        });
      }, function (err) {
        assert.equal(null, err);
        console.log('Done with the file, going to the next one...');
        cb();
      });
    });
  }, function(err){
    assert.equal(null, err);
    /** Write the file */
    console.log('Writing the JSON file...');
    fs.writeFileSync(__dirname + '/' + config.stationsFile, jsonStringify(newData));
    console.log('We are done, bailing!');
    cb();
  });
};



// }

function regenerateCsv(callback) {
  // File from http://www.rita.dot.gov/bts/sites/rita.dot.gov.bts/files/AdditionalAttachmentFiles/amtrak_sta.zip
  // Converted from txt -> dbf (via rename) -> csv (via http://dbfconv.com/ then pipe it thru the unix 'strings' function to clean it up)
  rawRail = fs.readFileSync(config.rail.csvInPath, 'utf8');
  allRailRows = [];
  allRailRows.push(config.rail.headerRow);
  console.log('Re-geocoding rail stations...');
  csv.parse(rawRail, {columns: true}, function (err, railData) {
    assert.equal(null, err);

    // Using the limit to throttle request rate so as not to exceed our query limit to google
    async.eachLimit (railData, config.geocoder.limit, function(railRow, cb) {
      if (railRow['STNTYPE'] != 'RAIL') {
        setTimeout(cb, config.geocoder.delay);
      }
      else {
        var fullAddress = railRow['ADDRESS1'] + ' ' + railRow['CITY'] + ', ' + railRow['STATE'] + ' ' + railRow['ZIP'];
        var fullUrl = config.geocoder.baseUrl + encodeURIComponent(fullAddress) + config.geocoder.keyUrl;
        request(fullUrl, function(error, response, bodyString) {
          body = JSON.parse(bodyString);
          if (body.results[0] != undefined) {
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
      }
    },
    function (err) {
      csv.stringify(allRailRows, function(err, data) {
        fs.writeFileSync(config.rail.csvOutPath, data);
      });
    });
  });
}

function generateMultiAirportCityCodes(callback) {
  var provider = new sds(config.sabreCredentials);
  var data = {
    airports: [],
    map: {}
  };
  console.log("Building multi-airport city map from scratch...")
  // Get list of all multi-airport cities
  provider.get('/v1/lists/supported/cities', {}, function (err, result){
    var multiAirportCities = JSON.parse(result).Cities;
    let totalRows = multiAirportCities.length;
    let count = 0;
    async.eachLimit(multiAirportCities, config.sabreLimits.limit, function(mac, callback){
      count++;
      console.log(`Working ${count}/${totalRows} rows...`);
      // If we have to skip an airport do it now
      if(config.multiAirportCity.macCodesToIgnore.indexOf(mac.code) !== -1){
        console.log(`Skipping code for ${mac.code}...`);
        return callback();
      }
      // Add it to our data
      delete mac.Links;
      data.airports.push(mac);
      // For each multi-airport city, get list of all airports
      provider.get('/v1/lists/supported/cities/' + mac.code + '/airports', {}, function(err, cityResult){
        var macAirports = JSON.parse(cityResult).Airports;
        for (var i in macAirports) {
          // Build object mapping IATA airport codes to multi-airport city codes
          data.map[macAirports[i].code] = mac.code;
        }
        setTimeout(callback, config.sabreLimits.delay);
      });
    }, function(err){
      if (err) {
        console.log("Error building multi-airport city map: " + err);
      }
      else {
        fs.writeFileSync(config.multiAirportCity.macCodeMapFile, jsonStringify(data));
        console.log("Generated multi-airport city map to json file...");
      }
    });
  });
}

if(require.main == module) {
  if (process.argv.length == 3) {
    if (process.argv[2] === 'rail') {
      regenerateCsv(function(err){
        if(err){
          console.error(err);
        }
        process.exit();
      });
    } else if (process.argv[2] === 'regen'){
      regenJson(function(err){
        if(err){
          console.error(err);
        }
        process.exit();
      });
    } else if (process.argv[2] === 'mac'){
      generateMultiAirportCityCodes(function(err){
        if(err){
          console.error(err);
        }
        process.exit();
      });
    } else {
      let err = `Error: invalid argument '${process.argv[2]}'...\n`;
      err += 'The only valid arguments are:\n';
      err += ' * rail: to re-generate the rail geocoding\n';
      err += ' * mac: to re-generate multi-airport city code mappings\n';
      err += ' * regen: to re-generate the stations JSON file\n';
      err += ' * -none-: to load the stations into the db';
      console.error(err);
      process.exit();
    }
  } else {
    loadToDb(function(err){
      if(err){
        console.error(err);
      }
      process.exit();
    });
  }
}

module.exports = loadToDb;
