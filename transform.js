const fs = require('fs');
const jsonStringify = require('json-pretty');
const files = [
  './iatacodes/airports.json'
];

for(var file of files){
  console.log(file);
  var data = require(file);
  console.log('data');
  fs.writeFileSync(file, jsonStringify(data));
}
