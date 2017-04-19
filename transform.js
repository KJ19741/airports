const fs = require('fs');
const jsonStringify = require('json-pretty');
const files = [
  './iatacodes/airports.json'
];

for(var file of files){
  var data = require(file);
  fs.writeFileSync(file, jsonStringify(data));
}
