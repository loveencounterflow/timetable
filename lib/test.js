var fs = require('fs');
var parse = require('csv-parse');
var transform = require('stream-transform');

var output = [];
var parser = parse({delimiter: ':'})
var input = fs.createReadStream('/QUICK/gtfs-data/germany-berlin-2014/trips.txt');
var count = 0;
var transformer = transform(function(record, callback){
  setTimeout(function(){
    callback(null, record.join(' ')+'\n');
  }, 500);
},{parallel: 3});
var transformer2 = transform(function(lines, callback){
  callback(null, lines+'\n');
},{parallel: 3});


input.pipe(parser).pipe(transformer).pipe(process.stdout);