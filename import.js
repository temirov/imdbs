var localhost = '127.0.0.1',
  redis = require("redis"),
  fs = require('fs'),
  util = require('util'),
  stream = require('stream'),
  domain = require('domain').create(),
  imdb_source_ratings = 'data/source/ratings.list';

var redis_config = {local: 
                      {ip: localhost,
                      port: 6379},
                    c9:
                      {ip: process.env.IP,
                      port: 16349},
                    ec2:
                      {ip: process.env.REDIS_HOST,
                      port: 6379},
                    };

switch (true) { 
  case redis_config.c9.ip:
    redis_config.current = edis_config.c9;
    break;
  case redis_config.ec2.ip:
    redis_config.current = redis_config.ec2;
    break;
  default:
    redis_config.current = redis_config.local;
}

// redis_config.current = redis_config.ec2;

util.inherits(FileByLine, stream.Transform);

function handleError(err) {
  if (err) {
    util.log('An error occured:\n');
    util.log(err.stack);
    process.exit(1);
  }
};

function parseYearFromTitle(title, callback){
  var year = null; 
  year = title.match(/\((18|19|20)\d{2}(\)|\/)/g);
  
  if (year) {
    year = year[0].substr(1,4)
  } else {
    util.log(util.format("Got no year for the film titled: %s", title));
  }

  callback(null, year);
};

function FileByLine(options) {
  if (!(this instanceof FileByLine)) {
    return new FileByLine(options);
  }

  stream.Transform.call(this, options);

  this._broken_line = null;
  this._chunkNumber = 0;
};

FileByLine.prototype._flush = function(callback) {
  util.log(util.format("Chunks processed: %d", this._chunkNumber));
  callback(null);
}

FileByLine.prototype._transform = function(chunk, encoding, next) {
  var offset = chunk.length,
    line_offset = 0, 
    prev_offset = 0,
    output = false,
    delayed_output = false,
    i = 0;

  while (chunk[offset] !== 0x0a && offset >= 0) {
    offset -= 1;
  };
  offset = -(chunk.length - offset);

  if (this._broken_line) {
    chunk = Buffer.concat([this._broken_line, chunk]);
  }
  this._broken_line = chunk.slice(offset);
  chunk = chunk.slice(0,chunk.length + offset)

  while (line_offset < chunk.length) {
    if (chunk[line_offset] === 0x0a) {
      this.push(chunk.slice(prev_offset, line_offset));
      prev_offset = line_offset;
    }
    line_offset += 1;
  };

  this._chunkNumber += 1;
  next();
};

var fileByLine = new FileByLine(),
  ratings = fs.createReadStream(imdb_source_ratings, {encoding: null});

ratings.on('error', handleError);
fileByLine.on('error', handleError);
domain.on('error', handleError);

domain.add(util);
domain.add(ratings);
domain.add(fileByLine);
domain.add(redis);

domain.run(function() {
  util.log("Data import started");

  var db = 0,
  redisDb = redis.createClient(redis_config.current.port, redis_config.current.ip);

  redisDb.on('error', handleError);

  redisDb.select(this.db, function(err, res){
    util.log(util.format("The DB %s has been selected", db));
    redisDb.flushdb(function(err, res){
      util.log(util.format("The DB %s has been flushed", db));
      redisDb.set("next.ratings.id", 0, function(err, res){
        util.log("Ratings ID zeroed");

        ratings.pipe(fileByLine);

        fileByLine.on('readable', function(){
          var line = null;
          while (line = fileByLine.read()) {
            line = line.toString().trim();
            // console.log(line);
            
            var distribution = line.substring(0,10).trim(),
            votes = line.substring(11,19).trim(),
            rank = line.substring(20,24).trim(),
            title = line.substring(25).trim();

            if (typeof title !== 'undefined') {
              redisDb.incr("next.ratings.id", function(err, incr){
                parseYearFromTitle(title, function(err, year){        
                  redisDb.hmset(
                    "ratings:" + incr, 
                    "distribution", distribution,
                    "rank", rank,
                    "votes", votes,
                    "title", title,
                    "year", year,
                    function(err, resp) {
                      handleError(err);
                    }
                  );
                });
              });
            } else {
              util.log(util.format("Got no title for the film: %s", line));
            }  
          }
        });
      });
    });
  });

  ratings.once('end', function() {
    redisDb.bgsave();
    redisDb.quit();
    util.log("The end");
    process.exit(0);       
  });
});
