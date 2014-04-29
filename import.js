var localhost         = '127.0.0.1',
  redis               = require("redis"),
  fs                  = require('fs'),
  util                = require('util'),
  domain              = require('domain').create(),
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

redis_config.current = redis_config.ec2;

var stream = require('stream');
util.inherits(ImportRedis, stream.Writable);
util.inherits(ReshapeChunks, stream.Transform);
util.inherits(SplitChunks, stream.Transform);

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

function ImportRedis(options) {
  if (!(this instanceof ImportRedis)) {
    return new ImportRedis(options);
  }

  stream.Writable.call(this, options);
  
  this._db = 0;
  this._db_ready = false;
  this._redis_db = redis.createClient(redis_config.current.port, redis_config.current.ip);
  this._redis_db.on('error', function(err) {
    handleError(err); 
  });

  var _self = this;

  this._redis_db.select(this._db, function(err, res){
    util.log(util.format("The DB %s has been selected", _self._db));
    _self._redis_db.flushdb(function(err, res){
      util.log(util.format("The DB %s has been flushed", _self._db));
      _self._db_ready = true;
      _self._redis_db.set("next.ratings.id", 0, function(err, res){
        util.log("Ratings ID zeroed");
      });
    });
  });
};

ImportRedis.prototype.insertRating = function(distribution, votes, rank, title, done) {
  if (typeof title !== 'undefined') {
    var _self = this;
    this._redis_db.incr("next.ratings.id", function(err, incr){
      parseYearFromTitle(title, function(err, year){        
        _self._redis_db.hmset(
          "ratings:" + incr, 
          "distribution", distribution,
          "rank", rank,
          "votes", votes,
          "title", title,
          "year", year,
          function(err, resp) {
            done(err);
          }
        );
      });
    });
  }
};

ImportRedis.prototype.splitRating = function(single_chunk, done, callback) {
  var short_string = single_chunk.toString().trim(),
    distribution   = short_string.substring(0,10).trim(),
    votes          = short_string.substring(11,19).trim(),
    rank           = short_string.substring(20,24).trim(),
    title          = short_string.substring(25).trim();
  callback(distribution, votes, rank, title, done);
};

ImportRedis.prototype._write = function(chunk, encoding, callback) {
  if (this._db_ready) {
    this.splitRating(chunk, callback, this.insertRating.bind(this));
  } else {
    // util.log(util.format("The DB %s is NOT ready", this._db));
    callback(false);
  };
};

function ReshapeChunks(options) {
  if (!(this instanceof ReshapeChunks)) {
    return new ReshapeChunks(options);
  }

  stream.Transform.call(this, options);

  this._broken_line = null;
}

ReshapeChunks.prototype._transform = function(chunk, encoding, callback) {
  var offset = chunk.length;

  while (chunk[offset] !== 0x0a) {
    offset -= 1;
  };
  offset = -(chunk.length - offset);

  if (this._broken_line) {
    chunk = Buffer.concat([this._broken_line, chunk]);
  }
  this._broken_line = chunk.slice(offset);

  callback(null, chunk);
};

function SplitChunks(options) {
  if (!(this instanceof SplitChunks)) {
    return new SplitChunks(options);
  }

  stream.Transform.call(this, options);
}

SplitChunks.prototype._transform = function(chunk, encoding, callback) {
  var offset        = 0, 
    prev_offset     = 0;

  while (offset < chunk.length) {
    if (chunk[offset] === 0x0a) {
      if (chunk.toString().match(/\"American Eats\"/)){
        util.log(util.format("The offset is: %d", offset));
      }
      this.push(chunk.slice(prev_offset, offset));
      prev_offset = offset;
    }
    offset += 1;
  };

  callback(null);
}; 

var importRedis = new ImportRedis();
var reshapeChunks = new ReshapeChunks();
var splitChunks = new SplitChunks();
var ratings = fs.createReadStream(imdb_source_ratings, {encoding: null});

domain.on('error', function(err) {
  handleError(err);
});

domain.add(ratings);
domain.add(importRedis);
domain.add(splitChunks);
domain.add(reshapeChunks);

domain.run(function() {
  util.log("Data import started");

  ratings
    .pipe(reshapeChunks)
    .pipe(splitChunks)
    .pipe(importRedis);

  ratings.once('end', function() {
    importRedis._redis_db.bgsave();
    importRedis._redis_db.quit();
    util.log("The end");
    process.exit(0);       
  });
});
