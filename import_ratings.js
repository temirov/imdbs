var localhost         = '127.0.0.1',
  redis               = require("redis"),
  fs                  = require('fs'),
  util                = require('util'),
  domain              = require('domain').create(),
  imdb_source_ratings = 'data/source/ratings.list',
  broken_line;

var redis_config = {local: 
                      {ip: localhost,
                      port: 6379},
                    c9:
                      {ip: process.env.IP,
                      port: 16349},
                    };

redis_config.current = redis_config.c9.ip ? redis_config.c9 : redis_config.local;

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

function isInt(year) {
  return !isNaN(parseInt(year, 10));
};

function parseYearFromTitle(title, callback){
  var year_position = 0,
    year = null; 

  do {
    year_position = title.indexOf('(', year_position) + 1;
    if (year_position) {
      year = title.substr(year_position, 4);
    }
  } while (!(isInt(year) || year_position === 0));

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

ImportRedis.prototype.insertRating = function(distribution, votes, rank, title) {
  if (typeof title != 'undefined') {
    var _self = this;
    this._redis_db.incr("next.ratings.id", function(err, incr){
      parseYearFromTitle(title, function(err, year){
        handleError(err);
        _self._redis_db.multi()
        .hmset(
          "ratings:" + incr, 
          "distribution", distribution,
          "rank", rank,
          "votes", votes,
          "title", title,
          "year", year,
          function(err, resp) {
            handleError(err);
          }
        )
        .zadd(
          "years", year, incr, function(err, resp) {
            handleError(err);
          }
        )
        .zadd(
          "ranks", rank, incr, function(err, resp) {
            handleError(err);
          }
        )
        .exec(function(err, resp) {
          handleError(err);
        });
      });
    });
  }
};

ImportRedis.prototype.splitRating = function(single_chunk, callback) {
  var short_string = single_chunk.toString().trim(),
    distribution   = short_string.substring(0,10).trim(),
    votes          = short_string.substring(11,19).trim(),
    rank           = short_string.substring(20,24).trim(),
    title          = short_string.substring(25).trim();
  callback(distribution, votes, rank, title);
};

ImportRedis.prototype._write = function(chunk, encoding, callback) {
  if (this._db_ready) {
    this.splitRating(chunk, this.insertRating.bind(this));
    callback(null);
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
}

ReshapeChunks.prototype._transform = function(chunk, encoding, callback) {
  var offset = chunk.length;

  while (chunk[offset] !== 0x0a) {
    offset -= 1;
  };
  offset = -(chunk.length - offset);

  if (broken_line) {
    chunk = Buffer.concat([broken_line, chunk]);
  }
  broken_line = chunk.slice(offset);

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

  var self = this;

  while (offset < chunk.length) {
    if (chunk[offset] === 0x0a) {
      self.push(chunk.slice(prev_offset, offset));
      prev_offset = offset;
    }
    offset += 1;
  };

  callback(null);
}; 

var import_redis = new ImportRedis();
var reshape_chunks = new ReshapeChunks();
var split_chunks = new SplitChunks();

domain.on('error', function(err) {
  handleError(err);
});

domain.add(import_redis);
domain.add(split_chunks);
domain.add(reshape_chunks);

domain.run(function() {
  var ratings = fs.createReadStream(imdb_source_ratings, {encoding: null});
  util.log("Data import started");

  ratings
    .pipe(reshape_chunks)
    .pipe(split_chunks)
    .pipe(import_redis);

  ratings.on('end', function() {
    import_redis._redis_db.bgsave();
    import_redis._redis_db.quit();
    util.log("The end");
    process.exit(0);       
  });
});
