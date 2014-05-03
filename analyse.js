var localhost = '127.0.0.1',
  redis = require('redis'),
  d3 = require('d3'),
  util = require('util'),
  async = require('async'),
  stream = require('stream'),
  domain = require('domain').create();

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

util.inherits(ReadRatings, stream.Readable);
util.inherits(RatingsStats, stream.Writable);

function handleError(err) {
  if (err) {
    util.log('An error occured:\n');
    util.log(err.stack);
    process.exit(1);
  }
};

function ReadRatings(options) {
  if (!(this instanceof ReadRatings))
    return new ReadRatings(options);

  stream.Readable.call(this, options);

  var _self = this;
  
  this._db = 0;
  this._redis_db = redis.createClient(redis_config.current.port, redis_config.current.ip);
  this._redis_db.on('error', handleError);

  this._ratings_id = null;

  this._redis_db.select(this._db, function(err, res){
    if (res == 'OK') {
      _self._redis_db.get('next.ratings.id', function(err, id){
        handleError(err);
        util.log(util.format("Max ratings_id is %d", id));
        _self._ratings_id = id;
        _self.read(0);
      });
    } else {
      handleError(err);
    };
  });
};

ReadRatings.prototype._read = function() {
  var _self = this;

  if (this._ratings_id === null) { 
    this.push('');
    return;
  } 

  if (this._ratings_id < 0) {
    this.push(null);
    return;
  } else {
    _self._redis_db.hmget('ratings:' + this._ratings_id, 'distribution', 'year', 'rank', 'title', function(err, result){
      handleError(err);
      
      var data = _self._ratings_id + '|' + result.join('|') + '|',
        buf = new Buffer(data, 'utf8');

      _self._ratings_id -= 1;

      _self.push(buf);
    });
  }
};

function RatingsStats(options) {
  if (!(this instanceof RatingsStats)) {
    return new RatingsStats(options);
  }

  stream.Writable.call(this, options);
  
  this._db = 1;
  this._db_ready = false;
  this._redis_db = redis.createClient(redis_config.current.port, redis_config.current.ip);
  this._redis_db.on('error', handleError);

  var _self = this;
  this._redis_db.select(_self._db, function(err, res){
    if (res == 'OK') {
      util.log(util.format("The DB %d has been selected", _self._db));
      _self._redis_db.flushdb(function(err, res){
        if (res == 'OK') {
          util.log(util.format("The DB %d has been flushed", _self._db));
          _self._db_ready = true;
        }
      });
    }
  });
};

RatingsStats.prototype._write = function(chunk, encoding, next) {
  if (!this._db_ready) {
    util.log('The DB is NOT ready');
    return next(false);
  }

  var split_data = chunk.toString().replace('null','').split('|');

  var id = split_data.shift();
  var distribution = split_data.shift();
  var year = split_data.shift();
  var rank = split_data.shift();
  var title = split_data.shift();

  // util.log(util.format("_ratings_id is %d", id));

  this._redis_db.multi()
    .sadd("years", year)
    .sadd("ranks", rank)
    .sadd("years:" + year, id)
    .sadd("ranks:" + rank, id)
    .sadd("distributions:" + distribution, id)
  .exec(function(err, resp) {
    handleError(err);
    next(null);
  });
};

function RanksStats(options) {
  if (!(this instanceof RanksStats)) {
    return new RanksStats(options);
  }

  this._db = 1;
  this._redis_db = redis.createClient(redis_config.current.port, redis_config.current.ip);
  this._db_ready = false;
  this._redis_db.on('error', handleError);

  var _self = this;

  this._redis_db.select(this._db, function(err, res){
    if (res == 'OK') {
      _self._db_ready = true;
    } 
  });
};

RanksStats.prototype.yearByRanks = function() {
  var _self = this;
  util.log("yearByRanks started");

  this._redis_db.smembers("years", function(err, years){
    _self._redis_db.smembers("ranks", function(err, ranks){
      async.each(years, 
        function(year, ycallback){
          async.each(ranks,
            function(rank, rcallback){
              _self._redis_db.sinterstore(
              "years_ranks:" + year + ":" + rank, "years:" + year, "ranks:" + rank, function(err, resp) {
                handleError(err);
                rcallback();
              });
            },
            function(err){
              if (!err) {
                ycallback();
              }
            }
          );
        }, 
        function(err){
          if (!err) {
            util.log("Ranking by year finished");
            _self._redis_db.emit('ranked');
          }
        }
      );
    });
  }); 
};

RanksStats.prototype.roundRanks = function() {
  var _self = this;
  var rank = 1;
  
  this._redis_db.once('ranked', function(err) {
    util.log("roundRanks started");
    async.whilst(
      function() { 
        return rank <= 10; 
      },
      function(next) {
        _self._redis_db.sunionstore(
          "ranks_grouped:" + rank, 
            "ranks:" + (rank - 0.4).toFixed(1), 
            "ranks:" + (rank - 0.3).toFixed(1), 
            "ranks:" + (rank - 0.2).toFixed(1), 
            "ranks:" + (rank - 0.1).toFixed(1), 
            "ranks:" + (rank).toFixed(1), 
            "ranks:" + (rank + 0.1).toFixed(1), 
            "ranks:" + (rank + 0.2).toFixed(1), 
            "ranks:" + (rank + 0.3).toFixed(1), 
            "ranks:" + (rank + 0.4).toFixed(1), 
            "ranks:" + (rank + 0.5).toFixed(1), 
          function(err, resp) {
            handleError(err);
            rank = parseFloat(rank) + 1;
            next();
          }
        );
      },
      function(err) {
        if (!err) {
          _self._redis_db.bgsave();
          _self._redis_db.quit();
          util.log("The end");
          process.exit(0);
        }
      }
    );
  });
};

var readRatings = new ReadRatings();
var ratingsStats = new RatingsStats();
var ranksStats = new RanksStats();

domain.on('error', handleError);
readRatings.on('error', handleError);
ratingsStats.on('error', handleError);

domain.add(readRatings);
domain.add(ratingsStats);
domain.add(ranksStats);
domain.add(util);

domain.run(function() {
  util.log("Data analysis started");

  readRatings
    .pipe(ratingsStats);

  ratingsStats.once('finish', function(){
    readRatings._redis_db.quit();
    ratingsStats._redis_db.quit();

    ranksStats.yearByRanks();
    ranksStats.roundRanks();
  });
});
