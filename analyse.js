var localhost         = '127.0.0.1',
  redis               = require('redis'),
  util                = require('util'),
  async               = require('async'),
  domain              = require('domain').create();

var redis_config = {local: 
                      {ip: localhost,
                      port: 6379},
                    c9:
                      {ip: process.env.IP,
                      port: 16349},
                    };

redis_config.current = redis_config.c9.ip ? redis_config.c9 : redis_config.local;

var stream = require('stream');

util.inherits(ExportRedis, stream.Readable);
util.inherits(ImportRedis, stream.Writable);
util.inherits(CreateStats, stream.Writable);

function handleError(err) {
  if (err) {
    util.log('An error occured:\n');
    util.log(err.stack);
    process.exit(1);
  }
};

function ExportRedis(options) {
  if (!(this instanceof ExportRedis))
    return new ExportRedis(options);

  stream.Readable.call(this, options);
  
  this._db = 0;
  this._data = '';
  this._buf_length = 0;

  this._redis_db = redis.createClient(redis_config.current.port, redis_config.current.ip);

  this._redis_db.on('error', function(err) {
    handleError(err); 
  });

  var _self = this;

  this._redis_db.select(this._db, function(err, res){
    if (res == 'OK') {
      _self._redis_db.get('next.ratings.id', function(err, id){
        if (res == 'OK') {
          _self._ratings_id = id;
          _self.read(0);
        } else {
          handleError(err);
        };
      });
    } else {
      handleError(err);
    };
  });
};

ExportRedis.prototype._read = function(size) {
  if (!size) size = 500;
  var _self = this;

  if (this._ratings_id) {
    async.whilst(
      function() { 
        return _self._ratings_id;
      },
      function(next) {
        _self._redis_db.hmget('ratings:' + _self._ratings_id, 'distribution', 'year', 'rank', 'title', function(err, result){
          // util.log(util.format("The returned result is: result: %s", util.inspect(result)));
          handleError(err);

          var data = _self._ratings_id + '|' + result.join('|') + '|';
          // util.log(util.format("The data is: data: %s", util.inspect(data)));
          var data_length = Buffer.byteLength(data);
          _self._buf_length += data_length;

          if (size - _self._buf_length < data_length) {
            var buf = new Buffer(_self._data, 'utf8');
            _self.push(buf);
            _self._data = null;
            _self._buf_length = 0;
          } else {
            _self._data += data;
          }
          
          _self._ratings_id -= 1;
          next();
        });
      },
      function(err) {
        if (!err) {
          _self._redis_db.bgsave();
          _self._redis_db.quit();
          _self.push(null);
          util.log("The end");
          process.exit(0);
        } else {
          handleError(err);
        }
      }
    ); 
  } else {
    return this.push('');
  }
};

function ImportRedis(options) {
  if (!(this instanceof ImportRedis)) {
    return new ImportRedis(options);
  }

  stream.Writable.call(this, options);
  
  this._db = 1;
  this._db_ready = false;
  this._redis_db = redis.createClient(redis_config.current.port, redis_config.current.ip);
  this._redis_db.on('error', function(err) {
    handleError(err); 
  });

  var _self = this;
  this._redis_db.select(_self._db, function(err, res){
    if (res == 'OK') {
      util.log(util.format("The DB %d has been selected", _self._db));
      _self._redis_db.flushdb(function(err, res){
        if (res == 'OK') {
          util.log(util.format("DB %d has been flushed", _self._db));
          _self._db_ready = true;
        }
      });
    }
  });
};

ImportRedis.prototype._write = function(chunk, encoding, callback) {
  var _self = this;

  if (!this._db_ready) {
    util.log('The DB is NOT ready');
    return;
  }

  var split_data = chunk.toString().replace('null','').split('|');
  while (split_data.length) {
    var id = split_data.shift();
    var distribution = split_data.shift();
    var year = split_data.shift();
    var rank = split_data.shift();
    var title = split_data.shift();

    _self._redis_db.multi()
      .sadd("years", year)
      .sadd("ranks", rank)
      .sadd("years:" + year, id)
      .sadd("ranks:" + rank, id)
      .sadd("distributions:" + distribution, id)
    .exec(function(err, resp) {
      handleError(err);
    });
  };

  callback(null);
};

function CreateStats(options) {
  if (!(this instanceof CreateStats)) {
    return new CreateStats(options);
  }

  this._db = 1;
  this._redis_db = redis.createClient(redis_config.current.port, redis_config.current.ip);
  this._redis_db.on('error', function(err) {
    handleError(err); 
  });

  var _self = this;
  this._redis_db.select(this._db, function(err, res){
    if (res == 'OK') {
      _self._redis_db.emit('selected');
    } 
  });
};

CreateStats.prototype.yearByRanks = function() {
  var _self = this;

  this._redis_db.once('selected', function(err) {
    util.log(util.format("The DB %d has been selected", _self._db));
    _self._redis_db.smembers("years", function(err, years){
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

        // years.forEach(function(year){
        //   ranks.forEach(function(rank){
        //     _self._redis_db.sinterstore(
        //       "years_ranks:" + year + ":" + rank, "years:" + year, "ranks:" + rank, function(err, resp) {
        //         handleError(err);
        //       }
        //     );
        //   });
        // });
        // util.log("Ranking by year finished");
        // _self._redis_db.emit('ranked');
      });
    }); 
  });
};

CreateStats.prototype.roundRanks = function() {
  var _self = this;
  var rank = 1;

  this._redis_db.once('ranked', function(err) {
    util.log(util.format("The DB %d has been selected", _self._db));
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

domain.on('error', function(err) {
  handleError(err);
});

// var redisRead = new ExportRedis();
// var prepareData = new ImportRedis();
var createStats = new CreateStats();

// domain.add(redisRead);
// domain.add(prepareData);
domain.add(createStats);

domain.run(function() {
  util.log("Data analysis started");

  // redisRead
  //   // .pipe(process.stdout);
  //   .pipe(prepareData);

  createStats.yearByRanks();
  createStats.roundRanks();

});
