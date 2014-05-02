var localhost         = '127.0.0.1',
  redis               = require('redis'),
  d3                  = require('d3'),
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
  this._redis_db_connections = 100;

  this._redis_db.on('error', function(err) {
    handleError(err); 
  });

  var _self = this;

  this._redis_db.select(this._db, function(err, res){
    if (res == 'OK') {
      _self._redis_db.get('next.ratings.id', function(err, id){
        handleError(err);
        _self._ratings_id = id;
        _self._next_rating = true;
        _self._ratings_ids = d3.range(+id);
        _self.read(0);
      });
    } else {
      handleError(err);
    };
  });
};

ExportRedis.prototype.flushData = function() {
  var buf = new Buffer(this._data, 'utf8');
  this.push(buf);
  this._data = null;
  this._buf_length = 0;
  util.log('Pushed data');
}

ExportRedis.prototype._read = function(size) {
  var _self = this;

  if (!size) size = 500;
  if (!this._ratings_ids) { 
    return this.push('');
  }

  // util.log(util.format("_self._next_rating is %s; _self._ratings_id is %s", _self._next_rating, _self._ratings_id));

  // (function loop(){
  //   if (_self._next_rating && _self._ratings_id > 0){
  //     _self._next_rating = false;
  //     _self._redis_db.hmget('ratings:' + _self._ratings_id, 'distribution', 'year', 'rank', 'title', function(err, result){
  //       handleError(err);

  //       var data = _self._ratings_id + '|' + result.join('|') + '|';
  //       var data_length = Buffer.byteLength(data);
  //       _self._buf_length += data_length;

  //       if (size - _self._buf_length < data_length) {
  //         _self.flushData();
  //       } else {
  //         _self._data += data;
  //       }
        
  //       _self._ratings_id -= 1;
  //       _self._next_rating = true;
  //       loop();
  //     });
  //   }
  // })();

  // while (_self._ratings_id > 0) {
  //   // util.log(util.format("_self._next_rating is %s; _self._ratings_id is %s", _self._next_rating, _self._ratings_id));
  //   // _self._next_rating = false;
  //   _self._redis_db.hmget('ratings:' + _self._ratings_id, 'distribution', 'year', 'rank', 'title', function(err, result){
  //     util.log(util.format("_ratings_id is %d", _self._ratings_id));
  //     handleError(err);

  //     var data = _self._ratings_id + '|' + result.join('|') + '|';
  //     var data_length = Buffer.byteLength(data);
  //     _self._buf_length += data_length;

  //     if (size - _self._buf_length < data_length) {
  //       _self.flushData();
  //     } else {
  //       _self._data += data;
  //     }
      
  //     _self._ratings_id -= 1;
  //   });
  // }

  // // util.log(util.format("_ratings_id is %d", _self._ratings_id));
  // _self.flushData();
  // _self._redis_db.bgsave();
  // _self._redis_db.quit();
  // _self.push(null);
  // util.log("The end");
  // process.exit(0);
  // _self._redis_db.emit('finished_reading');

  // function test(){
  //   return (_self._next_rating && _self._ratings_id > 0);
  // }

  function getData(id, next){
    _self._redis_db.hmget('ratings:' + id, 'distribution', 'year', 'rank', 'title', function(err, result){
      handleError(err);
      util.log(util.format("_ratings_id is %d", id));

      var data = id + '|' + result.join('|') + '|';
      var data_length = Buffer.byteLength(data);
      _self._buf_length += data_length;

      if (size - _self._buf_length < data_length) {
        _self.flushData();
      } else {
        _self._data += data;
      }

      _self._ratings_ids = _self._ratings_ids.filter(function(d){
        return d !== id;
      });

      next(null);
    });
  }

  function finalStep(err) {
    handleError(err);
    util.log("Final Step");
    // util.log(util.format("_ratings_id is %d", _self._ratings_id));
    _self.flushData();
    _self._redis_db.bgsave();
    _self._redis_db.quit();
    _self.push(null);
    util.log("The end");
    // process.exit(0);
    _self._redis_db.emit('finished_reading');
  }

  // // util.log(util.format("_ratings_ids is %s", this._ratings_ids));

  // // async.eachLimit(this._ratings_ids, this._redis_db_connections, getData, finalStep);
  async.each(this._ratings_ids, getData, finalStep);

  // async.whilst(
  //   test, 
  //   getData, 
  //   finalStep
  // ); 
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
    return callback(false);
  }

  var split_data = chunk.toString().replace('null','').split('|');

  while (split_data.length) {
    var id = split_data.shift();
    var distribution = split_data.shift();
    var year = split_data.shift();
    var rank = split_data.shift();
    var title = split_data.shift();

    util.log(util.format("_ratings_id is %d", id));

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

  this._redis_db.once('finished_reading', function(err){
    _self._redis_db.select(this._db, function(err, res){
      if (res == 'OK') {
        _self._redis_db.emit('selected');
      } 
    });
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

var redisRead = new ExportRedis();
var prepareData = new ImportRedis();
var createStats = new CreateStats();

domain.add(redisRead);
domain.add(prepareData);
domain.add(createStats);

domain.run(function() {
  util.log("Data analysis started");

  redisRead
    .pipe(process.stdout)
    .pipe(prepareData);

  // createStats.yearByRanks();
  // createStats.roundRanks();

});
