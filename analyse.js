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
// var Readable = stream.Readable;
// var Writable = stream.Writable;

util.inherits(ExportRedis, stream.Readable);
util.inherits(ImportRedis, stream.Writable);

function handle_error(err) {
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
  this._dists = '';
  this._buf_length = 0;

  this._redis_db = redis.createClient(redis_config.current.port, redis_config.current.ip);

  this._redis_db.on('error', function(err) {
    handle_error(err); 
  });

  var self = this;

  this._redis_db.select(this._db, function(err, res){
    if (res == 'OK') {
      self._redis_db.get('next.ratings.id', function(err, id){
        if (res == 'OK') {
          self._ratings_id = id;
          self.read(0);
        } else {
          handle_error(err);
        };
      });
    } else {
      handle_error(err);
    };
  });
};

ExportRedis.prototype._read = function(size) {
  if (!size) size = 500;
  var self = this;

  if (this._ratings_id) {
    async.whilst(
      function() { 
        return self._ratings_id;
      },
      function(next) {
        // self._redis_db.hget('ratings:' + self._ratings_id, 'distribution', function(err, distribution){
        self._redis_db.hmget('ratings:' + self._ratings_id, 'distribution', 'year', 'rank', function(err, result){
          // util.log(util.format("The returned result is: result: %s", util.inspect(result)));
          handle_error(err);

          var distribution = result[0]; 
          var year = result[1]; 
          var rank = result[2]; 

          var data = self._ratings_id + ',' + distribution + ',' + year + ',' + rank + ',';
          // util.log(util.format("The data is: data: %s", util.inspect(data)));
          var data_length = Buffer.byteLength(data);
          self._buf_length += data_length;

          if (size - self._buf_length < data_length) {
            var buf = new Buffer(self._dists, 'utf8');
            self.push(buf);
            self._dists = null;
            self._buf_length = 0;
          } else {
            self._dists += data;
          }
          
          self._ratings_id -= 1;
          next();
        });
      },
      function(err) {
        if (!err) {
          self._redis_db.bgsave();
          self._redis_db.quit();
          self.push(null);
          util.log("The end");
          process.exit(0);
        } else {
          handle_error(err);
        }
      }
    ); 
  } else {
    return this.push('');
  }
};

function ImportRedis(options) {
  if (!(this instanceof ImportRedis))
    return new ImportRedis(options);

  stream.Writable.call(this, options);
  
  this._db = 1;
  this._db_ready = false;
  
  this._redis_db = redis.createClient(redis_config.current.port, redis_config.current.ip);

  this._redis_db.on('error', function(err) {
    handle_error(err); 
  });

  var self = this;

  this._redis_db.select(self._db, function(err, res){
    if (res == 'OK') {
      self._redis_db.flushdb(function(err, res){
        if (res == 'OK') {
          util.log(util.format("DB %d has been flushed", self._db));
          self._db_ready = true;
        } else {
          handle_error(err);
        };
      });
    } else {
      handle_error(err);
    };
  });
};

ImportRedis.prototype._write = function(chunk, encoding, callback) {
  if (this._db_ready) {
    // util.log(util.format("chunk.length: %d", chunk.length));
    // var split_data = chunk.toString().split(',');
    var split_data = chunk.toString().replace('null','').split(',');
    // util.log(util.format("split_data: %s \n", split_data));

    while (split_data.length) {
      var id = split_data.shift();
      var distribution = split_data.shift();
      var year = split_data.shift();
      var rank = split_data.shift();

      // util.log(util.format("DATA before write: distribution: %s, id: %d, year: %s, rank: %s", distribution, id, year, rank));

      // var self = this;
      // self.distribution = distribution;
      // util.log(util.format("BEFORE lpush: distribution: %s, id: %d", distribution, id));

      // (self._redis_db.lpush('distributions:' + distribution + ':ids', id, function(err, l){
      //   console.log(util.inspect(err));
      //   // handle_error(err);
      //   console.log(util.inspect(l));
      // }))(self);

      // this._redis_db.lpush('distributions:' + distribution + ':ids', id, (function(err, length){
      //   handle_error(err);
      //   // util.log(util.inspect(self));
      //   util.log(util.format("AFTER lpush: distribution: %s, id: %d", distribution, id));
      //   // util.log(util.format("chunk.length: %d", chunk.length));

      //   // self._redis_db.zrem('distributions', distribution, function(err, res){
      //   //   handle_error(err);
      //   //   // util.log(util.format("length: %d", length));
      //   //   self._redis_db.zadd('distributions', length, distribution, function(err, res){
      //   //     handle_error(err);
      //   //   });
      //   // });
      // })(this)); 

      // this._redis_db.sadd(
      //   "years:" + year, id, function(err, resp) {
      //     handle_error(err);
      //   }
      // )

      this._redis_db.multi()
      .zadd(
        "distributions", distribution, id, function(err, resp) {
          handle_error(err);
        }
      )
      .sadd(
        "years:" + year, id, function(err, resp) {
          handle_error(err);
        }
      )
      .sadd(
        "ranks:" + rank, id, function(err, resp) {
          handle_error(err);
        }
      )
      .sadd(
        "distributions:" + distribution, id, function(err, resp) {
          handle_error(err);
        }
      )
      .exec(function(err, resp) {
        handle_error(err);
      });
      // .lpush('distributions:' + distribution + ':ids', id, function(err, length){
      //   handle_error(err);
      //   // util.log(util.format("AFTER lpush: length: %d", length));
      //   // util.log(util.format("chunk.length: %d", chunk.length));
      // }); 
    };
  } else {
    util.log('The DB is NOT ready');
  };

  callback(null);
};

domain.on('error', function(err) {
  handle_error(err);
});

var redis_read = new ExportRedis();
var redis_write = new ImportRedis();

domain.add(redis_read);
domain.add(redis_write);

domain.run(function() {
  redis_read
    // .pipe(process.stdout);
    .pipe(redis_write);

  // redis_read.on('end', function() {
  //   db.bgsave();
  //   db.quit();
  //   util.log("The end");
  //   process.exit(0);       
  // });  
});