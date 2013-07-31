var localhost         = '127.0.0.1',
  redis               = require("redis"),
  util                = require('util'),
  async               = require("async"),
  domain              = require('domain').create();

var redis_config = {local: 
                      {ip: localhost,
                      port: 6379},
                    c9:
                      {ip: process.env.IP,
                      port: 16349},
                    };

redis_config.current = redis_config.c9.ip ? redis_config.c9 : redis_config.local;
var db = redis.createClient(redis_config.current.port, redis_config.current.ip);

var Readable = require('stream').Readable;
util.inherits(ExportRedis, Readable);

function ExportRedis(options) {
  if (!(this instanceof ExportRedis))
    return new ExportRedis(options);

  Readable.call(this, options);
  
  this._index = 0;
  this._db    = 0;
  this._dists  = [];
  
  this._rawHeader = [];
  this.header = null;
};

ExportRedis.prototype._read = function(size) {
  if (!size) size = 500;
  var self = this;
  var buf  = new Buffer(size);

  db.select(self._db, function(err, res){
    if (res == 'OK') {
      db.get("next.ratings.id", function(err, id){
        if (res == 'OK') {
          util.log("The analysis has started");
          async.whilst(
            function() { 
              return id;
            },
            function(next) {
              db.hget('ratings:' + id, "distribution", function(err, distribution){
                handle_error(err);

                // self._dists.push({i: id, d: distribution});
                // we don't have data yet
                
                if size - Buffer.byteLength(self._dists) < Buffer.byteLength({i: id, d: distribution}) {
                  buf.write(self._dists, 'utf8');
                  self.push(buf);
                  self._dists = [];
                } else {
                  self._dists.push({i: id, d: distribution});
                  self.push(null);
                }
                
                id -= 1;
                next();
              });
            },
            function(err) {
              if (!err) {
                // db.bgsave();
                db.quit();
                self.push(null);
                util.log("The end");
                process.exit(0);
              } else {
                handle_error(err);
              }
            }
          );
        } else {
          handle_error(err);
        }
      });
    } else {
      handle_error(err);
    };
  });
};

function handle_error(err) {
  if (err) {
    util.log('HORRIBLE ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!');
    util.log(err);
    process.exit(1);
  }
};

function returnInt(element){
  return parseInt(element, 10);
};

function returnBigger(element1, element2){
  return element1 < element2 ? element2 : element1;
};

function cleanseAnalysis(distribution){
  db.del("distributions:" + distribution);
}

function buildDistributions(id, distribution){
  db.select(1, function(err, res){
    if (res == 'OK') {
      db.lpush("distributions:" + distribution + ":ids", id, function(err, length){
        handle_error(err);
        db.zrem("distributions", distribution, function(err, res){
          handle_error(err);
          db.zadd("distributions", length, distribution);
        });
      });
    } else {
      handle_error(err);
    };
  });
};


db.on("error", function(err) {
  handle_error(err); 
});

domain.on('error', function(err) {
  handle_error(err);
});

var redis_read = new ExportRedis();

domain.add(db);
domain.add(redis_read);

domain.run(function() {
  redis_read
    .pipe(process.stdout);
});