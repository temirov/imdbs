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

function ExportRedis(opt) {
  Readable.call(this, opt);
  this._max = 1000000;
  this._index = 1;
  this._db = 0;
}

ExportRedis.prototype._read = function() {
  var i = this._index++;
  if (i > this._max)
    this.push(null);
  else {
    var str = '' + i;
    var buf = new Buffer(str, 'ascii');
    this.push(buf);
  }

  db.select(this._db, function(err, res){
    if (res == 'OK') {
      handle_error(err);
      db.get("next.ratings.id", function(err, id){
        if (res == 'OK') {
          util.log("The analysis has started");
          async.whilst(
            function() { 
              return id;
            },
            function(next) {
              var i = 0;
              db.hget('ratings:' + id, "distribution", function(err, distribution){
                handle_error(err);
                if (i == 50) {
                  var buf = new Buffer(res, 'utf8');
                  this.push(buf);
                  i = 0;
                } else {
                  res.push({i: id, d: distribution});
                  // this.push(null);
                  i += 1;
                };

                id -= 1;
                next();
              });
            },
            function(err) {
              if (!err) {
                db.bgsave();
                db.quit();
                util.log("The end");
                process.exit(0);
              } else {
                handle_error(err);
              }; 
            };
          );
    } else {
      handle_error(err);
    };
  });


};

function handle_error(err) {
  if (err) {
    util.log('HORRIBLE ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!');
    util.log(util.inspect(err));
    process.exit(1);
  }
};

function returnInt(element){
  return parseInt(element, 10);
};

function returnBigger(element1, element2){
  return element1 < element2 ? element2 : element1;
};

function cleanseAnalysis(){
  db.del("distributions:" + distribution);
}

function buildDistributions(id, distribution){
  db.select(1, function(err, res){
    if (res == 'OK') {
      db.lpush("distributions:" + distribution + ":ids", id, function(err, length){
        if (!err){
          db.zrem("distributions", distribution, function(err, res){
            handle_error(err);
            db.zadd("distributions", length, distribution);
          });
        } else {
          handle_error(err);
        };
      });
    } else {
      handle_error(err);
    };
  });
};

function readDistributions(){
  db.select(0, function(err, res){
    if (res == 'OK') {
      handle_error(err);
      async.whilst(
        function() { 
          return id;
        },
        function(next) {
          db.hmget('ratings:' + id, "distribution", function(err, distribution){
            handle_error(err);
            buildDistributions(id, distribution);
            id -= 1;
            next();
          });
        },
        function(err) {
          if (!err) {
            db.bgsave();
            db.quit();
            util.log("The end");
            process.exit(0);
          } else {
            handle_error(err);
          }; 
        };
      );
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

domain.add(db);

domain.run(function() {
  db.select(0, function(err, res){
    if (res == 'OK') {
      db.get("next.ratings.id", function(err, id){
        if (res == 'OK') {
          util.log("The analysis has started");
          readDistributions;
        } else {
          handle_error(err);
        };
      });
    } else {
      handle_error(err);
    }
  });
});