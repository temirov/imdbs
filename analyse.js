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

function returnInt(element){
  return parseInt(element, 10);
}

function returnBigger(element1, element2){
  return element1 < element2 ? element2 : element1;
}

db.on("error", function (err) {
  console.log("Error " + err);
  process.exit(1); 
})

domain.on('error', function(er) {
  util.log(util.inspect(er));
});

domain.add(db);

domain.run(function() {
  db.select(0, function(err, res){
    if (res == 'OK') {
      db.get("next.ratings.id", function(err, id){
        db.del("distributions", function(err, res){
          if (!err) {
            async.whilst(
              function() { 
                return id;
              },
              function(next) {
                db.hmget('ratings:' + id, "distribution", function(err, distribution){
                  // db.del("distributions:" + distribution);
                  db.lpush("distributions:" + distribution, id, function(err, length){
                    if (!err){
                      var minusone = length - 1;
                      if (minusone > 1) db.srem("distributions:frequency:" + minusone, distribution);
                      db.sadd("distributions:frequency:" + length, distribution);
                    }
                  });
                  db.lpush("distribution:" + distribution + "ids", +  id, 
                  db.hincrby("distributions", distribution, 1);
                  // util.log(util.format('Distribution is %s, id is %d', distribution, id));
                  id -= 1;
                  next();
                });
              },
              function(err) {
                db.quit();
                util.log("The end");
                process.exit(0); 
              }
            );
          }
        });
      });

      // db.hlen("distributions", function(err, id){
      //   db.hget

      // });

      // db.hvals("distributions", function(err, values){
      //   util.log(util.format('Values are %d', values.length));
        
      //   util.log(util.format('Max value is %d', values.map(returnInt).reduce(returnBigger)));
        
      //   db.quit();
      //   util.log("The end");
      //   process.exit(0); 
      // });

    } else {
      util.log("An error occured: %s", res);
      if (err) {
        process_error(err);
        process.exit(1);
      }
    }
  });
});