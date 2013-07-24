var redis      = require("redis"),
  localhost = '127.0.0.1',
  redis_config = {local: 
                    {ip: localhost,
                    port: 6379},
                  c9:
                    {ip: process.env.IP,
                    port: 16349},
                  };
                  
redis_config.current = redis_config.c9.ip ? redis_config.c9 : redis_config.local;
var db = redis.createClient(redis_config.current.port, redis_config.current.ip);

module.exports = function initS (app) {
  app.get('/s', function(req, res) {
    db.zcount("years", "1974", "1974", function(err, count){
      res.json(JSON.parse(count));
    });
  });
};