var redis      = require("redis"),
  util         = require("util"),
  async        = require("async"),
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
    getYearsCount(function(err, years){
      res.json(years);
    });
  });
};

function getYearsCount(callback){
  var years = [], 
    year = 1888, 
    err = null,
    current_year = new Date().getFullYear();
  
  async.whilst(
    function() { 
      return year <= current_year; 
    },
    function(next) {
      db.zcount("years", year, year, function(err, count){
        // util.log(util.format("year is: %s, count is: %d", year, count));
        years.push({y: year, c: count});
        year += 1;
        next();
      });
    },
    function(err) {
      callback(err, years);
    }
  );
}